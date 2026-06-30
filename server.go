package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"sync"
	"time"

	// Pure Go Driver (Zero C compiler/GCC required!)
	_ "modernc.org/sqlite"
)

// --- Configuration ---
const (
	Port            = 16000
	StartingCapital = 1000000.0 // Baseline virtual cash for portfolio calculations
)

var (
	baseDir, _   = os.Getwd()
	jsonFile     = filepath.Join(baseDir, "prices.json")
	databaseFile = filepath.Join(baseDir, "transactions.db")
	analyticsFile = filepath.Join(baseDir, "portfolio_analytics.db") // Pointer to your new analytics DB
	tplFile      = filepath.Join(baseDir, "tpl.json") 
)

var servedWebpages = []string{"index.html", "leaderboard.html", "manual.html"}

// Hardcoded tournament login credentials
var users = map[string]string{
	"Testing":             "Testing@123",
	"Admin":               "Admin@123",
	"Ojasvi Lunawat":      "O_L@XB",
	"Pal Sakhala":         "P_S@XB",
	"Mohnish Sachdev":     "M_S@IXB",
	"Gokul Mewani":        "G_M@IXB",
	"Nidhi Gorde":         "N_G@XE",
	"Shalmali Puranik":    "S_P@XE",
	"Shivansh Agrawal":    "S_A@IXF",
	"Anamika Hom":         "A_H@IXE",
	"Ahillya Deore":       "A_D@IXE",
	"Jay Hargunani":       "J_H@IXI",
	"Aryan Patil":         "A_P@IXE",
	"Rugved Dighe":        "R_D@XD",
	"Arnav Shewale":       "A_S@XB",
	"Aditi Bagle":         "A_B@XC",
	"Bhoomi Bedmutha":     "B_B@XB", 
	"Animesh Garg":        "A_G@XD",
	"Piyush Chaudhari":    "P_C@XF",
	"Advait Malli":        "A_M@XI",
	"Advait Gangurde":     "A_G@IXE",
	"Arnav Dhatrak":       "A_D@IXC",
	"Shreyash Fartade":    "S_F@IXC",
	"Omar Bakshi":         "O_B@XF",
	"Almaan Karim":        "A_K@XI",
	"Aayush Lagali":       "A_L@XB",
	"Sarvarth Agarwal":    "S_A@XD",
	"Arko Majumdar":       "A_M@XI",
	"Kavish Rawal":        "K_R@XB",
	"Tanuj Lalwani":       "T_L@XB",
	"Shivam Ahuja":        "S_A@XB",
	"Tanmay Jadhav":       "T_J@XB",
	"Shagun Jangid":       "S_J@XE",
	"Devansh Tarwani":     "D_T@XB",
	"Gorav Bhandari":      "G_B@XB",
	"Aayushi Sable":       "A_S@XE",
	"Kanya Nagpal":        "K_N@XI",
	"Siddhi Suryawanshi":  "S_S@XI",
}

// Global SQLite connection pointers
var db *sql.DB
var analyticsDb *sql.DB // Database handle for portfolio analytics records

// In-memory charts tracking variables (Now tracking user profits instead of sectors)
var (
	userHistoryData  = make(map[string][]map[string]interface{}) // Tracks individual user live P/L timeline
	maxHistoryPoints = 600
	historyLock      sync.Mutex
)

// In-memory Leaderboard state storage
var (
	leaderboardEntries []LeaderboardEntry
	leaderboardLock    sync.Mutex
)

// Sanitizer script logic to make sure user tables don't compromise sql syntax patterns
var safeNameRegex = regexp.MustCompile(`^[a-zA-Z0-9_]+$`)

type LeaderboardEntry struct {
	Username string  `json:"username"`
	TotalPL  float64 `json:"total_profit_loss"`
	Rank     int     `json:"rank"`
}

// --- Helper Functions ---

func getStockPrices() map[string]interface{} {
	file, err := os.ReadFile(jsonFile)
	if err != nil {
		return map[string]interface{}{"error": "Price data file not found", "stocks": map[string]interface{}{}}
	}

	var data map[string]interface{}
	if err := json.Unmarshal(file, &data); err != nil {
		return map[string]interface{}{"error": "Failed to parse price data", "stocks": map[string]interface{}{}}
	}
	return data
}

// LIVE ENGINE LOOP: Evaluates individual live performance every 3 seconds
func updateUserPLHistoryLoop() {
	for {
		time.Sleep(3 * time.Second)
		
		priceDataRaw := getStockPrices()
		stocksMap, ok := priceDataRaw["stocks"].(map[string]interface{})
		if !ok {
			continue
		}

		livePrices := make(map[string]float64)
		for _, catData := range stocksMap {
			category, ok := catData.(map[string]interface{})
			if !ok {
				continue
			}
			for ticker, priceVal := range category {
				if p, ok := priceVal.(float64); ok {
					livePrices[ticker] = p
				}
			}
		}

		historyLock.Lock()
		rows, err := db.Query("SELECT name FROM sqlite_schema WHERE type='table' AND name NOT LIKE 'sqlite_%';")
		if err != nil {
			historyLock.Unlock()
			continue
		}

		var tables []string
		for rows.Next() {
			var name string
			if rows.Scan(&name) == nil {
				tables = append(tables, name)
			}
		}
		rows.Close()

		timestamp := time.Now().Format("15:04:05")

		for _, username := range tables {
			txRows, err := db.Query(fmt.Sprintf(`SELECT Type, Stock, Quantity, Price_TT FROM "%s"`, username))
			if err != nil {
				continue
			}

			cash := StartingCapital
			holdingsQty := make(map[string]int)

			for txRows.Next() {
				var action, stock string
				var qty int
				var priceTT float64
				if txRows.Scan(&action, &stock, &qty, &priceTT) == nil {
					if action == "BUY" || action == "buy" {
						cash -= priceTT
						holdingsQty[stock] += qty
					} else if action == "SELL" || action == "sell" {
						cash += priceTT
						holdingsQty[stock] -= qty
					}
				}
			}
			txRows.Close()

			stockValue := 0.0
			for ticker, qty := range holdingsQty {
				if qty > 0 {
					stockValue += float64(qty) * livePrices[ticker]
				}
			}

			totalNetWorth := cash + stockValue
			overallProfitOrLoss := totalNetWorth - StartingCapital

			point := map[string]interface{}{
				"time":  timestamp,
				"price": overallProfitOrLoss,
			}
			userHistoryData[username] = append(userHistoryData[username], point)

			if len(userHistoryData[username]) > maxHistoryPoints {
				userHistoryData[username] = userHistoryData[username][1:]
			}
		}
		historyLock.Unlock()
	}
}

func isWebpageAllowed(filename string) bool {
	for _, name := range servedWebpages {
		if name == filename {
			return true
		}
	}
	return false
}

// --- API Route Handlers ---

func login(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var creds struct {
		Username string `json:"username"`
		Password string `json:"password"`
	}

	if err := json.NewDecoder(r.Body).Decode(&creds); err != nil {
		http.Error(w, "Bad request", http.StatusBadRequest)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	storedPassword, exists := users[creds.Username]
	if exists && storedPassword == creds.Password {
		json.NewEncoder(w).Encode(map[string]interface{}{"success": true, "message": "Login successful"})
	} else {
		json.NewEncoder(w).Encode(map[string]interface{}{"success": false, "message": "Invalid username or password"})
	}
}

func apiPrices(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	data := getStockPrices()
	json.NewEncoder(w).Encode(data)
}

func getChartHistory(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	
	username := r.URL.Query().Get("username")
	if username == "" {
		http.Error(w, "Missing username query parameter", http.StatusBadRequest)
		return
	}

	historyLock.Lock()
	defer historyLock.Unlock()

	userTimeline := make(map[string][]map[string]interface{})
	if timeline, exists := userHistoryData[username]; exists {
		userTimeline["Your Profit Timeline"] = timeline
	} else {
		userTimeline["Your Profit Timeline"] = []map[string]interface{}{}
	}

	json.NewEncoder(w).Encode(userTimeline)
}

func apiGetTransactions(w http.ResponseWriter, r *http.Request) {
	username := r.URL.Query().Get("username")
	if username == "" {
		http.Error(w, "Missing username", http.StatusBadRequest)
		return
	}

	if !safeNameRegex.MatchString(username) {
		http.Error(w, "Invalid username format", http.StatusBadRequest)
		return
	}
	query := fmt.Sprintf("SELECT Time, Type, Stock, Quantity, Price_PS, Price_TT FROM \"%s\"", username)

	rows, err := db.Query(query)
	if err != nil {
		http.Error(w, "Could not retrieve transactions", http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	var txs []map[string]interface{}
	for rows.Next() {
		var time, action, stock string
		var quantity int
		var pricePS, priceTT float64
		if err := rows.Scan(&time, &action, &stock, &quantity, &pricePS, &priceTT); err != nil {
			continue
		}
		txs = append(txs, map[string]interface{}{
			"time": time, "action": action, "stock": stock, "quantity": quantity, 
			"price_per_stock": pricePS, "total_price": priceTT,
		})
	}
	
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(txs)
}

// REFACTORED HISTORY ENDPOINT: Pulls directly from portfolio_analytics.db
func apiUserHistoryMetrics(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	username := r.URL.Query().Get("username")
	if username == "" {
		http.Error(w, "Missing username parameter", http.StatusBadRequest)
		return
	}

	if !safeNameRegex.MatchString(username) {
		http.Error(w, "Invalid username format", http.StatusBadRequest)
		return
	}

	// Query key-value metric snapshot metrics table belonging to requested user
	query := fmt.Sprintf(`SELECT metric_key, metric_value FROM "%s"`, username)
	rows, err := analyticsDb.Query(query)
	if err != nil {
		http.Error(w, "Portfolio analytics data not found for user", http.StatusNotFound)
		return
	}
	defer rows.Close()

	// Convert table format back into a cleanly accessible JSON object map
	metricsMap := make(map[string]interface{})
	for rows.Next() {
		var key, valStr string
		if err := rows.Scan(&key, &valStr); err != nil {
			continue
		}

		// If it's the nested holdings object block, parse it into structured JSON directly
		if key == "active_holdings" {
			var holdingsObj interface{}
			if err := json.Unmarshal([]byte(valStr), &holdingsObj); err == nil {
				metricsMap[key] = holdingsObj
				continue
			}
		}

		// Check if value string can be sent as float/integer primitive numerical parameters
		if valFloat, err := json.Number(valStr).Float64(); err == nil {
			metricsMap[key] = valFloat
		} else if valInt, err := json.Number(valStr).Int64(); err == nil {
			metricsMap[key] = valInt
		} else {
			metricsMap[key] = valStr
		}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(metricsMap)
}

func recordTransaction(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var tx struct {
		User          string  `json:"user"`
		Action        string  `json:"action"`
		Stock         string  `json:"stock"`
		Quantity      int     `json:"quantity"`
		PricePerStock float64 `json:"price_per_stock"`
		TotalPrice    float64 `json:"total_price"`
		Timestamp     string  `json:"timestamp"`
	}

	if err := json.NewDecoder(r.Body).Decode(&tx); err != nil {
		http.Error(w, "Bad request", http.StatusBadRequest)
		return
	}

	if !safeNameRegex.MatchString(tx.User) {
		http.Error(w, "Invalid username format", http.StatusBadRequest)
		return
	}

	createTableSQL := fmt.Sprintf(`
	CREATE TABLE IF NOT EXISTS "%s" (
		"Time" TEXT,
		"Type" TEXT,
		"Stock" TEXT,
		"Quantity" INTEGER,
		"Price_PS" REAL,
		"Price_TT" REAL
	);`, tx.User)

	_, err := db.Exec(createTableSQL)
	if err != nil {
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}

	insertSQL := fmt.Sprintf(`INSERT INTO "%s" (Time, Type, Stock, Quantity, Price_PS, Price_TT) VALUES (?, ?, ?, ?, ?, ?);`, tx.User)
	_, err = db.Exec(insertSQL, tx.Timestamp, tx.Action, tx.Stock, tx.Quantity, tx.PricePerStock, tx.TotalPrice)
	if err != nil {
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]bool{"success": true})
}

func serveTplJSON(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	
	leaderboardLock.Lock()
	defer leaderboardLock.Unlock()

	fileData, err := os.ReadFile(tplFile)
	if err != nil {
		json.NewEncoder(w).Encode([]interface{}{})
		return
	}
	w.Write(fileData)
}

func initLeaderboard() {
	file, err := os.ReadFile(tplFile)
	if err != nil {
		log.Println("Could not load tpl.json, starting empty.")
		return
	}
	leaderboardLock.Lock()
	json.Unmarshal(file, &leaderboardEntries)
	leaderboardLock.Unlock()
}

func syncTotalPL(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var incoming LeaderboardEntry
	if err := json.NewDecoder(r.Body).Decode(&incoming); err != nil {
		http.Error(w, "Bad request", http.StatusBadRequest)
		return
	}

	leaderboardLock.Lock()
	defer leaderboardLock.Unlock()

	found := false
	for i, entry := range leaderboardEntries {
		if entry.Username == incoming.Username {
			leaderboardEntries[i].TotalPL = incoming.TotalPL
			found = true
			break
		}
	}
	if !found {
		leaderboardEntries = append(leaderboardEntries, LeaderboardEntry{Username: incoming.Username, TotalPL: incoming.TotalPL})
	}

	sort.Slice(leaderboardEntries, func(i, j int) bool {
		return leaderboardEntries[i].TotalPL > leaderboardEntries[j].TotalPL
	})

	for i := range leaderboardEntries {
		leaderboardEntries[i].Rank = i + 1
	}

	updatedData, err := json.MarshalIndent(leaderboardEntries, "", "  ")
	if err != nil {
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}

	if err := os.WriteFile(tplFile, updatedData, 0644); err != nil {
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]bool{"success": true})
}

// --- Main Server Engine Lifecycle ---

func main() {
	var err error
	
	// 1. Core transactions database connection
	db, err = sql.Open("sqlite", databaseFile)
	if err != nil {
		log.Fatalf("Failed to open core transactions database: %v", err)
	}
	defer db.Close()

	// 2. Open pipeline connection context into portfolio_analytics.db
	analyticsDb, err = sql.Open("sqlite", analyticsFile)
	if err != nil {
		log.Fatalf("Failed to bind to live metrics storage context: %v", err)
	}
	defer analyticsDb.Close()

	go updateUserPLHistoryLoop()

	mux := http.NewServeMux()

	// API Routing Maps
	mux.HandleFunc("/api/login", login)
	mux.HandleFunc("/api/prices", apiPrices)
	mux.HandleFunc("/api/chart-history", getChartHistory)
	mux.HandleFunc("/api/history", apiUserHistoryMetrics) // Bound to analytics tracking DB queries
	mux.HandleFunc("/api/transaction", recordTransaction)
	mux.HandleFunc("/api/leaderboard", serveTplJSON)
	mux.HandleFunc("/api/sync-tpl", syncTotalPL)
	mux.HandleFunc("/api/transactions", apiGetTransactions)
	initLeaderboard()

	// Dynamic Static Router
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		path := filepath.Clean(r.URL.Path)
		if path == "/" || path == "." {
			path = "index.html"
		} else {
			path = filepath.Base(path)
		}

		if !isWebpageAllowed(path) {
			http.Error(w, "Access Forbidden", http.StatusForbidden)
			return
		}

		fullPath := filepath.Join(baseDir, path)
		http.ServeFile(w, r, fullPath)
	})

	corsHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")

		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusOK)
			return
		}
		mux.ServeHTTP(w, r)
	})

	fmt.Print("\033[H\033[2J") // Clear terminal screen
    
	fmt.Println("\033[35m==================================================\033[0m")
	fmt.Println("\033[36m    ASHOKA UNIVERSAL SCHOOL MOCK-STOCK ENGINE     \033[0m")
	fmt.Println("\033[35m==================================================\033[0m")
    
	fmt.Printf("\033[32m[SUCCESS]\033[0m Backend API server is live on port: \033[33m%d\033[0m\n", Port)
	fmt.Printf("\033[34m[INFO]\033[0m    Database bound to: \033[33m%s\033[0m\n", databaseFile)
	fmt.Printf("\033[34m[INFO]\033[0m    Analytics trace bound to: \033[33m%s\033[0m\n", analyticsFile)
	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%d", Port), corsHandler))
}
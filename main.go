package main

import (
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"sync"
	"time"

	_ "modernc.org/sqlite" // Pure Go driver (Zero C compiler/GCC required)
)

// --- Configuration Constants ---
const (
	Port            = 16000
	StartingCapital = 100000.0 // Adjusted virtual baseline cash for portfolio calculations
)

var (
	baseDir, _    = os.Getwd()
	jsonFile      = filepath.Join(baseDir, "prices.json")
	databaseFile  = filepath.Join(baseDir, "transactions.db")
	analyticsFile = filepath.Join(baseDir, "portfolio_analytics.db")
	tplFile       = filepath.Join(baseDir, "tpl.json")
	passwordsFile = filepath.Join(baseDir, "passwords.json") // Reference to credentials storage
)

var servedWebpages = []string{"index.html", "leaderboard.html", "manual.html"}

// Global dynamic map for tournament login credentials (loaded from passwords.json)
var users = make(map[string]string)

// Global SQLite connection pools
var db *sql.DB
var analyticsDb *sql.DB

// --- Core Data Structures ---
type Portfolio struct {
	Username          string             `json:"username"`
	StartingCash      float64            `json:"starting_cash"`
	RemainingCash     float64            `json:"remaining_cash"`
	CurrentStockValue float64            `json:"current_stock_holding_value"`
	RealizedPL        float64            `json:"realized_p_l"`
	UnrealizedPL      float64            `json:"unrealized_p_l"`
	TotalNetWorth     float64            `json:"total_net_worth"`
	OverallPL         float64            `json:"overall_total_p_l"`
	TotalTransactions int                `json:"total_transactions"`
	TotalUnsoldShares int                `json:"total_unsold_shares"`
	Rank              int                `json:"rank"`
	ActiveHoldings    map[string]Holding `json:"active_holdings"`
}

type Holding struct {
	Quantity        int     `json:"quantity"`
	AverageBuyPrice float64 `json:"average_buy_price"`
	CurrentPrice    float64 `json:"current_market_price"`
	TotalCost       float64 `json:"total_cost_basis"`
	CurrentValue    float64 `json:"current_market_value"`
}

type PriceData struct {
	Stocks map[string]map[string]float64 `json:"stocks"`
}

type LeaderboardEntry struct {
	Username string  `json:"username"`
	TotalPL  float64 `json:"total_profit_loss"`
	Rank     int     `json:"rank"`
}

// Memory tracking allocations
var (
	userHistoryData    = make(map[string][]map[string]interface{})
	maxHistoryPoints   = 600
	historyLock        sync.Mutex
	leaderboardEntries []LeaderboardEntry
	leaderboardLock    sync.Mutex
	safeNameRegex      = regexp.MustCompile(`^[a-zA-Z0-9_]+$`)
)

// --- Initialization Functions ---

func initPasswords() {
	// If passwords.json doesn't exist, create it as a clean, blank json ecosystem
	if _, err := os.Stat(passwordsFile); os.IsNotExist(err) {
		log.Println("[INFO] passwords.json not found. Initializing blank credentials registry...")
		blankRegistry := map[string]string{}
		data, err := json.MarshalIndent(blankRegistry, "", "  ")
		if err != nil {
			log.Fatalf("Failed to encode clean credentials data schema: %v", err)
		}
		if err := os.WriteFile(passwordsFile, data, 0644); err != nil {
			log.Fatalf("Failed to establish passwords.json placeholder instance: %v", err)
		}
	}

	// Read credentials file straight into server RAM allocations
	fileData, err := os.ReadFile(passwordsFile)
	if err != nil {
		log.Fatalf("Failed to read passwords.json: %v", err)
	}
	if err := json.Unmarshal(fileData, &users); err != nil {
		log.Fatalf("Failed to parse passwords.json configuration format: %v", err)
	}
	log.Printf("\033[32m[SUCCESS]\033[0m Loaded %d registered tournament profiles from passwords.json\n", len(users))
}

// --- Helper Functions & Pipeline Logic ---

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

func loadMarketPrices(filePath string) (map[string]float64, error) {
	file, err := os.ReadFile(filePath)
	if err != nil {
		return nil, err
	}
	var data PriceData
	if err := json.Unmarshal(file, &data); err != nil {
		return nil, err
	}
	flattenedPrices := make(map[string]float64)
	for _, industryStocks := range data.Stocks {
		for ticker, price := range industryStocks {
			flattenedPrices[ticker] = price
		}
	}
	return flattenedPrices, nil
}

func getUserTables(dbInstance *sql.DB) ([]string, error) {
	rows, err := dbInstance.Query("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%';")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err == nil {
			tables = append(tables, name)
		}
	}
	return tables, nil
}

func computeUserPortfolio(dbInstance *sql.DB, username string, prices map[string]float64) (Portfolio, error) {
	query := fmt.Sprintf(`SELECT Type, Stock, Quantity, Price_TT FROM "%s"`, username)
	rows, err := dbInstance.Query(query)
	if err != nil {
		return Portfolio{}, err
	}
	defer rows.Close()

	cash := StartingCapital
	realizedPL := 0.0
	txCount := 0

	type trackingHolding struct {
		qty       int
		totalCost float64
	}
	holdingsMap := make(map[string]*trackingHolding)

	for rows.Next() {
		var action, stock string
		var qty int
		var priceTT float64
		if err := rows.Scan(&action, &stock, &qty, &priceTT); err != nil {
			continue
		}
		txCount++

		if _, exists := holdingsMap[stock]; !exists {
			holdingsMap[stock] = &trackingHolding{}
		}

		if action == "BUY" || action == "buy" {
			cash -= priceTT
			holdingsMap[stock].qty += qty
			holdingsMap[stock].totalCost += priceTT
		} else if action == "SELL" || action == "sell" {
			cash += priceTT
			th := holdingsMap[stock]
			if th.qty > 0 {
				avgCost := th.totalCost / float64(th.qty)
				costOfSoldShares := avgCost * float64(qty)
				realizedPL += (priceTT - costOfSoldShares)

				th.qty -= qty
				th.totalCost -= costOfSoldShares
			}
		}
	}

	currentStockHoldingValue := 0.0
	unrealizedPL := 0.0
	unsoldSharesCount := 0
	activeHoldings := make(map[string]Holding)

	for ticker, track := range holdingsMap {
		if track.qty <= 0 {
			continue
		}

		currentMarketPrice := prices[ticker]
		marketVal := float64(track.qty) * currentMarketPrice
		unrealizedStockPL := marketVal - track.totalCost

		currentStockHoldingValue += marketVal
		unrealizedPL += unrealizedStockPL
		unsoldSharesCount += track.qty

		activeHoldings[ticker] = Holding{
			Quantity:        track.qty,
			AverageBuyPrice: track.totalCost / float64(track.qty),
			CurrentPrice:    currentMarketPrice,
			TotalCost:       track.totalCost,
			CurrentValue:    marketVal,
		}
	}

	totalNetWorth := cash + currentStockHoldingValue
	overallPL := totalNetWorth - StartingCapital

	return Portfolio{
		Username:          username,
		StartingCash:      StartingCapital,
		RemainingCash:     cash,
		CurrentStockValue: currentStockHoldingValue,
		RealizedPL:        realizedPL,
		UnrealizedPL:      unrealizedPL,
		TotalNetWorth:     totalNetWorth,
		OverallPL:         overallPL,
		TotalTransactions: txCount,
		TotalUnsoldShares: unsoldSharesCount,
		ActiveHoldings:    activeHoldings,
	}, nil
}

func runAnalyticsPipeline() {
	livePrices, err := loadMarketPrices(jsonFile)
	if err != nil {
		log.Printf("Skipping update. Error parsing market prices: %v", err)
		return
	}

	tables, err := getUserTables(db)
	if err != nil {
		log.Printf("Failed table catalog analysis index scan: %v", err)
		return
	}

	var summaries []Portfolio
	for _, username := range tables {
		portfolio, err := computeUserPortfolio(db, username, livePrices)
		if err != nil {
			log.Printf("Skipping identity trace for %s due to processing error: %v", username, err)
			continue
		}
		summaries = append(summaries, portfolio)
	}

	sort.Slice(summaries, func(i, j int) bool {
		return summaries[i].OverallPL > summaries[j].OverallPL
	})
	for i := range summaries {
		summaries[i].Rank = i + 1
	}

	saveToJSON(summaries)
	saveToCSV(summaries)
	saveToSQLite(summaries)

	fmt.Printf("\033[32m[SUCCESS]\033[0m Reports and Database instances finalized at: %s\n\n", time.Now().Format("15:04:05"))
}

func saveToJSON(reports []Portfolio) {
	jsonFile, err := os.Create(tplFile)
	if err != nil {
		log.Printf("Failed creating tpl.json document: %v", err)
		return
	}
	defer jsonFile.Close()

	encoder := json.NewEncoder(jsonFile)
	encoder.SetIndent("", "  ")
	_ = encoder.Encode(reports)
}

func saveToCSV(reports []Portfolio) {
	csvFile, err := os.Create("trader_report.csv")
	if err != nil {
		log.Printf("Failed creating trader_report.csv file: %v", err)
		return
	}
	defer csvFile.Close()

	writer := csv.NewWriter(csvFile)
	defer writer.Flush()

	_ = writer.Write([]string{
		"Rank", "Trader Name", "Total Transactions", "Total Profit/Loss",
		"Liquid Cash Remaining", "Unsold Shares Vol", "Portfolio Valuation",
	})

	for _, r := range reports {
		_ = writer.Write([]string{
			strconv.Itoa(r.Rank),
			r.Username,
			strconv.Itoa(r.TotalTransactions),
			fmt.Sprintf("%.2f", r.OverallPL),
			fmt.Sprintf("%.2f", r.RemainingCash),
			strconv.Itoa(r.TotalUnsoldShares),
			fmt.Sprintf("%.2f", r.CurrentStockValue),
		})
	}
}

func saveToSQLite(reports []Portfolio) {
	for _, r := range reports {
		createTableStmt := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS "%s" (
			metric_key TEXT PRIMARY KEY,
			metric_value TEXT
		);`, r.Username)

		if _, err := analyticsDb.Exec(createTableStmt); err != nil {
			log.Printf("Failed dynamically building target summary entity structure: %v", err)
			continue
		}

		_, _ = analyticsDb.Exec(fmt.Sprintf(`DELETE FROM "%s"`, r.Username))

		stmt, err := analyticsDb.Prepare(fmt.Sprintf(`INSERT INTO "%s" (metric_key, metric_value) VALUES (?, ?)`, r.Username))
		if err != nil {
			continue
		}

		metrics := map[string]string{
			"rank":                         strconv.Itoa(r.Rank),
			"starting_cash":                fmt.Sprintf("%.2f", r.StartingCash),
			"remaining_cash":               fmt.Sprintf("%.2f", r.RemainingCash),
			"current_stock_holding_value":  fmt.Sprintf("%.2f", r.CurrentStockValue),
			"realized_p_l":                 fmt.Sprintf("%.2f", r.RealizedPL),
			"unrealized_p_l":               fmt.Sprintf("%.2f", r.UnrealizedPL),
			"total_net_worth":              fmt.Sprintf("%.2f", r.TotalNetWorth),
			"overall_total_p_l":            fmt.Sprintf("%.2f", r.OverallPL),
			"total_transactions":           strconv.Itoa(r.TotalTransactions),
			"total_unsold_shares":          strconv.Itoa(r.TotalUnsoldShares),
		}

		holdingsJSON, _ := json.Marshal(r.ActiveHoldings)
		metrics["active_holdings"] = string(holdingsJSON)

		for k, v := range metrics {
			_, _ = stmt.Exec(k, v)
		}
		stmt.Close()
	}
}

// --- Background Engine Loops ---

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

// --- HTTP Request Handlers ---

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
	json.NewEncoder(w).Encode(getStockPrices())
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

	query := fmt.Sprintf(`SELECT metric_key, metric_value FROM "%s"`, username)
	rows, err := analyticsDb.Query(query)
	if err != nil {
		http.Error(w, "Portfolio analytics data not found for user", http.StatusNotFound)
		return
	}
	defer rows.Close()

	metricsMap := make(map[string]interface{})
	for rows.Next() {
		var key, valStr string
		if err := rows.Scan(&key, &valStr); err != nil {
			continue
		}
		if key == "active_holdings" {
			var holdingsObj interface{}
			if err := json.Unmarshal([]byte(valStr), &holdingsObj); err == nil {
				metricsMap[key] = holdingsObj
				continue
			}
		}
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

	fmt.Printf("\033[33m[EVENT]\033[0m Trade written successfully for %s. Triggering internal pipeline...\n", tx.User)
	runAnalyticsPipeline()

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

// --- Core Initialization & Server Lifecycle ---

// --- Core Initialization & Server Lifecycle ---

func main() {
	var err error

	// 1. Establish global connections to Core Transaction DB
	db, err = sql.Open("sqlite", databaseFile)
	if err != nil {
		log.Fatalf("Failed to open core transactions database: %v", err)
	}
	defer db.Close()

	// 2. Establish global connections to Portfolio Analytics DB
	analyticsDb, err = sql.Open("sqlite", analyticsFile)
	if err != nil {
		log.Fatalf("Failed to open analytics database: %v", err)
	}
	defer analyticsDb.Close()

	// 3. Load dynamic username and password credentials from passwords.json
	initPasswords()

	// Initial report build baseline on startup
	fmt.Println("\033[36m[SYSTEM] Initializing Unified Analytics Engine...\033[0m")
	runAnalyticsPipeline()
	initLeaderboard()

	// Fire up background process loops as concurrent goroutines
	go updateUserPLHistoryLoop()

	// ----------------------------------------------------
	// 4. ROUTER 1: PORT 16000 (Main App, index, manual)
	// ----------------------------------------------------
	mux16000 := http.NewServeMux()
	mux16000.HandleFunc("/api/login", login)
	mux16000.HandleFunc("/api/prices", apiPrices)
	mux16000.HandleFunc("/api/chart-history", getChartHistory)
	mux16000.HandleFunc("/api/history", apiUserHistoryMetrics)
	mux16000.HandleFunc("/api/transaction", recordTransaction)
	mux16000.HandleFunc("/api/transactions", apiGetTransactions)

	// Static Assets Route Middleware for Port 16000
	mux16000.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		path := filepath.Clean(r.URL.Path)
		if path == "/" || path == "." {
			path = "index.html"
		} else {
			path = filepath.Base(path)
		}

		// Port 16000 explicitly allows index.html and manual.html
		if path != "index.html" && path != "manual.html" {
			http.Error(w, "Access Forbidden on Port 16000", http.StatusForbidden)
			return
		}

		fullPath := filepath.Join(baseDir, path)
		http.ServeFile(w, r, fullPath)
	})

	// ----------------------------------------------------
	// 5. ROUTER 2: PORT 5000 (Leaderboard Only)
	// ----------------------------------------------------
	mux5000 := http.NewServeMux()
	mux5000.HandleFunc("/api/leaderboard", serveTplJSON)
	mux5000.HandleFunc("/api/sync-tpl", syncTotalPL)

	// Static Assets Route Middleware for Port 5000
	mux5000.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		path := filepath.Clean(r.URL.Path)
		if path == "/" || path == "." || filepath.Base(path) == "leaderboard.html" {
			http.ServeFile(w, r, filepath.Join(baseDir, "leaderboard.html"))
			return
		}
		http.Error(w, "Access Forbidden on Port 5000", http.StatusForbidden)
	})

	// ----------------------------------------------------
	// 6. CORS & LIFECYCLE MANAGEMENT
	// ----------------------------------------------------
	createCORSHandler := func(mux *http.ServeMux) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Access-Control-Allow-Origin", "*")
			w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
			w.Header().Set("Access-Control-Allow-Headers", "Content-Type")

			if r.Method == http.MethodOptions {
				w.WriteHeader(http.StatusOK)
				return
			}
			mux.ServeHTTP(w, r)
		})
	}

	fmt.Print("\033[H\033[2J") // Clear screen terminal output formatting

	fmt.Println("\033[35m==================================================\033[0m")
	fmt.Println("\033[36m    ASHOKA UNIVERSAL SCHOOL MOCK-STOCK ENGINE     \033[0m")
	fmt.Println("\033[35m==================================================\033[0m")

	fmt.Printf("\033[34m[INFO]\033[0m    Database bound to: \033[33m%s\033[0m\n", databaseFile)
	fmt.Printf("\033[34m[INFO]\033[0m    Analytics trace bound to: \033[33m%s\033[0m\n", analyticsFile)

	// Spin up Port 5000 in a concurrent background goroutine
	go func() {
		fmt.Printf("\033[32m[SUCCESS]\033[0m Leaderboard Display engine live on port: \033[33m5000\033[0m\n")
		if err := http.ListenAndServe(":5000", createCORSHandler(mux5000)); err != nil {
			log.Fatalf("Leaderboard server on port 5000 failed: %v", err)
		}
	}()

	// Keep the main thread alive hosting Port 16000
	fmt.Printf("\033[32m[SUCCESS]\033[0m Main Trading Engine live on port: \033[33m16000\033[0m\n")
	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%d", Port), createCORSHandler(mux16000)))
}
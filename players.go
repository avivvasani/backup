package main

import (
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"time"

	"github.com/fsnotify/fsnotify"
	_ "modernc.org/sqlite" // FIXED: Switched to pure Go driver (No CGO required for Windows)
)

// --- Configuration Constants ---
const (
	StartingCash = 1000000.0
	DatabaseFile = "transactions.db"
	PricesFile   = "prices.json"
	AnalyticsDB  = "portfolio_analytics.db"
)

// Advanced Portfolio Metrics
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

func main() {
	fmt.Print("\033[H\033[2J")
	fmt.Println("\033[36m[SYSTEM] Starting Live Cross-Platform File-Watcher Engine...\033[0m")

	runAnalyticsPipeline()

	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		log.Fatalf("Failed to initialize system file-watcher: %v", err)
	}
	defer watcher.Close()

	err = watcher.Add(DatabaseFile)
	if err != nil {
		err = watcher.Add(".")
		if err != nil {
			log.Fatalf("Failed to establish directory trace points: %v", err)
		}
	}

	for {
		select {
		case event, ok := <-watcher.Events:
			if !ok {
				return
			}
			if event.Has(fsnotify.Write) && filepath.Base(event.Name) == DatabaseFile {
				fmt.Printf("\033[33m[EVENT]\033[0m Ledger modification caught at %s. Regenerating records...\n", time.Now().Format("15:04:05"))
				runAnalyticsPipeline()
			}
		case err, ok := <-watcher.Errors:
			if !ok {
				return
			}
			log.Printf("Watcher monitoring warning: %v", err)
		}
	}
}

func runAnalyticsPipeline() {
	baseDir, _ := os.Getwd()
	dbPath := filepath.Join(baseDir, DatabaseFile)
	jsonPath := filepath.Join(baseDir, PricesFile)

	livePrices, err := loadMarketPrices(jsonPath)
	if err != nil {
		log.Printf("Skipping update. Error parsing market prices: %v", err)
		return
	}

	// FIXED: Driver set to 'sqlite' to match modernc.org
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		log.Printf("Failed to open main database: %v", err)
		return
	}
	defer db.Close()
	
	// FIXED: Optimization parameters to prevent database-locking on Windows
	db.SetMaxOpenConns(1)

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

func getUserTables(db *sql.DB) ([]string, error) {
	rows, err := db.Query("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%';")
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

func computeUserPortfolio(db *sql.DB, username string, prices map[string]float64) (Portfolio, error) {
	query := fmt.Sprintf(`SELECT Type, Stock, Quantity, Price_TT FROM "%s"`, username)
	rows, err := db.Query(query)
	if err != nil {
		return Portfolio{}, err
	}
	defer rows.Close()

	cash := StartingCash
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
	overallPL := totalNetWorth - StartingCash

	return Portfolio{
		Username:          username,
		StartingCash:      StartingCash,
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

func saveToJSON(reports []Portfolio) {
	jsonFile, err := os.Create("tpl.json")
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
	// FIXED: Changed driver token to 'sqlite' for modernc compatibility
	db, err := sql.Open("sqlite", AnalyticsDB)
	if err != nil {
		log.Printf("Database connection execution block error: %v", err)
		return
	}
	defer db.Close()

	_, _ = db.Exec("PRAGMA synchronous = OFF; PRAGMA journal_mode = MEMORY;")

	for _, r := range reports {
		createTableStmt := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS "%s" (
			metric_key TEXT PRIMARY KEY,
			metric_value TEXT
		);`, r.Username)

		if _, err := db.Exec(createTableStmt); err != nil {
			log.Printf("Failed dynamically building target summary entity structure: %v", err)
			continue
		}

		_, _ = db.Exec(fmt.Sprintf(`DELETE FROM "%s"`, r.Username))

		stmt, err := db.Prepare(fmt.Sprintf(`INSERT INTO "%s" (metric_key, metric_value) VALUES (?, ?)`, r.Username))
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
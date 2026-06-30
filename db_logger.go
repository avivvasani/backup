package main

import (
	"crypto/md5"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"time"

	_ "modernc.org/sqlite"
)

const (
	WATCH_FILE = "prices.json"
	DB_FILE    = "stock_history.db"
	TICK_RATE  = 500 * time.Millisecond // Check for file modifications twice a second
)

// --- COLORS ---
const (
	Reset  = "\033[0m"
	Green  = "\033[32m"
	Cyan   = "\033[36m"
	Blue   = "\033[34m"
	Yellow = "\033[33m"
	Gray   = "\033[90m"
)

type FinalOutput struct {
	LastUpdated string                            `json:"last_updated"`
	Stocks      map[string]map[string]interface{} `json:"stocks"`
}

// Computes MD5 hash of the file to see if content changed without heavy disk parsing
func getFileHash(filepath string) (string, error) {
	file, err := os.Open(filepath)
	if err != nil {
		return "", err
	}
	defer file.Close()

	hash := md5.New()
	if _, err := io.Copy(hash, file); err != nil {
		return "", err
	}
	return fmt.Sprintf("%x", hash.Sum(nil)), nil
}

func main() {
	fmt.Print("\033[H\033[2J") // Clear screen
	fmt.Printf("%sSQLite3 Analytics & Logging Engine Initialized%s\n", Blue, Reset)
	fmt.Printf("Watching: %s | Database: %s | Pulse: %v\n\n", WATCH_FILE, DB_FILE, TICK_RATE)

	// 1. Initialize/Connect to SQLite3 Database using modernc driver
	db, err := sql.Open("sqlite", DB_FILE)
	if err != nil {
		fmt.Printf("[\033[31mFATAL%s] Database connection failed: %v\n", Reset, err)
		return
	}
	defer db.Close()

	// Optimize SQLite settings for high-frequency writes during the competition
	_, _ = db.Exec("PRAGMA journal_mode=WAL;")
	_, _ = db.Exec("PRAGMA synchronous=NORMAL;")

	var lastHash string
	createdTables := make(map[string]bool)

	ticker := time.NewTicker(TICK_RATE)
	defer ticker.Stop()

	fmt.Printf("%s[SYSTEM] Standing by for incoming price updates...%s\n", Gray, Reset)

	for range ticker.C {
		currentHash, err := getFileHash(WATCH_FILE)
		if err != nil {
			// File might not be generated yet by the fetcher; skip silently until it appears
			continue
		}

		// If hash matches, nothing changed. skip processing loop
		if currentHash == lastHash {
			continue
		}

		// File changed! Start parsing
		lastHash = currentHash

		fileBytes, err := os.ReadFile(WATCH_FILE)
		if err != nil {
			continue
		}

		var input FinalOutput
		if err := json.Unmarshal(fileBytes, &input); err != nil {
			fmt.Printf("%s[WARN] Failed to unmarshal JSON payload: %v%s\n", Yellow, err, Reset)
			continue
		}

		// --- TIMESTAMP FORMATTING FIX ---
		// Parse the RFC3339 time from the fetcher and transform it to HH:MM:SS
		formattedTime := time.Now().Format("15:04:05") // Fallback to local system time if parse fails
		if parsedTime, err := time.Parse(time.RFC3339, input.LastUpdated); err == nil {
			formattedTime = parsedTime.Format("15:04:05")
		}

		tx, err := db.Begin()
		if err != nil {
			continue
		}

		logCount := 0

		// Flatten categories to access individual tickers
		for _, stocksMap := range input.Stocks {
			for tickerSymbol, priceVal := range stocksMap {
				// Safely extract price as float64 from JSON interface
				price, ok := priceVal.(float64)
				if !ok {
					continue
				}

				// 2. Dynamic Table Creation if it doesn't exist yet
				if !createdTables[tickerSymbol] {
					// Use double quotes around table name so symbols with special characters don't break SQL syntax
					query := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS "%s" (
						id INTEGER PRIMARY KEY AUTOINCREMENT,
						timestamp TEXT NOT NULL,
						price REAL NOT NULL
					);`, tickerSymbol)
					
					_, err := tx.Exec(query)
					if err != nil {
						fmt.Printf("%s[ERROR] Failed to create table for %s: %v%s\n", Yellow, tickerSymbol, err, Reset)
						continue
					}
					createdTables[tickerSymbol] = true
				}

				// 3. Log data timestamp point inside transaction using our HH:MM:SS format
				insertQuery := fmt.Sprintf(`INSERT INTO "%s" (timestamp, price) VALUES (?, ?);`, tickerSymbol)
				_, err = tx.Exec(insertQuery, formattedTime, price)
				if err == nil {
					logCount++
				}
			}
		}

		// Commit all logs simultaneously in a single atomic storage write block
		if err := tx.Commit(); err == nil && logCount > 0 {
			fmt.Printf("%s[%s]%s Captured change frame: Logged %s%d tickers%s to database with HH:MM:SS format.\n",
				Gray, time.Now().Format("15:04:05"), Reset, Green, logCount, Reset)
		} else {
			_ = tx.Rollback()
		}
	}
}
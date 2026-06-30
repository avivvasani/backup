package main

import (
	"database/sql"
	"encoding/csv"
	"fmt"
	"os"
	"strings"
	"strconv"

	_ "modernc.org/sqlite"
)

const (
	TX_DB   = "transactions.db"
	HIST_DB = "stock_history.db"
	CSV_OUT = "discrepancies.log.csv"
)

// --- COLORS ---
const (
	Reset  = "\033[0m"
	Green  = "\033[32m"
	Blue   = "\033[34m"
	Yellow = "\033[33m"
	Red    = "\033[31m"
	Cyan   = "\033[36m"
	Gray   = "\033[90m"
)

type Transaction struct {
	ID        string
	Timestamp string
	Action    string
	Stock     string
	Price     float64
	Player    string
}

func main() {
	fmt.Print("\033[H\033[2J") // Clear terminal space
	fmt.Printf("%sUniversal Multi-Table Pricing Auditor%s\n", Blue, Reset)
	fmt.Printf("Analyzing ALL tables in: %s <-> Verifying with: %s\n\n", TX_DB, HIST_DB)

	// 1. Connect to transactions.db
	txDB, err := sql.Open("sqlite", TX_DB)
	if err != nil {
		fmt.Printf("%s[FATAL] Failed to connect to %s: %v%s\n", Red, TX_DB, err, Reset)
		return
	}
	defer txDB.Close()

	// 2. Connect to stock_history.db
	histDB, err := sql.Open("sqlite", HIST_DB)
	if err != nil {
		fmt.Printf("%s[FATAL] Failed to connect to %s: %v%s\n", Red, HIST_DB, err, Reset)
		return
	}
	defer histDB.Close()

	// --- FETCH ALL USER TABLES WITHOUT EXCEPTION ---
	tableRows, err := txDB.Query("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
	if err != nil {
		fmt.Printf("%s[FATAL] Could not query table catalog from %s: %v%s\n", Red, TX_DB, err, Reset)
		return
	}
	defer tableRows.Close()

	var tables []string
	for tableRows.Next() {
		var name string
		if err := tableRows.Scan(&name); err == nil {
			tables = append(tables, name)
		}
	}

	if len(tables) == 0 {
		fmt.Printf("%s[FATAL] Zero tables detected inside %s!%s\n", Red, TX_DB, Reset)
		return
	}

	fmt.Printf("%s[SYSTEM] Target matrix locked. Scanning %d total tables...%s\n\n", Gray, len(tables), Reset)

	// Prepare dynamic CSV output handle
	csvFile, err := os.Create(CSV_OUT)
	if err != nil {
		fmt.Printf("%s[ERROR] Cannot generate output matrix CSV: %v%s\n", Red, err, Reset)
		return
	}
	defer csvFile.Close()

	writer := csv.NewWriter(csvFile)
	defer writer.Flush()

	// Write CSV Headers
	_ = writer.Write([]string{"Source Table", "Transaction ID", "Timestamp", "Player Name", "Stock Name", "Transaction Price", "Actual Database Price", "Status"})

	mismatchCount := 0
	totalChecked := 0

	fmt.Printf("%s%-22s %-6s %-12s %-12s %-8s %-10s %-10s %s\n", Blue, "TABLE", "TX_ID", "TIME", "PLAYER", "STOCK", "TX_PRICE", "ACT_PRICE", "ALERT"+Reset)
	fmt.Println(strings.Repeat("-", 105))

	// Loop through every single table in the database
	for _, tableName := range tables {
		columns, err := getTableColumns(txDB, tableName)
		if err != nil {
			fmt.Printf("%s[SKIP] Failed to read schema info for table '%s': %v%s\n", Yellow, tableName, err, Reset)
			continue
		}

		// Adaptive lookup mapping strategy (case-insensitive dictionary match)
		idCol := findColumn(columns, []string{"id", "transaction_id", "tx_id", "rowid", "uid", "uuid"})
		timeCol := findColumn(columns, []string{"timestamp", "time", "date", "created_at", "datetime", "logged_at"})
		actionCol := findColumn(columns, []string{"action", "type", "operation", "side", "method"})
		stockCol := findColumn(columns, []string{"stock", "ticker", "symbol", "asset", "item", "stock_name"})
		priceCol := findColumn(columns, []string{"price", "amount", "cost", "value", "rate", "exec_price"})
		playerCol := findColumn(columns, []string{"player", "user", "username", "account", "buyer", "trader", "client"})

		// Guard rails: Check if this specific table actually looks like a transaction log
		if timeCol == "" || stockCol == "" || priceCol == "" {
			// Not a transaction log table, skip silently
			continue
		}

		// Dynamically construct selection tokens
		var selectFields []string
		var scanDestinations []interface{}
		var tx Transaction

		if idCol != "" {
			selectFields = append(selectFields, fmt.Sprintf(`"%s"`, idCol))
			scanDestinations = append(scanDestinations, &tx.ID)
		} else {
			tx.ID = "N/A"
		}

		selectFields = append(selectFields, fmt.Sprintf(`"%s"`, timeCol), fmt.Sprintf(`"%s"`, stockCol), fmt.Sprintf(`"%s"`, priceCol))
		scanDestinations = append(scanDestinations, &tx.Timestamp, &tx.Stock, &tx.Price)

		if actionCol != "" {
			selectFields = append(selectFields, fmt.Sprintf(`"%s"`, actionCol))
			scanDestinations = append(scanDestinations, &tx.Action)
		}
		if playerCol != "" {
			selectFields = append(selectFields, fmt.Sprintf(`"%s"`, playerCol))
			scanDestinations = append(scanDestinations, &tx.Player)
		}

		query := fmt.Sprintf(`SELECT %s FROM "%s"`, strings.Join(selectFields, ", "), tableName)
		rows, err := txDB.Query(query)
		if err != nil {
			continue
		}

		// Process rows inside current table
		for rows.Next() {
			tx.Action = "UNKNOWN"
			tx.Player = "UNKNOWN"

			if err := rows.Scan(scanDestinations...); err != nil {
				continue
			}
			totalChecked++

			// Check cross-reference inside stock_history.db
			var actualPrice float64
			historyQuery := fmt.Sprintf(`SELECT price FROM "%s" WHERE timestamp = ? LIMIT 1`, tx.Stock)

			err = histDB.QueryRow(historyQuery, tx.Timestamp).Scan(&actualPrice)
			if err != nil {
				mismatchCount++
				_ = writer.Write([]string{tableName, tx.ID, tx.Timestamp, tx.Player, tx.Stock, strconv.FormatFloat(tx.Price, 'f', 2, 64), "MISSING", "TIMESTAMP_NOT_FOUND"})

				fmt.Printf("%s%-22s %-6s %-12s %-12s %-8s %-10.2f %-10s %-15s%s\n",
					Yellow, truncateString(tableName, 22), tx.ID, tx.Timestamp, tx.Player, tx.Stock, tx.Price, "N/A", "[TIME MISSING]", Reset)
				continue
			}

			// Validate values match boundaries
			if tx.Price != actualPrice {
				mismatchCount++
				_ = writer.Write([]string{tableName, tx.ID, tx.Timestamp, tx.Player, tx.Stock, strconv.FormatFloat(tx.Price, 'f', 2, 64), strconv.FormatFloat(actualPrice, 'f', 2, 64), "PRICE_MISMATCH"})

				fmt.Printf("%s%-22s %-6s %-12s %-12s %-8s %-10.2f %-10.2f %-15s%s\n",
					Red, truncateString(tableName, 22), tx.ID, tx.Timestamp, tx.Player, tx.Stock, tx.Price, actualPrice, "[BAD PRICE]", Reset)
			}
		}
		rows.Close()
	}

	fmt.Println(strings.Repeat("-", 105))
	if mismatchCount == 0 {
		fmt.Printf("\n%s[SUCCESS] Verification complete. Scanned %d total rows across all recognized tables. No fraud vectors detected!%s\n", Green, totalChecked, Reset)
	} else {
		fmt.Printf("\n%s[AUDIT COMPLETED] Flagged %d total anomalies out of %d checked records.%s\n", Red, mismatchCount, totalChecked, Reset)
		fmt.Printf("Consolidated audit metrics flushed into production log target: %s%s%s\n", Cyan, CSV_OUT, Reset)
	}
}

// Interrogate the table's actual metadata configuration layout
func getTableColumns(db *sql.DB, tableName string) ([]string, error) {
	rows, err := db.Query(fmt.Sprintf(`PRAGMA table_info("%s")`, tableName))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var columns []string
	for rows.Next() {
		var cid int
		var name, ctype string
		var notnull, pk int
		var dfltVal interface{}
		if err := rows.Scan(&cid, &name, &ctype, &notnull, &dfltVal, &pk); err == nil {
			columns = append(columns, name)
		}
	}
	return columns, nil
}

// Check case-insensitive variant options inside the slice
func findColumn(columns []string, targets []string) string {
	for _, col := range columns {
		lowerCol := strings.ToLower(col)
		for _, target := range targets {
			if lowerCol == strings.ToLower(target) {
				return col
			}
		}
	}
	return ""
}

// Utility string shortener for uniform terminal output columns formatting
func truncateString(str string, num int) string {
	if len(str) > num {
		return str[0:num-3] + "..."
	}
	return str
}
package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"
)

const (
	CONCURRENCY    = 6                // Perfect line overhead for rapid cycles
	STAGGER_DELAY  = 5 * time.Millisecond // Micro-stagger to break atomic concurrency blocks
	FETCH_INTERVAL = 5 * time.Second  // 5-second blazing updates for a live interface
	CONFIG_FILE    = "stocks.json"    // Dynamic stock target file
	OUT_FILE       = "prices.json"    // Production matrix flush target
)

// --- COLORS ---
const (
	Reset  = "\033[0m"
	Green  = "\033[32m"
	Cyan   = "\033[36m"
	Blue   = "\033[34m"
	Gray   = "\033[90m"
	Yellow = "\033[33m"
	Red    = "\033[31m"
)

var (
	transport = &http.Transport{
		MaxIdleConns:        50,
		MaxIdleConnsPerHost: 10,
		IdleConnTimeout:     90 * time.Second,
	}
	client = &http.Client{
		Transport: transport,
		Timeout:   2 * time.Second, // Drop dead packets immediately to protect the 5s window
	}
)

type PriceResponse struct {
	Chart struct {
		Result []struct {
			Meta struct {
				Price float64 `json:"regularMarketPrice"`
			} `json:"meta"`
		} `json:"result"`
	} `json:"chart"`
}

type FinalOutput struct {
	LastUpdated string                            `json:"last_updated"`
	Stocks      map[string]map[string]interface{} `json:"stocks"`
}

// Loads structural mappings dynamically from stocks.json
func loadStocksConfig() map[string][]string {
	file, err := os.ReadFile(CONFIG_FILE)
	if err != nil {
		fmt.Printf("%s[FATAL] Could not read %s: %v%s\n", Red, CONFIG_FILE, err, Reset)
		os.Exit(1)
	}

	var config map[string][]string
	if err := json.Unmarshal(file, &config); err != nil {
		fmt.Printf("%s[FATAL] Invalid JSON structure in %s: %v%s\n", Red, CONFIG_FILE, err, Reset)
		os.Exit(1)
	}
	return config
}

// Seed the initial map configuration from prices.json if it exists
func loadExistingPrices(categories map[string][]string) map[string]map[string]interface{} {
	data := make(map[string]map[string]interface{})
	for cat := range categories {
		data[cat] = make(map[string]interface{})
	}

	file, err := os.ReadFile(OUT_FILE)
	if err != nil {
		return data // Safely start with empty values if file doesn't exist yet
	}

	var existing FinalOutput
	if err := json.Unmarshal(file, &existing); err == nil {
		for cat, stocks := range existing.Stocks {
			if _, exists := data[cat]; exists {
				for sym, price := range stocks {
					data[cat][sym] = price
				}
			}
		}
	}
	return data
}

func main() {
	fmt.Print("\033[H\033[2J") // Clear terminal space completely

	// 1. Dynamic Extraction from external Configuration file
	stocksByCategory := loadStocksConfig()

	tickerToCategories := make(map[string][]string)
	var uniqueTickers []string
	seen := make(map[string]bool)

	for cat, syms := range stocksByCategory {
		for _, sym := range syms {
			if !seen[sym] {
				seen[sym] = true
				uniqueTickers = append(uniqueTickers, sym)
			}
			tickerToCategories[sym] = append(tickerToCategories[sym], cat)
		}
	}

	// 2. Hydrate database from disk state retention asset
	data := loadExistingPrices(stocksByCategory)

	fetchCount := 0
	ticker := time.NewTicker(FETCH_INTERVAL)
	defer ticker.Stop()

	for {
		cycleStart := time.Now()
		fmt.Print("\033[H")
		fmt.Printf("%sNSE Competition High-Frequency Engine%s\n", Blue, Reset)
		fmt.Printf("Targets Loaded: %s%d Categories%s | Unique Tick: %s%d%s | Rate: %v\n\n",
			Cyan, len(stocksByCategory), Reset, Green, len(uniqueTickers), Reset, FETCH_INTERVAL)

		var wg sync.WaitGroup
		sem := make(chan struct{}, CONCURRENCY)
		mu := sync.Mutex{}

		successCount := 0
		var failedTickers []string

		for _, sym := range uniqueTickers {
			wg.Add(1)
			go func(symbol string) {
				defer wg.Done()
				sem <- struct{}{}
				defer func() { <-sem }()

				escapedSymbol := url.PathEscape(symbol + ".NS")
				apiURL := fmt.Sprintf("https://query2.finance.yahoo.com/v8/finance/chart/%s", escapedSymbol)

				req, _ := http.NewRequest("GET", apiURL, nil)
				req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
				req.Header.Set("Accept", "application/json")
				req.Header.Set("Connection", "keep-alive")

				resp, err := client.Do(req)
				if err != nil {
					mu.Lock()
					failedTickers = append(failedTickers, symbol)
					mu.Unlock()
					return
				}
				defer func() {
					_, _ = io.Copy(io.Discard, resp.Body)
					resp.Body.Close()
				}()

				if resp.StatusCode == http.StatusOK {
					var pr PriceResponse
					if err := json.NewDecoder(resp.Body).Decode(&pr); err == nil && len(pr.Chart.Result) > 0 {
						price := pr.Chart.Result[0].Meta.Price
						if price > 0 {
							mu.Lock()
							successCount++
							for _, cat := range tickerToCategories[symbol] {
								data[cat][symbol] = price
							}
							mu.Unlock()
							return
						}
					}
				}

				mu.Lock()
				failedTickers = append(failedTickers, symbol)
				mu.Unlock()
			}(sym)

			time.Sleep(STAGGER_DELAY)
		}

		wg.Wait()
		fetchCount++
		fetchDuration := time.Since(cycleStart)

		// Overwrite prices.json cleanly (retains previous pricing on random failure drops)
		output := FinalOutput{
			LastUpdated: time.Now().Format(time.RFC3339),
			Stocks:      data,
		}
		prettyJSON, _ := json.MarshalIndent(output, "", "  ")
		_ = os.WriteFile(OUT_FILE, prettyJSON, 0644)

		// Print Summary Matrix Info
		fmt.Printf("\r\033[2K%s[%d]%s Matrix Processed: %s%v%s | Alive: %s%d%s | Missed/Dropped: %s%d%s\n",
			Gray, fetchCount, Reset, Cyan, fetchDuration, Reset, Green, successCount, Reset, Yellow, len(failedTickers), Reset)

		// DYNAMIC FIX: Cleanly show which specific tickers dropped this frame right under the matrix status
		fmt.Print("\033[2K") // Clear target line
		if len(failedTickers) > 0 {
			fmt.Printf("%s-> Dropped/Missed: %s%s\n", Red, strings.Join(failedTickers, ", "), Reset)
		} else {
			fmt.Printf("%s-> All pipelines healthy (0 drops)%s\n", Green, Reset)
		}

		// High-resolution progress interval wait routine
		for {
			elapsed := time.Since(cycleStart)
			if elapsed >= FETCH_INTERVAL {
				break
			}
			
			timeLeft := (FETCH_INTERVAL - elapsed).Seconds()
			fmt.Printf("\r\033[2K%sNext pipeline burst in: %.1fs remaining%s", Gray, timeLeft, Reset)
			time.Sleep(100 * time.Millisecond)
		}
		fmt.Print("\033[2K\r")

		<-ticker.C
	}
}
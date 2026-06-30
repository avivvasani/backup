package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"sync"
	"time"
)

// --- CONFIG ---
const (
	CONCURRENCY    = 8                // Optimized overlap to finish in ~0.9s
	FETCH_INTERVAL = 5 * time.Second  // Hard 5-second cycle cap
	OUT_FILE       = "prices.json"
)

// --- COLORS ---
const (
	Reset = "\033[0m"
	Green = "\033[32m"
	Cyan  = "\033[36m"
	Blue  = "\033[34m"
	Gray  = "\033[90m"
)

var (
	transport = &http.Transport{
		MaxIdleConns:        100,
		MaxIdleConnsPerHost: 10,             // Slightly above concurrency for overhead buffer
		IdleConnTimeout:     90 * time.Second,
	}
	client = &http.Client{
		Transport: transport,
		Timeout:   2 * time.Second, // Drop lagging connections quickly to defend the loop window
	}
)

var StocksByCategory = map[string][]string{
	"Banking & Financials":         {"BANDHANBNK", "RBLBANK", "YESBANK", "IDFCFIRSTB", "INDUSINDBK", "FEDERALBNK", "BANKBARODA", "PNB", "CANBK", "UCOBANK", "IOB", "IREDA", "IFCI", "HUDCO", "PFC", "RECLTD", "POONAWALLA", "M&MFIN", "MANAPPURAM", "ANGELONE", "BSE", "MCX", "CDSL", "IEX"},
	"Defense & PSU Momentum":       {"IDEAFORGE", "PARAS", "MTARTECH", "MAZDOCK", "COCHINSHIP", "GRSE", "HAL", "BDL", "BEL", "DCXINDIA"},
	"Energy & Renewables":          {"SUZLON", "ADANIPOWER", "ADANIGREEN", "GENSOL", "IREDA", "JSWENERGY", "TATAPOWER", "JPPOWER", "SJVN", "NHPC"},
	"Consumer & Internet":          {"PAYTM", "POLICYBZR", "NYKAA", "HONASA", "DELHIVERY", "TRENT", "ETHOSLTD", "MAPMYINDIA"},
	"IT & Digital":                 {"TANLA", "NETWEB", "KPITTECH", "PERSISTENT", "COFORGE", "TATAELXSI", "63MOONS", "HAPPSTMNDS", "NEWGEN"},
	"Metals & Commodities":         {"HINDCOPPER", "VEDL", "JINDALSTEL", "SAIL", "TATASTEEL", "NMDC", "MOIL", "NATIONALUM"},
	"Chemicals & Manufacturing":    {"FLUOROCHEM", "DEEPAKNTR", "NAVINFLUOR", "SRF", "PIIND", "FACT", "GSFC", "GNFC"},
	"Railways & Infra":             {"RVNL", "IRFC", "RAILTEL", "IRCON", "TITAGARH", "KEC", "DELHIVERY"},
	"Telecom & Media":              {"IDEA", "HFCL", "GTLINFRA", "ZEEL", "NETWORK18"},
	"Auto & EV":                    {"OLECTRA", "SONACOMS", "UNOMINDA", "RKFORGE", "FORCEMOT"},
	"Smallcap Financial Chaos":     {"CSBBANK", "DCBBANK", "DHANBANK", "ESAFSFB", "J&KBANK", "KARURVYSYA", "SOUTHBANK", "SURYODAY", "UTKARSHBNK", "PSB", "IDBI", "INDIANB"},
	"Speculative Power & Infra":    {"BORORENEW", "GREENPANEL", "TORNTPOWER", "PATELENG", "ASHOKA", "RKEC", "KNRCON", "ENGINERSIN", "NCC", "NBCC"},
	"Momentum Chemicals":           {"CLEAN", "VINATIORGA", "ROSSARI", "SUDARSCHEM", "SUMICHEM", "NOCIL", "ATUL", "TATACHEM", "SHARDACROP", "UPL"},
	"High-Move Consumer & Retail":  {"ABFRL", "DMART", "SENCO", "KALYANKJIL", "TRENT", "ETHOSLTD", "MAPMYINDIA", "HONASA"},
	"Fast-Moving Industrials":      {"KAYNES", "SYRMA", "DIXON", "KEI", "POLYCAB", "TRITURBINE", "ELGIEQUIP", "VOLTAS"},
	"Midcap Tech Momentum":         {"DSSL", "KELLTONTEC", "SAKSOFT", "RAMCOSYS", "ROUTE", "NELCO", "INTELLECT", "ZENSARTECH", "CYIENT"},
	"Commodities & Cyclicals":      {"ELECTCAST", "SUNFLAG", "WELCORP", "APLAPOLLO", "RATNAMANI", "GMDC", "KIOCL", "MIDHANI"},
	"Railway & Logistics Movers":   {"ALLCARGO", "GATEWAY", "TCI", "JWL", "TEXRAIL", "CONCOR", "BLUEDART"},
	"Real Estate Momentum":         {"DLF", "LODHA", "GODREJPROP", "PRESTIGE", "OBEROIRLTY", "PHOENIXLTD", "BRIGADE", "SOBHA", "SUNTECK"},
	"Telecom & Entertainment":      {"ITI", "INDUSTOWER", "PVRINOX", "SAREGAMA", "TIPSMUSIC", "SUNTV", "ZEEL"},
	"Agri & Sugar Cycles":          {"RENUKA", "TRIVENI", "BALRAMCHIN", "DHAMPURSUG", "BANARISUG", "DALMIASUG", "EIDPARRY"},
	"Auto & EV Beta":               {"VARROC", "FIEMIND", "ENDURANCE", "SUPRAJIT", "SUBROS", "JAMNAAUTO", "TVSMOTOR", "ASHOKLEY"},
}

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

func renderProgressBar(timeLeft time.Duration) {
	width := 30
	percent := timeLeft.Seconds() / FETCH_INTERVAL.Seconds()
	if percent < 0 {
		percent = 0
	}
	filledLength := int(float64(width) * percent)

	bar := ""
	for i := 0; i < width-filledLength; i++ {
		bar += "█"
	}
	for i := 0; i < filledLength; i++ {
		bar += "░"
	}
	fmt.Printf("\r%sNext cycle fetch in: [%s] %.1fs remaining%s", Gray, bar, timeLeft.Seconds(), Reset)
}

func main() {
	tickerToCategories := make(map[string][]string)
	var uniqueTickers []string
	seen := make(map[string]bool)

	for cat, syms := range StocksByCategory {
		for _, sym := range syms {
			if !seen[sym] {
				seen[sym] = true
				uniqueTickers = append(uniqueTickers, sym)
			}
			tickerToCategories[sym] = append(tickerToCategories[sym], cat)
		}
	}

	fetchCount := 0
	fmt.Print("\033[H\033[2J")

	ticker := time.NewTicker(FETCH_INTERVAL)
	defer ticker.Stop()

	for {
		cycleStart := time.Now()
		fmt.Print("\033[H")
		fmt.Printf("%sNSE Stocks' Prices Fetching Production Daemon%s\n", Blue, Reset)
		fmt.Printf("Unique Stocks: %s%d%s | Active Workers: %s%d %s\n\n", Green, len(uniqueTickers), Reset, Cyan, CONCURRENCY, Reset)

		var wg sync.WaitGroup
		sem := make(chan struct{}, CONCURRENCY)
		mu := sync.Mutex{}

		data := make(map[string]map[string]interface{})
		for cat := range StocksByCategory {
			data[cat] = make(map[string]interface{})
		}

		for _, sym := range uniqueTickers {
			wg.Add(1)
			go func(symbol string) {
				defer wg.Done()
				sem <- struct{}{}
				defer func() { <-sem }()

				escapedSymbol := url.PathEscape(symbol + ".NS")
				apiURL := fmt.Sprintf("https://query2.finance.yahoo.com/v8/finance/chart/%s", escapedSymbol)

				req, _ := http.NewRequest("GET", apiURL, nil)
				req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")
				req.Header.Set("Accept", "application/json")
				req.Header.Set("Connection", "keep-alive")

				resp, err := client.Do(req)
				if err != nil {
					return
				}
				// CRITICAL FIX: Empty buffer completely and close connection body to prevent memory leak crashes
				defer func() {
					_, _ = io.Copy(io.Discard, resp.Body)
					resp.Body.Close()
				}()

				if resp.StatusCode == http.StatusOK {
					var pr PriceResponse
					if err := json.NewDecoder(resp.Body).Decode(&pr); err == nil {
						// PANIC GUARD: Verify array size exists before accessing elements
						if len(pr.Chart.Result) > 0 {
							price := pr.Chart.Result[0].Meta.Price
							if price > 0 {
								mu.Lock()
								for _, cat := range tickerToCategories[symbol] {
									data[cat][symbol] = price
								}
								mu.Unlock()
							}
						}
					}
				}
			}(sym)
		}

		wg.Wait()
		fetchCount++
		fetchDuration := time.Since(cycleStart)

		output := FinalOutput{
			LastUpdated: time.Now().Format(time.RFC3339),
			Stocks:      data,
		}
		prettyJSON, _ := json.MarshalIndent(output, "", "  ")
		_ = os.WriteFile(OUT_FILE, prettyJSON, 0644)

		fmt.Printf("\r\033[2K%s[%d]%s Global map flushed cleanly in: %s%v%s\n", Gray, fetchCount, Reset, Cyan, fetchDuration, Reset)

		for {
			elapsed := time.Since(cycleStart)
			if elapsed >= FETCH_INTERVAL {
				break
			}
			renderProgressBar(FETCH_INTERVAL - elapsed)
			time.Sleep(100 * time.Millisecond)
		}
		fmt.Print("\033[2K\r")

		<-ticker.C
	}
}
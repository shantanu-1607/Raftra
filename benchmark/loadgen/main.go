package main

import (
	"bytes"
	"flag"
	"fmt"
	"io"
	"math"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// ANSI colors & styling
const (
	colorReset = "\033[0m"
	colorBold  = "\033[1m"
	colorDim   = "\033[2m"
	colorRed   = "\033[31m"
	colorGreen = "\033[32m"
	colorCyan  = "\033[36m"
)

// hslToRGB converts HSL values to RGB (0-255).
func hslToRGB(h, s, l float64) (int, int, int) {
	c := (1 - math.Abs(2*l-1)) * s
	hp := h / 60.0
	x := c * (1 - math.Abs(math.Mod(hp, 2)-1))
	m := l - c/2

	var r, g, b float64
	switch {
	case hp < 1:
		r, g, b = c, x, 0
	case hp < 2:
		r, g, b = x, c, 0
	case hp < 3:
		r, g, b = 0, c, x
	case hp < 4:
		r, g, b = 0, x, c
	case hp < 5:
		r, g, b = x, 0, c
	default:
		r, g, b = c, 0, x
	}
	clamp := func(v int) int {
		if v < 0 {
			return 0
		}
		if v > 255 {
			return 255
		}
		return v
	}
	return clamp(int((r + m) * 255)), clamp(int((g + m) * 255)), clamp(int((b + m) * 255))
}

func rgb(r, g, b int) string {
	return fmt.Sprintf("\033[38;2;%d;%d;%dm", r, g, b)
}

func gradientText(text string, hueStart float64) string {
	var sb strings.Builder
	runes := []rune(text)
	n := len(runes)
	if n == 0 {
		return ""
	}
	for i, ch := range runes {
		t := float64(i) / float64(n)
		hue := math.Mod(hueStart+t*50.0, 360.0)
		r, g, b := hslToRGB(hue, 0.75, 0.65)
		sb.WriteString(rgb(r, g, b))
		sb.WriteRune(ch)
	}
	sb.WriteString(colorReset)
	return sb.String()
}

func printBanner() {
	banner := []string{
		`  ██████╗  █████╗ ███████╗████████╗██████╗  █████╗ `,
		`  ██╔══██╗██╔══██╗██╔════╝╚══██╔══╝██╔══██╗██╔══██╗`,
		`  ██████╔╝███████║█████╗     ██║   ██████╔╝███████║`,
		`  ██╔══██╗██╔══██║██╔══╝     ██║   ██╔══██╗██╔══██║`,
		`  ██║  ██║██║  ██║██║        ██║   ██║  ██║██║  ██║`,
		`  ╚═╝  ╚═╝╚═╝  ╚═╝╚═╝        ╚═╝   ╚═╝  ╚═╝╚═╝  ╚═╝`,
	}
	fmt.Println()
	for i, line := range banner {
		hue := 240.0 + float64(i)*7.0
		r, g, b := hslToRGB(hue, 0.75, 0.65)
		fmt.Printf("%s%s%s\n", rgb(r, g, b), line, colorReset)
	}
	fmt.Printf("\n  %s  •  High-Performance Workload Generator\n\n",
		gradientText("Raftra Load Generator", 250))
}

// translateRedirectURL bridges Docker internal container names (node1, node2, node3)
// to host-accessible ports when running loadgen from outside Docker.
func translateRedirectURL(targetURL *url.URL, initialURL *url.URL) *url.URL {
	host := initialURL.Hostname()
	if host == "localhost" || host == "127.0.0.1" {
		switch targetURL.Hostname() {
		case "node1":
			targetURL.Host = fmt.Sprintf("%s:8001", host)
		case "node2":
			targetURL.Host = fmt.Sprintf("%s:8002", host)
		case "node3":
			targetURL.Host = fmt.Sprintf("%s:8003", host)
		}
	}
	return targetURL
}

type workerResult struct {
	setLatencies []time.Duration
	getLatencies []time.Duration
	setErrors    int64
	getErrors    int64
}

func parseRatio(ratioStr string) (float64, error) {
	parts := strings.Split(ratioStr, ":")
	if len(parts) != 2 {
		return 0, fmt.Errorf("ratio must be formatted as SET:GET (e.g. 80:20)")
	}
	setWeight, err := strconv.ParseFloat(parts[0], 64)
	if err != nil {
		return 0, fmt.Errorf("invalid set ratio: %v", err)
	}
	getWeight, err := strconv.ParseFloat(parts[1], 64)
	if err != nil {
		return 0, fmt.Errorf("invalid get ratio: %v", err)
	}
	total := setWeight + getWeight
	if total <= 0 {
		return 0, fmt.Errorf("ratio sum must be greater than zero")
	}
	return setWeight / total, nil
}

func percentile(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(math.Ceil((p/100.0)*float64(len(sorted)))) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

func formatDuration(d time.Duration) string {
	if d < time.Millisecond {
		return fmt.Sprintf("%.2fµs", float64(d.Nanoseconds())/1000.0)
	}
	return fmt.Sprintf("%.2fms", float64(d.Microseconds())/1000.0)
}

func main() {
	addrFlag := flag.String("addr", "http://localhost:8001", "Target Raftra HTTP gateway address")
	opsFlag := flag.Int("ops", 1000, "Total number of operations to execute")
	concurrencyFlag := flag.Int("concurrency", 10, "Number of concurrent worker goroutines")
	ratioFlag := flag.String("ratio", "80:20", "Workload ratio formatted as SET:GET (e.g. 80:20, 50:50, 100:0)")
	keyPrefixFlag := flag.String("key-prefix", "bench_key", "Prefix for generated key names")
	flag.Parse()

	printBanner()

	setProbability, err := parseRatio(*ratioFlag)
	if err != nil {
		fmt.Fprintf(os.Stderr, "  %s✗ Error:%s %v\n", colorRed, colorReset, err)
		os.Exit(1)
	}

	targetAddr := *addrFlag
	if !strings.HasPrefix(targetAddr, "http://") && !strings.HasPrefix(targetAddr, "https://") {
		targetAddr = "http://" + targetAddr
	}
	targetAddr = strings.TrimRight(targetAddr, "/")

	parsedBase, err := url.Parse(targetAddr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "  %s✗ Error:%s Invalid target address: %v\n", colorRed, colorReset, err)
		os.Exit(1)
	}

	fmt.Printf("  Target Node:     %s%s%s\n", colorCyan, targetAddr, colorReset)
	fmt.Printf("  Total Ops:       %s%d%s\n", colorBold, *opsFlag, colorReset)
	fmt.Printf("  Concurrency:     %s%d workers%s\n", colorBold, *concurrencyFlag, colorReset)
	fmt.Printf("  Workload Ratio:  %s%s%s (%.0f%% SET / %.0f%% GET)\n",
		colorBold, *ratioFlag, colorReset, setProbability*100, (1-setProbability)*100)
	fmt.Println()

	var opCounter int64
	var completedCounter int64
	totalOps := int64(*opsFlag)
	concurrency := *concurrencyFlag

	results := make([]workerResult, concurrency)
	var wg sync.WaitGroup

	startTime := time.Now()

	for w := 0; w < concurrency; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			// High-performance HTTP client with connection reuse & redirect handling
			client := &http.Client{
				Timeout: 10 * time.Second,
				Transport: &http.Transport{
					MaxIdleConns:        2000,
					MaxIdleConnsPerHost: 2000,
					IdleConnTimeout:     30 * time.Second,
				},
				CheckRedirect: func(req *http.Request, via []*http.Request) error {
					return http.ErrUseLastResponse
				},
			}

			rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(workerID)*1000))
			res := &results[workerID]

			for {
				opNum := atomic.AddInt64(&opCounter, 1)
				if opNum > totalOps {
					break
				}

				key := fmt.Sprintf("%s_%d", *keyPrefixFlag, rng.Intn(1000))
				isSet := rng.Float64() < setProbability

				opStart := time.Now()
				var opErr error

				if isSet {
					val := fmt.Sprintf("val_%d_%d", workerID, opNum)
					opErr = executeHTTP(client, "PUT", targetAddr, "/api/v1/kv/"+key, []byte(val), parsedBase)
					elapsed := time.Since(opStart)
					if opErr != nil {
						res.setErrors++
					} else {
						res.setLatencies = append(res.setLatencies, elapsed)
					}
				} else {
					opErr = executeHTTP(client, "GET", targetAddr, "/api/v1/kv/"+key, nil, parsedBase)
					elapsed := time.Since(opStart)
					if opErr != nil {
						res.getErrors++
					} else {
						res.getLatencies = append(res.getLatencies, elapsed)
					}
				}

				atomic.AddInt64(&completedCounter, 1)
			}
		}(w)
	}

	wg.Wait()
	totalDuration := time.Since(startTime)

	// Aggregate worker stats
	var allSetLatencies []time.Duration
	var allGetLatencies []time.Duration
	var totalSetErrors int64
	var totalGetErrors int64

	for _, res := range results {
		allSetLatencies = append(allSetLatencies, res.setLatencies...)
		allGetLatencies = append(allGetLatencies, res.getLatencies...)
		totalSetErrors += res.setErrors
		totalGetErrors += res.getErrors
	}

	sort.Slice(allSetLatencies, func(i, j int) bool { return allSetLatencies[i] < allSetLatencies[j] })
	sort.Slice(allGetLatencies, func(i, j int) bool { return allGetLatencies[i] < allGetLatencies[j] })

	totalCompleted := len(allSetLatencies) + len(allGetLatencies)
	totalErrors := totalSetErrors + totalGetErrors
	totalThroughput := float64(totalCompleted) / totalDuration.Seconds()
	setThroughput := float64(len(allSetLatencies)) / totalDuration.Seconds()
	getThroughput := float64(len(allGetLatencies)) / totalDuration.Seconds()

	fmt.Println("  -------------------------------------------------------------")
	fmt.Println(gradientText("  BENCHMARK RESULTS", 240))
	fmt.Println("  -------------------------------------------------------------")
	fmt.Printf("  Total Operations:    %s%d%s\n", colorBold, totalCompleted+int(totalErrors), colorReset)
	fmt.Printf("  Duration:            %s%.2fs%s\n", colorBold, totalDuration.Seconds(), colorReset)
	fmt.Printf("  Throughput:          %s%.1f ops/sec%s\n", colorGreen, totalThroughput, colorReset)
	if len(allSetLatencies) > 0 {
		fmt.Printf("  SET Throughput:      %s%.1f ops/sec%s (%d ops)\n", colorCyan, setThroughput, colorReset, len(allSetLatencies))
	}
	if len(allGetLatencies) > 0 {
		fmt.Printf("  GET Throughput:      %s%.1f ops/sec%s (%d ops)\n", colorCyan, getThroughput, colorReset, len(allGetLatencies))
	}
	fmt.Println()

	if len(allSetLatencies) > 0 {
		fmt.Println("  Latency (SET - Quorum Replicated):")
		fmt.Printf("    P50:   %s%s%s\n", colorBold, formatDuration(percentile(allSetLatencies, 50)), colorReset)
		fmt.Printf("    P95:   %s%s%s\n", colorBold, formatDuration(percentile(allSetLatencies, 95)), colorReset)
		fmt.Printf("    P99:   %s%s%s\n", colorBold, formatDuration(percentile(allSetLatencies, 99)), colorReset)
		fmt.Printf("    Min:   %s\n", formatDuration(allSetLatencies[0]))
		fmt.Printf("    Max:   %s\n", formatDuration(allSetLatencies[len(allSetLatencies)-1]))
		fmt.Println()
	}

	if len(allGetLatencies) > 0 {
		fmt.Println("  Latency (GET - In-Memory Read):")
		fmt.Printf("    P50:   %s%s%s\n", colorBold, formatDuration(percentile(allGetLatencies, 50)), colorReset)
		fmt.Printf("    P95:   %s%s%s\n", colorBold, formatDuration(percentile(allGetLatencies, 95)), colorReset)
		fmt.Printf("    P99:   %s%s%s\n", colorBold, formatDuration(percentile(allGetLatencies, 99)), colorReset)
		fmt.Printf("    Min:   %s\n", formatDuration(allGetLatencies[0]))
		fmt.Printf("    Max:   %s\n", formatDuration(allGetLatencies[len(allGetLatencies)-1]))
		fmt.Println()
	}

	errorRate := (float64(totalErrors) / float64(totalOps)) * 100.0
	errColor := colorGreen
	if totalErrors > 0 {
		errColor = colorRed
	}
	fmt.Printf("  Errors:              %s%d (%.2f%%)%s\n", errColor, totalErrors, errorRate, colorReset)
	fmt.Println("  -------------------------------------------------------------")
	fmt.Println()
}

func executeHTTP(client *http.Client, method, baseURL, path string, body []byte, initialURL *url.URL) error {
	currentURL := baseURL + path
	redirects := 0
	const maxRedirects = 5

	for {
		var reader io.Reader
		if body != nil {
			reader = bytes.NewReader(body)
		}
		req, err := http.NewRequest(method, currentURL, reader)
		if err != nil {
			return err
		}

		resp, err := client.Do(req)
		if err != nil {
			return err
		}

		// Handle HTTP 307 Temporary Redirect to Raft Leader
		if resp.StatusCode == http.StatusTemporaryRedirect || resp.StatusCode == http.StatusPermanentRedirect {
			_ = resp.Body.Close()
			redirects++
			if redirects > maxRedirects {
				return fmt.Errorf("exceeded max redirects")
			}
			location := resp.Header.Get("Location")
			if location == "" {
				return fmt.Errorf("empty redirect location header")
			}
			locURL, err := url.Parse(location)
			if err != nil {
				return err
			}
			locURL = translateRedirectURL(locURL, initialURL)
			currentURL = locURL.String()
			continue
		}

		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()

		if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusNotFound {
			return fmt.Errorf("server returned status: %d", resp.StatusCode)
		}
		return nil
	}
}

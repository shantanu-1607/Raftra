package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

// Build-time settings. Release builds override them with
// -ldflags "-X main.version=... -X main.defaultAddr=..." (see .goreleaser.yaml);
// a local `make build` keeps these defaults.
var (
	version     = "0.1.0"
	defaultAddr = "http://localhost:8001"
)

// ============================================================
// ANSI Escape Codes
// ============================================================

const (
	colorReset  = "\033[0m"
	colorBold   = "\033[1m"
	colorDim    = "\033[2m"
	colorRed    = "\033[31m"
	colorGreen  = "\033[32m"
	colorYellow = "\033[33m"
	colorCyan   = "\033[36m"
	hideCursor  = "\033[?25l"
	showCursor  = "\033[?25h"
	clearLine   = "\033[2K"
)

// ============================================================
// Data Types (match the Raftra HTTP API responses)
// ============================================================

// StatusResponse matches the JSON structure returned by GET /status
type StatusResponse struct {
	Role        string `json:"role"`
	Term        uint64 `json:"term"`
	IsLeader    bool   `json:"is_leader"`
	LeaderID    string `json:"leader_id"`
	CommitIndex uint64 `json:"commit_index"`
	LastApplied uint64 `json:"last_applied"`
}

// KVResponse matches the JSON structure returned by /api/v1/kv/* endpoints
type KVResponse struct {
	Success bool   `json:"success,omitempty"`
	Key     string `json:"key,omitempty"`
	Value   string `json:"value,omitempty"`
	Deleted string `json:"deleted,omitempty"`
	Error   string `json:"error,omitempty"`
}

// ============================================================
// Gradient & Color Engine (24-bit True Color)
// ============================================================

// hslToRGB converts HSL color values (h: 0-360, s: 0-1, l: 0-1) to RGB (0-255).
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

	return clamp(int((r + m) * 255)), clamp(int((g + m) * 255)), clamp(int((b + m) * 255))
}

func clamp(v int) int {
	if v < 0 {
		return 0
	}
	if v > 255 {
		return 255
	}
	return v
}

// rgb returns an ANSI 24-bit foreground color escape sequence.
func rgb(r, g, b int) string {
	return fmt.Sprintf("\033[38;2;%d;%d;%dm", r, g, b)
}

// renderGradientLine applies a horizontal hue gradient across a string of text.
// hueStart is the starting hue (0-360), hueSpan is how much hue to sweep across the line.
func renderGradientLine(line string, hueStart, hueSpan, saturation, lightness float64) string {
	var sb strings.Builder
	runes := []rune(line)
	n := len(runes)
	if n == 0 {
		return ""
	}
	for i, ch := range runes {
		t := float64(i) / float64(n)
		hue := math.Mod(hueStart+t*hueSpan, 360.0)
		if hue < 0 {
			hue += 360
		}
		r, g, b := hslToRGB(hue, saturation, lightness)
		sb.WriteString(rgb(r, g, b))
		sb.WriteRune(ch)
	}
	sb.WriteString(colorReset)
	return sb.String()
}

// gradientText applies a gradient to an entire string (used for short labels).
func gradientText(text string, hueStart float64) string {
	return renderGradientLine(text, hueStart, 50.0, 0.75, 0.65)
}

// ============================================================
// ASCII Art Banner
// ============================================================

var banner = []string{
	`  ██████╗   █████╗  ███████╗ ████████╗ ██████╗   █████╗ `,
	`  ██╔══██╗ ██╔══██╗ ██╔════╝ ╚══██╔══╝ ██╔══██╗ ██╔══██╗`,
	`  ██████╔╝ ███████║ █████╗      ██║    ██████╔╝ ███████║`,
	`  ██╔══██╗ ██╔══██║ ██╔══╝      ██║    ██╔══██╗ ██╔══██║`,
	`  ██║  ██║ ██║  ██║ ██║         ██║    ██║  ██║ ██║  ██║`,
	`  ╚═╝  ╚═╝ ╚═╝  ╚═╝ ╚═╝         ╚═╝    ╚═╝  ╚═╝ ╚═╝  ╚═╝`,
}

// renderBannerFrame renders all banner lines with a given hue offset for animation.
func renderBannerFrame(hueOffset float64) string {
	var sb strings.Builder
	for i, line := range banner {
		// Each successive line starts slightly further along the hue wheel
		lineHue := hueOffset + float64(i)*8.0
		sb.WriteString(renderGradientLine(line, lineHue, 50.0, 0.75, 0.65))
		if i < len(banner)-1 {
			sb.WriteString("\n")
		}
	}
	return sb.String()
}

// ============================================================
// Animation System
// ============================================================

// animateBanner displays the banner with a line-by-line reveal, then shimmers the gradient.
func animateBanner() {
	fmt.Print(hideCursor)

	bannerHeight := len(banner)
	baseHue := 240.0 // Start from blue-purple

	// --- Phase 1: Reveal lines one-by-one ---
	for i := 0; i < bannerHeight; i++ {
		lineHue := baseHue + float64(i)*8.0
		fmt.Println(renderGradientLine(banner[i], lineHue, 50.0, 0.75, 0.65))
		time.Sleep(50 * time.Millisecond)
	}

	// --- Phase 2: Shimmer — slide the gradient hue across the banner ---
	shimmerFrames := 25
	for frame := 0; frame < shimmerFrames; frame++ {
		// Move cursor back up to the top of the banner
		fmt.Printf("\033[%dA", bannerHeight)

		hueOffset := baseHue + float64(frame)*4.0
		for i, line := range banner {
			lineHue := hueOffset + float64(i)*8.0
			fmt.Print(clearLine)
			fmt.Println(renderGradientLine(line, lineHue, 50.0, 0.75, 0.65))
		}
		time.Sleep(40 * time.Millisecond)
	}

	fmt.Print(showCursor)
}

// typeWriter prints text one character at a time for a cinematic feel.
func typeWriter(text string, charDelay time.Duration) {
	for _, ch := range text {
		fmt.Printf("%c", ch)
		time.Sleep(charDelay)
	}
	fmt.Println()
}

// ============================================================
// Loading Spinner (shown while HTTP requests are in-flight)
// ============================================================

var spinnerFrames = []string{"⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"}

type spinner struct {
	done chan struct{}
}

func startSpinner(label string) *spinner {
	s := &spinner{done: make(chan struct{})}
	go func() {
		i := 0
		for {
			select {
			case <-s.done:
				fmt.Printf("\r%s\r", clearLine)
				return
			default:
				hue := float64(235 + (i*5)%45)
				r, g, b := hslToRGB(hue, 0.8, 0.65)
				fmt.Printf("\r  %s%s%s %s%s%s",
					rgb(r, g, b), spinnerFrames[i%len(spinnerFrames)], colorReset,
					colorDim, label, colorReset)
				i++
				time.Sleep(80 * time.Millisecond)
			}
		}
	}()
	return s
}

func (s *spinner) stop() {
	close(s.done)
	time.Sleep(90 * time.Millisecond) // Let goroutine clean up
}

// ============================================================
// Prompt Rendering
// ============================================================

// prompt returns the colorful interactive prompt string.
func prompt() string {
	// Colored dot + gradient "raftra" + arrow
	r, g, b := hslToRGB(250, 0.75, 0.7)
	dot := fmt.Sprintf("%s●%s", rgb(r, g, b), colorReset)
	name := gradientText("raftra", 235)
	r2, g2, b2 := hslToRGB(260, 0.7, 0.65)
	arrow := fmt.Sprintf("%s❯%s", rgb(r2, g2, b2), colorReset)
	return fmt.Sprintf("%s %s %s ", dot, name, arrow)
}

// ============================================================
// Separator & Formatted Output
// ============================================================

func printSeparator() {
	line := strings.Repeat("─", 50)
	fmt.Println(renderGradientLine(line, 235, 45, 0.5, 0.45))
}

// ============================================================
// Interactive Mode
// ============================================================

func runInteractiveMode(c *cluster) {
	fmt.Println() // Breathing room

	// --- Animated banner ---
	animateBanner()

	// --- Tagline with typewriter effect ---
	fmt.Println()
	fmt.Print(hideCursor)
	tagline := "  Distributed consensus at your fingertips."
	typeWriter(gradientText(tagline, 240), 3*time.Millisecond)
	fmt.Print(showCursor)

	// --- Info block ---
	fmt.Println()
	printSeparator()
	r, g, b := hslToRGB(255, 0.7, 0.7)
	fmt.Printf("  %sv%s%s  •  ", rgb(r, g, b), version, colorReset)
	r2, g2, b2 := hslToRGB(240, 0.6, 0.65)
	fmt.Printf("%s%s%s\n", rgb(r2, g2, b2), c, colorReset)
	printSeparator()
	fmt.Printf("  Type %shelp%s for commands, %sexit%s to quit.\n\n",
		colorBold, colorReset, colorBold, colorReset)

	// --- REPL Loop ---
	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Print(prompt())
		if !scanner.Scan() {
			fmt.Println() // Clean exit on Ctrl+D
			break
		}
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		args := strings.Fields(line)
		cmd := strings.ToLower(args[0])

		switch cmd {
		case "exit", "quit":
			printSeparator()
			fmt.Printf("  %s👋 Goodbye!%s\n\n", colorDim, colorReset)
			return

		case "help":
			fmt.Println()
			printSeparator()
			fmt.Println(gradientText("  Commands", 235))
			printSeparator()
			fmt.Printf("  %sstatus%s             Show cluster node status\n", colorBold, colorReset)
			fmt.Printf("  %sset%s <key> <value>  Store a key-value pair\n", colorBold, colorReset)
			fmt.Printf("  %sget%s <key>          Retrieve a value by key\n", colorBold, colorReset)
			fmt.Printf("  %sdelete%s <key>       Remove a key from the store\n", colorBold, colorReset)
			fmt.Printf("  %sexit%s               Quit the CLI\n", colorBold, colorReset)
			printSeparator()
			fmt.Println()

		default:
			runCommand(args, c)
		}
	}
}

// ============================================================
// Usage (for one-off mode / --help)
// ============================================================

func printUsage() {
	fmt.Println()
	// Static gradient banner (no animation for --help)
	fmt.Println(renderBannerFrame(260))
	fmt.Println()
	fmt.Printf("  %sRaftra CLI%s v%s — Distributed key-value store client\n\n", colorBold, colorReset, version)
	fmt.Printf("  %sUSAGE%s\n", colorBold, colorReset)
	fmt.Printf("    raftra-cli                        Start interactive mode\n")
	fmt.Printf("    raftra-cli [flags] <command>       Run a single command\n\n")
	fmt.Printf("  %sCOMMANDS%s\n", colorBold, colorReset)
	fmt.Printf("    %sstatus%s                          Show cluster node status\n", colorCyan, colorReset)
	fmt.Printf("    %sset%s    <key> <value>             Store a key-value pair\n", colorCyan, colorReset)
	fmt.Printf("    %sget%s    <key>                     Retrieve a value\n", colorCyan, colorReset)
	fmt.Printf("    %sdelete%s <key>                     Remove a key\n\n", colorCyan, colorReset)
	fmt.Printf("  %sFLAGS%s\n", colorBold, colorReset)
	fmt.Printf("    --addr string    Comma-separated node addresses (default: %s)\n", defaultAddr)
	fmt.Printf("    --help           Show this help message\n\n")
}

// ============================================================
// Entry Point
// ============================================================

func main() {
	var addr string
	flag.StringVar(&addr, "addr", defaultAddr, "Comma-separated HTTP addresses of Raftra nodes")
	flag.Usage = printUsage
	flag.Parse()

	c, err := newCluster(addr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "  %s✗ Error:%s %v\n", colorRed, colorReset, err)
		os.Exit(1)
	}

	args := flag.Args()

	// No arguments → launch interactive REPL
	if len(args) == 0 {
		runInteractiveMode(c)
		return
	}

	// Otherwise → one-off command execution
	runCommand(args, c)
}

// ============================================================
// Command Router
// ============================================================

func runCommand(args []string, c *cluster) {
	command := strings.ToLower(args[0])

	switch command {
	case "status":
		handleStatus(c)

	case "get":
		if len(args) < 2 {
			fmt.Fprintf(os.Stderr, "  %s✗%s 'get' requires a key. Usage: %sget <key>%s\n", colorRed, colorReset, colorBold, colorReset)
			return
		}
		handleGet(c, args[1])

	case "set":
		if len(args) < 3 {
			fmt.Fprintf(os.Stderr, "  %s✗%s 'set' requires key and value. Usage: %sset <key> <value>%s\n", colorRed, colorReset, colorBold, colorReset)
			return
		}
		val := strings.Join(args[2:], " ")
		handleSet(c, args[1], val)

	case "delete", "del":
		if len(args) < 2 {
			fmt.Fprintf(os.Stderr, "  %s✗%s 'delete' requires a key. Usage: %sdelete <key>%s\n", colorRed, colorReset, colorBold, colorReset)
			return
		}
		handleDelete(c, args[1])

	default:
		fmt.Fprintf(os.Stderr, "  %s✗%s Unknown command %q. Type %shelp%s for usage.\n", colorRed, colorReset, command, colorBold, colorReset)
	}
}

// ============================================================
// HTTP Client — Redirect-Aware Request Engine
// ============================================================

// translateRedirectURL bridges Docker internal service names (node1, node2, node3)
// to host-accessible ports (localhost:8001, localhost:8002, localhost:8003)
// when running the CLI from the host machine.
func translateRedirectURL(targetURL *url.URL, initialURL *url.URL) *url.URL {
	host := initialURL.Hostname()
	isLocal := host == "localhost" || host == "127.0.0.1"

	if isLocal {
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

// executeRequest performs an HTTP request with automatic HTTP 307 redirect following.
func executeRequest(method, targetURL string, body []byte, initialURL *url.URL) (*http.Response, []byte, error) {
	client := &http.Client{
		Timeout: 5 * time.Second,
		// Disable automatic redirect so we can inspect, translate Docker hostnames, and notify the user
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}

	currentURL := targetURL
	redirectCount := 0
	maxRedirects := 5

	for {
		var bodyReader io.Reader
		if body != nil {
			bodyReader = bytes.NewReader(body)
		}

		req, err := http.NewRequest(method, currentURL, bodyReader)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to create request: %w", err)
		}

		resp, err := client.Do(req)
		if err != nil {
			return nil, nil, fmt.Errorf("request failed: %w", err)
		}

		// Handle HTTP 307 / 308 (Follower -> Leader redirect)
		if resp.StatusCode == http.StatusTemporaryRedirect || resp.StatusCode == http.StatusPermanentRedirect {
			_ = resp.Body.Close()
			redirectCount++
			if redirectCount > maxRedirects {
				return nil, nil, fmt.Errorf("exceeded maximum redirect limit (%d)", maxRedirects)
			}

			location := resp.Header.Get("Location")
			if location == "" {
				return nil, nil, fmt.Errorf("redirect received with missing Location header")
			}

			locURL, err := url.Parse(location)
			if err != nil {
				return nil, nil, fmt.Errorf("invalid redirect location %q: %w", location, err)
			}

			// Translate Docker internal hostnames if calling from host machine
			locURL = translateRedirectURL(locURL, initialURL)
			currentURL = locURL.String()

			r, g, b := hslToRGB(240, 0.7, 0.65) // Blue-purple for redirect notices
			fmt.Printf("  %s↳%s Redirecting to leader → %s%s%s\n",
				rgb(r, g, b), colorReset, colorBold, currentURL, colorReset)
			continue
		}

		respBody, err := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if err != nil {
			return nil, nil, fmt.Errorf("failed to read response body: %w", err)
		}

		return resp, respBody, nil
	}
}

// ============================================================
// Command Handlers
// ============================================================

func handleStatus(c *cluster) {
	sp := startSpinner("Fetching node status...")
	resp, body, err := c.do("GET", "/status", nil)
	sp.stop()

	if err != nil {
		fmt.Fprintf(os.Stderr, "  %s✗%s %v\n", colorRed, colorReset, err)
		return
	}

	if resp.StatusCode != http.StatusOK {
		fmt.Fprintf(os.Stderr, "  %s✗%s %s\n", colorRed, colorReset, describeError(resp.StatusCode, body))
		return
	}

	var status StatusResponse
	if err := json.Unmarshal(body, &status); err != nil {
		fmt.Fprintf(os.Stderr, "  %s✗%s Failed to parse response: %v\n", colorRed, colorReset, err)
		return
	}

	// Render a beautiful status card
	fmt.Println()
	printSeparator()
	fmt.Println(gradientText("  Node Status", 235))
	printSeparator()

	// Role with contextual color
	var roleDisplay string
	if status.IsLeader {
		r, g, b := hslToRGB(250, 0.8, 0.7)
		roleDisplay = fmt.Sprintf("%s★ %s (Leader)%s", rgb(r, g, b), status.Role, colorReset)
	} else {
		r, g, b := hslToRGB(230, 0.5, 0.6)
		roleDisplay = fmt.Sprintf("%s◦ %s%s", rgb(r, g, b), status.Role, colorReset)
	}

	fmt.Printf("  Role          %s\n", roleDisplay)
	fmt.Printf("  Term          %s%d%s\n", colorBold, status.Term, colorReset)
	fmt.Printf("  Leader        %s%s%s\n", colorBold, status.LeaderID, colorReset)
	fmt.Printf("  Commit Index  %d\n", status.CommitIndex)
	fmt.Printf("  Last Applied  %d\n", status.LastApplied)
	printSeparator()
	fmt.Println()
}

func handleGet(c *cluster, key string) {
	sp := startSpinner("Reading key...")
	resp, body, err := c.do("GET", "/api/v1/kv/"+url.PathEscape(key), nil)
	sp.stop()

	if err != nil {
		fmt.Fprintf(os.Stderr, "  %s✗%s %v\n", colorRed, colorReset, err)
		return
	}

	if resp.StatusCode == http.StatusNotFound {
		r, g, b := hslToRGB(245, 0.6, 0.65)
		fmt.Printf("  %s⚠ Key not found:%s %s\n", rgb(r, g, b), colorReset, key)
		return
	}

	if resp.StatusCode != http.StatusOK {
		fmt.Fprintf(os.Stderr, "  %s✗%s %s\n", colorRed, colorReset, describeError(resp.StatusCode, body))
		return
	}

	var kv KVResponse
	if err := json.Unmarshal(body, &kv); err != nil {
		// If response is raw string, display directly
		fmt.Printf("  %s\n", string(body))
		return
	}

	r, g, b := hslToRGB(250, 0.6, 0.7)
	fmt.Printf("  %s%s%s → %s%s%s\n", rgb(r, g, b), key, colorReset, colorBold, kv.Value, colorReset)
}

func handleSet(c *cluster, key, value string) {
	sp := startSpinner("Writing to cluster...")
	resp, body, err := c.do("PUT", "/api/v1/kv/"+url.PathEscape(key), []byte(value))
	sp.stop()

	if err != nil {
		fmt.Fprintf(os.Stderr, "  %s✗%s %v\n", colorRed, colorReset, err)
		return
	}

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		fmt.Fprintf(os.Stderr, "  %s✗%s %s\n", colorRed, colorReset, describeError(resp.StatusCode, body))
		return
	}

	r, g, b := hslToRGB(235, 0.8, 0.7)
	fmt.Printf("  %s✔%s %s%s%s = %s\n", rgb(r, g, b), colorReset, colorBold, key, colorReset, value)
}

func handleDelete(c *cluster, key string) {
	sp := startSpinner("Deleting from cluster...")
	resp, body, err := c.do("DELETE", "/api/v1/kv/"+url.PathEscape(key), nil)
	sp.stop()

	if err != nil {
		fmt.Fprintf(os.Stderr, "  %s✗%s %v\n", colorRed, colorReset, err)
		return
	}

	if resp.StatusCode != http.StatusOK {
		fmt.Fprintf(os.Stderr, "  %s✗%s %s\n", colorRed, colorReset, describeError(resp.StatusCode, body))
		return
	}

	r, g, b := hslToRGB(235, 0.8, 0.7)
	fmt.Printf("  %s✔%s Deleted %s%s%s\n", rgb(r, g, b), colorReset, colorBold, key, colorReset)
}

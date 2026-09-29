package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

// ANSI color codes for formatted terminal output
const (
	colorReset  = "\033[0m"
	colorRed    = "\033[31m"
	colorGreen  = "\033[32m"
	colorYellow = "\033[33m"
	colorCyan   = "\033[36m"
	colorBold   = "\033[1m"
)

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

func printUsage() {
	fmt.Printf(`%sRaftra CLI%s — Command-line client for the Raftra distributed key-value store

%sUSAGE:%s
  raftra-cli [flags] <command> [arguments...]

%sCOMMANDS:%s
  %sset%s <key> <value>   Set a key-value pair (auto-redirects to leader)
  %sget%s <key>           Get the value of a key
  %sdelete%s <key>        Delete a key (auto-redirects to leader)
  %sstatus%s              Show the Raft state of the targeted node

%sFLAGS:%s
  --addr string         HTTP address of the Raftra node (default: "http://localhost:8001")
  --help                Show this help message

%sEXAMPLES:%s
  raftra-cli set name shantanu
  raftra-cli --addr=http://localhost:8002 get name
  raftra-cli --addr=http://localhost:8003 delete name
  raftra-cli --addr=http://localhost:8002 status
`,
		colorBold, colorReset,
		colorBold, colorReset,
		colorBold, colorReset,
		colorCyan, colorReset,
		colorCyan, colorReset,
		colorCyan, colorReset,
		colorCyan, colorReset,
		colorBold, colorReset,
		colorBold, colorReset,
	)
}

func main() {
	var addr string
	flag.StringVar(&addr, "addr", "http://localhost:8001", "HTTP address of the Raftra node")
	flag.Usage = printUsage
	flag.Parse()

	args := flag.Args()
	if len(args) == 0 {
		printUsage()
		os.Exit(1)
	}

	// Normalize base URL
	if !strings.HasPrefix(addr, "http://") && !strings.HasPrefix(addr, "https://") {
		addr = "http://" + addr
	}
	addr = strings.TrimRight(addr, "/")

	parsedBase, err := url.Parse(addr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%sError:%s Invalid node address %q: %v\n", colorRed, colorReset, addr, err)
		os.Exit(1)
	}

	command := strings.ToLower(args[0])

	switch command {
	case "status":
		handleStatus(addr, parsedBase)

	case "get":
		if len(args) < 2 {
			fmt.Fprintf(os.Stderr, "%sError:%s 'get' requires a key. Example: raftra-cli get <key>\n", colorRed, colorReset)
			os.Exit(1)
		}
		handleGet(addr, parsedBase, args[1])

	case "set":
		if len(args) < 3 {
			fmt.Fprintf(os.Stderr, "%sError:%s 'set' requires key and value. Example: raftra-cli set <key> <value>\n", colorRed, colorReset)
			os.Exit(1)
		}
		// Join remaining arguments so values with spaces are preserved
		val := strings.Join(args[2:], " ")
		handleSet(addr, parsedBase, args[1], val)

	case "delete", "del":
		if len(args) < 2 {
			fmt.Fprintf(os.Stderr, "%sError:%s 'delete' requires a key. Example: raftra-cli delete <key>\n", colorRed, colorReset)
			os.Exit(1)
		}
		handleDelete(addr, parsedBase, args[1])

	default:
		fmt.Fprintf(os.Stderr, "%sError:%s Unknown command %q\n\n", colorRed, colorReset, command)
		printUsage()
		os.Exit(1)
	}
}

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

			fmt.Printf("%s↳ [redirect]%s Follower redirecting to leader at %s%s%s\n",
				colorYellow, colorReset, colorCyan, currentURL, colorReset)
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

func handleStatus(baseURL string, parsedBase *url.URL) {
	targetURL := baseURL + "/status"
	resp, body, err := executeRequest("GET", targetURL, nil, parsedBase)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%sError:%s %v\n", colorRed, colorReset, err)
		os.Exit(1)
	}

	if resp.StatusCode != http.StatusOK {
		fmt.Fprintf(os.Stderr, "%sError:%s Node returned HTTP %d: %s\n", colorRed, colorReset, resp.StatusCode, string(body))
		os.Exit(1)
	}

	var status StatusResponse
	if err := json.Unmarshal(body, &status); err != nil {
		fmt.Fprintf(os.Stderr, "%sError:%s Failed to parse status response: %v\n", colorRed, colorReset, err)
		os.Exit(1)
	}

	roleColor := colorYellow
	if status.IsLeader {
		roleColor = colorGreen
	}

	fmt.Printf("%s=== Raftra Node Status ===%s\n", colorBold, colorReset)
	fmt.Printf("  Role:         %s%s%s\n", roleColor, status.Role, colorReset)
	fmt.Printf("  Term:         %d\n", status.Term)
	fmt.Printf("  Is Leader:    %t\n", status.IsLeader)
	fmt.Printf("  Leader ID:    %s\n", status.LeaderID)
	fmt.Printf("  Commit Index: %d\n", status.CommitIndex)
	fmt.Printf("  Last Applied: %d\n", status.LastApplied)
}

func handleGet(baseURL string, parsedBase *url.URL, key string) {
	targetURL := fmt.Sprintf("%s/api/v1/kv/%s", baseURL, url.PathEscape(key))
	resp, body, err := executeRequest("GET", targetURL, nil, parsedBase)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%sError:%s %v\n", colorRed, colorReset, err)
		os.Exit(1)
	}

	if resp.StatusCode == http.StatusNotFound {
		fmt.Printf("%sKey not found:%s %s\n", colorYellow, colorReset, key)
		return
	}

	if resp.StatusCode != http.StatusOK {
		fmt.Fprintf(os.Stderr, "%sError:%s HTTP %d: %s\n", colorRed, colorReset, resp.StatusCode, string(body))
		os.Exit(1)
	}

	var kv KVResponse
	if err := json.Unmarshal(body, &kv); err != nil {
		// If response is raw string, display directly
		fmt.Println(string(body))
		return
	}

	fmt.Println(kv.Value)
}

func handleSet(baseURL string, parsedBase *url.URL, key, value string) {
	targetURL := fmt.Sprintf("%s/api/v1/kv/%s", baseURL, url.PathEscape(key))
	resp, body, err := executeRequest("PUT", targetURL, []byte(value), parsedBase)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%sError:%s %v\n", colorRed, colorReset, err)
		os.Exit(1)
	}

	if resp.StatusCode == http.StatusServiceUnavailable {
		fmt.Fprintf(os.Stderr, "%sError:%s Cluster has no elected leader right now. Try again in a moment.\n", colorYellow, colorReset)
		os.Exit(1)
	}

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		fmt.Fprintf(os.Stderr, "%sError:%s HTTP %d: %s\n", colorRed, colorReset, resp.StatusCode, string(body))
		os.Exit(1)
	}

	fmt.Printf("%s✔ OK%s [key=%s%s%s]\n", colorGreen, colorReset, colorBold, key, colorReset)
}

func handleDelete(baseURL string, parsedBase *url.URL, key string) {
	targetURL := fmt.Sprintf("%s/api/v1/kv/%s", baseURL, url.PathEscape(key))
	resp, body, err := executeRequest("DELETE", targetURL, nil, parsedBase)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%sError:%s %v\n", colorRed, colorReset, err)
		os.Exit(1)
	}

	if resp.StatusCode == http.StatusServiceUnavailable {
		fmt.Fprintf(os.Stderr, "%sError:%s Cluster has no elected leader right now. Try again in a moment.\n", colorYellow, colorReset)
		os.Exit(1)
	}

	if resp.StatusCode != http.StatusOK {
		fmt.Fprintf(os.Stderr, "%sError:%s HTTP %d: %s\n", colorRed, colorReset, resp.StatusCode, string(body))
		os.Exit(1)
	}

	fmt.Printf("%s✔ Deleted%s [key=%s%s%s]\n", colorGreen, colorReset, colorBold, key, colorReset)
}

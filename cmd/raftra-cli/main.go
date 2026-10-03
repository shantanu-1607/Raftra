package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"sort"
	"strings"
	"sync"
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
// Entry Point
// ============================================================

func main() {
	setupTerminal()

	var addr string
	flag.StringVar(&addr, "addr", defaultAddr, "Comma-separated HTTP addresses of Raftra nodes")
	flag.Usage = printUsage
	flag.Parse()

	c, err := newCluster(addr)
	if err != nil {
		printFail(err.Error())
		os.Exit(1)
	}

	args := flag.Args()
	if len(args) == 0 {
		runInteractiveMode(c)
		return
	}
	if !runCommand(args, c) {
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Println()
	printBanner(false)
	fmt.Println()
	fmt.Printf("  %s %s\n", bold("Raftra CLI"), dim("v"+version))
	fmt.Println("  A client for Raftra, a fault-tolerant key-value store built on Raft.")
	fmt.Println()
	fmt.Println("  " + orange("Usage"))
	fmt.Println("    raftra-cli                          start the interactive shell")
	fmt.Println("    raftra-cli [--addr list] <command>  run one command and exit")
	fmt.Println()
	fmt.Println("  " + orange("Commands"))
	printCommandList("    ")
	fmt.Println()
	fmt.Println("  " + orange("Flags"))
	fmt.Println("    --addr   comma-separated node addresses")
	fmt.Println("             " + dim("default: "+defaultAddr))
	fmt.Println()
}

var commandHelp = [][2]string{
	{"status", "who leads, and each node's role, term and commit index"},
	{"set <key> <value>", "store a value (the CLI finds the leader for you)"},
	{"get <key>", "read a value"},
	{"delete <key>", "remove a key"},
}

func printCommandList(indent string) {
	for _, c := range commandHelp {
		fmt.Println(indent + padRight(bone(c[0]), 20) + dim(c[1]))
	}
}

// ============================================================
// Interactive Mode
// ============================================================

func runInteractiveMode(c *cluster) {
	// Piped input (a script): run each line, no chrome.
	if !stdinTTY || !stdoutTTY {
		scanner := bufio.NewScanner(os.Stdin)
		for scanner.Scan() {
			if args := strings.Fields(scanner.Text()); len(args) > 0 {
				if cmd := strings.ToLower(args[0]); cmd == "exit" || cmd == "quit" {
					return
				}
				runCommand(args, c)
			}
		}
		return
	}

	// Leave the terminal tidy on Ctrl+C: below the input box, cursor visible.
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt)
	go func() {
		<-sig
		fmt.Print(showCursor + "\033[2B\r\n")
		os.Exit(130)
	}()

	fmt.Println()
	printBanner(true)
	fmt.Println()

	sp := startSpinner("Connecting to the cluster…")
	nodes := fetchStatuses(c)
	sp.stop()
	printWelcome(c, nodes)

	reader := bufio.NewReader(os.Stdin)
	for {
		line, ok := readBoxed(reader)
		if !ok {
			printResult(cDim, dim("Bye."))
			fmt.Println()
			return
		}
		if line == "" {
			continue
		}
		args := strings.Fields(line)
		switch strings.ToLower(args[0]) {
		case "exit", "quit":
			printResult(cDim, dim("Bye."))
			fmt.Println()
			return
		case "help", "?":
			printResult(cOrange, "Commands")
			fmt.Print("  " + dim(glyphTree) + "  ")
			for i, cmd := range append(commandHelp, [2]string{"clear", "clear the screen"}, [2]string{"exit", "quit (or Ctrl+D)"}) {
				if i > 0 {
					fmt.Print("     ")
				}
				fmt.Println(padRight(bone(cmd[0]), 20) + dim(cmd[1]))
			}
		case "clear":
			fmt.Print("\033[H\033[2J")
			continue
		default:
			runCommand(args, c)
		}
		fmt.Println()
	}
}

func printWelcome(c *cluster, nodes []nodeStatus) {
	w := boxWidth()
	cluster := c.nodes[0].Host
	if n := len(c.nodes); n > 1 {
		cluster += fmt.Sprintf(" and %d more", n-1)
	}
	leader := dim("no node answered")
	if l := leaderOf(nodes); l != nil {
		leader = orange(l.st.LeaderID) + dim(fmt.Sprintf(", term %d", l.st.Term))
	} else if answered(nodes) > 0 {
		leader = paint(cAmber, "none right now") + dim(", an election is running")
	}
	fmt.Print(box(w,
		spread(orange(glyphStar)+" "+bold("Welcome to Raftra"), dim("v"+version), w),
		"",
		"  "+bone("A fault-tolerant key-value store, replicated with Raft."),
		"",
		"  "+dim("cluster")+"  "+cluster,
		"  "+dim("leader ")+"  "+leader,
	))
	fmt.Println()
	fmt.Println("  " + dim("Tip: the public playground kills its leader every 10 minutes."))
	fmt.Println("  " + dim("Run ") + bone("status") + dim(" now and again to watch it move."))
	fmt.Println()
}

// readBoxed draws an input box, reads one line inside it, then replaces the box
// with a dimmed echo of what was typed. It reports false on end of input (Ctrl+D).
func readBoxed(r *bufio.Reader) (string, bool) {
	w := boxWidth()
	fmt.Print(rule("╭"+strings.Repeat("─", w+2)+"╮") + "\n")
	fmt.Print(rule("│") + " " + orange(">") + strings.Repeat(" ", w-1) + " " + rule("│") + "\n")
	fmt.Print(rule("╰"+strings.Repeat("─", w+2)+"╯") + "\n")
	fmt.Print("  " + dim("help for commands, exit to quit"))
	// Up to the top border, remember it, then into the box after "│ > ".
	fmt.Print("\033[3A\r\0337\033[1B\r\033[4C")

	line, err := r.ReadString('\n')
	fmt.Print("\0338\033[J") // back to the top border; clear the box
	if err != nil && line == "" {
		return "", false
	}
	line = strings.TrimSpace(line)
	if line != "" {
		fmt.Println(dim("> " + line))
	}
	return line, true
}

// ============================================================
// Command Router
// ============================================================

// runCommand runs one command and reports whether it succeeded.
func runCommand(args []string, c *cluster) bool {
	command := strings.ToLower(args[0])
	redirects = nil

	switch command {
	case "status":
		return handleStatus(c)
	case "get":
		if len(args) < 2 {
			printFail("get needs a key", dim("Usage: get <key>"))
			return false
		}
		return handleGet(c, args[1])
	case "set":
		if len(args) < 3 {
			printFail("set needs a key and a value", dim("Usage: set <key> <value>"))
			return false
		}
		return handleSet(c, args[1], strings.Join(args[2:], " "))
	case "delete", "del":
		if len(args) < 2 {
			printFail("delete needs a key", dim("Usage: delete <key>"))
			return false
		}
		return handleDelete(c, args[1])
	default:
		printFail(fmt.Sprintf("Unknown command %q", command), dim("Commands: status, set, get, delete. Try help."))
		return false
	}
}

// ============================================================
// HTTP Client — Redirect-Aware Request Engine
// ============================================================

// redirects records the leader redirects followed by the current command, so the
// result can mention them once the spinner has stopped.
var redirects []string

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
			redirects = append(redirects, locURL.Host)
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

// nodeStatus is one node's answer to GET /status (st is nil if it didn't answer).
type nodeStatus struct {
	host string
	st   *StatusResponse
}

// fetchStatuses asks every node for its status at once.
func fetchStatuses(c *cluster) []nodeStatus {
	out := make([]nodeStatus, len(c.nodes))
	client := &http.Client{Timeout: 2 * time.Second}
	var wg sync.WaitGroup
	for i, u := range c.nodes {
		out[i].host = u.Host
		wg.Add(1)
		go func(i int, u *url.URL) {
			defer wg.Done()
			resp, err := client.Get(u.String() + "/status")
			if err != nil {
				return
			}
			defer resp.Body.Close()
			var st StatusResponse
			if resp.StatusCode == http.StatusOK && json.NewDecoder(resp.Body).Decode(&st) == nil {
				out[i].st = &st
			}
		}(i, u)
	}
	wg.Wait()
	return out
}

// leaderOf returns the node that claims leadership in the highest term, if any.
// A deposed leader can still claim the role briefly, until it hears the new term.
func leaderOf(nodes []nodeStatus) *nodeStatus {
	var best *nodeStatus
	for i := range nodes {
		n := &nodes[i]
		if n.st != nil && n.st.IsLeader && (best == nil || n.st.Term > best.st.Term) {
			best = n
		}
	}
	return best
}

func answered(nodes []nodeStatus) int {
	n := 0
	for _, s := range nodes {
		if s.st != nil {
			n++
		}
	}
	return n
}

// statusLines renders one aligned line per node.
func statusLines(nodes []nodeStatus) []string {
	sorted := append([]nodeStatus(nil), nodes...)
	sort.SliceStable(sorted, func(i, j int) bool { return sorted[i].host < sorted[j].host })
	width := 0
	for _, n := range sorted {
		if len(n.host) > width {
			width = len(n.host)
		}
	}
	leader := leaderOf(nodes)
	lines := make([]string, 0, len(sorted))
	for _, n := range sorted {
		host := padRight(n.host, width+2)
		if n.st == nil {
			lines = append(lines, dim(host)+paint(cRed, "no answer"))
			continue
		}
		role := strings.ToLower(n.st.Role)
		switch {
		case leader != nil && n.host == leader.host:
			role = orange(padRight("leader", 11))
		case role == "candidate":
			role = paint(cAmber, padRight(role, 11))
		default:
			role = bone(padRight("follower", 11))
		}
		lines = append(lines, host+role+dim(fmt.Sprintf("term %-4d commit %d", n.st.Term, n.st.CommitIndex)))
	}
	return lines
}

func handleStatus(c *cluster) bool {
	sp := startSpinner("Asking every node…")
	nodes := fetchStatuses(c)
	sp.stop()

	lines := statusLines(nodes)
	switch l := leaderOf(nodes); {
	case l != nil:
		printResult(cOrange, fmt.Sprintf("%s leads term %d", bold(l.st.LeaderID), l.st.Term), lines...)
	case answered(nodes) > 0:
		printResult(cAmber, "No leader right now, an election is running", lines...)
	default:
		printFail("No node answered", append(lines, dim("The playground may be restarting. Try again shortly."))...)
		return false
	}
	return true
}

// redirectNote describes the leader redirects of the last request, if any.
func redirectNote() []string {
	if len(redirects) == 0 {
		return nil
	}
	return []string{dim("Followed the leader to " + redirects[len(redirects)-1] + ".")}
}

func handleGet(c *cluster, key string) bool {
	sp := startSpinner("Reading…")
	resp, body, err := c.do("GET", "/api/v1/kv/"+url.PathEscape(key), nil)
	sp.stop()

	if err != nil {
		printFail("Read failed", err.Error())
		return false
	}
	if resp.StatusCode == http.StatusNotFound {
		printResult(cAmber, bold(key)+" isn't set")
		return true
	}
	if resp.StatusCode != http.StatusOK {
		printFail("Read failed", describeError(resp.StatusCode, body))
		return false
	}

	value := string(body)
	var kv KVResponse
	if json.Unmarshal(body, &kv) == nil {
		value = kv.Value
	}
	printResult(cTeal, bold(key)+" = "+value, dim("Read from "+resp.Request.URL.Host+". Reads on a follower can briefly lag the leader."))
	return true
}

func handleSet(c *cluster, key, value string) bool {
	sp := startSpinner("Writing to a majority…")
	resp, body, err := c.do("PUT", "/api/v1/kv/"+url.PathEscape(key), []byte(value))
	sp.stop()

	if err != nil {
		printFail("Write failed", err.Error())
		return false
	}
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		printFail("Write failed", describeError(resp.StatusCode, body))
		return false
	}
	printResult(cTeal, bold(key)+" = "+value, append([]string{dim("Committed: a majority of nodes stored it.")}, redirectNote()...)...)
	return true
}

func handleDelete(c *cluster, key string) bool {
	sp := startSpinner("Deleting…")
	resp, body, err := c.do("DELETE", "/api/v1/kv/"+url.PathEscape(key), nil)
	sp.stop()

	if err != nil {
		printFail("Delete failed", err.Error())
		return false
	}
	if resp.StatusCode != http.StatusOK {
		printFail("Delete failed", describeError(resp.StatusCode, body))
		return false
	}
	printResult(cTeal, "Deleted "+bold(key), append([]string{dim("Committed: a majority of nodes stored the delete.")}, redirectNote()...)...)
	return true
}

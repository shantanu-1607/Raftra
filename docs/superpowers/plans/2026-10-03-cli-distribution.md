# CLI Distribution Implementation Plan (Plan 2 of 4)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `raftra-cli` downloadable and playground-ready. It accepts several node addresses and fails over when one is down, explains playground errors in plain words, defaults to the public playground in release builds, and is published for macOS, Linux and Windows by an automated release pipeline with a one-line installer.

**Architecture:** A new `cmd/raftra-cli/client.go` holds a small `cluster` type that sends each request to the first node able to answer (reusing the existing redirect-following `executeRequest`) and retries across nodes with a bounded budget. `main.go` switches its handlers to that type. Build-time variables (`version`, `defaultAddr`) are overridden by GoReleaser via `-ldflags -X`. GitHub Actions runs CI on every push and GoReleaser on `v*` tags. `site/install.sh` installs the right archive after verifying its checksum.

**Tech Stack:** Go 1.26 standard library (`net/http`, `net/http/httptest`, `sync/atomic`), GoReleaser v2 (run via `go run`, not added to `go.mod`), GitHub Actions, POSIX `sh`.

**Spec:** `docs/superpowers/specs/2026-10-03-public-playground-design.md` §5.

## Global Constraints

- **No new Go module dependencies.** GoReleaser is invoked as `go run github.com/goreleaser/goreleaser/v2@latest …` and never added to `go.mod`.
- Public playground node URLs, verbatim: `https://raftra-n1.duckdns.org`, `https://raftra-n2.duckdns.org`, `https://raftra-n3.duckdns.org`.
- Release builds set `main.defaultAddr` to `https://raftra-n1.duckdns.org,https://raftra-n2.duckdns.org,https://raftra-n3.duckdns.org`. `make build` keeps `http://localhost:8001`, so `DEMO_WALKTHROUGH.md` stays valid unchanged.
- CLI retry rules: connection errors and HTTP `502`/`503`/`504` are retryable, meaning move on to the next node. After a full pass over all nodes, pause **300 ms**. Give up after about **3 s** total. Any other status (200, 404, 409, 413, 429, 507, …) is a final answer and is returned at once.
- Archive names, verbatim: `raftra-cli_<os>_<arch>.tar.gz` (`.zip` on Windows), `raftra-server_linux_<arch>.tar.gz`, plus `checksums.txt`. Targets: CLI on darwin/linux/windows × amd64/arm64; server on linux × amd64/arm64. `CGO_ENABLED=0`, `-s -w`.
- Installer: macOS and Linux only. It installs to `~/.local/bin` (override with `RAFTRA_INSTALL_DIR`), verifies the SHA-256 checksum, and prints a PATH hint if needed. `RAFTRA_DOWNLOAD_BASE` overrides the download base URL, for local testing.
- **The user runs every test command** (`AGENTS.md` interactive testing mode). Implementers may run tests to verify their own work.
- **Never commit or change git state.** The user makes all commits. Hand-off steps list the files and a suggested message.
- Explain written code to the user afterwards in simple language.

## File Structure

| File | Status | Responsibility |
|---|---|---|
| `cmd/raftra-cli/client.go` | Create | `cluster` (node list plus retry policy), `newCluster`, `parseAddrs`, `retryable`, `(*cluster).do`, `(*cluster).String`, `describeError` |
| `cmd/raftra-cli/client_test.go` | Create | `httptest`-based tests for parsing, failover, retry and give-up, and error messages |
| `cmd/raftra-cli/main.go` | Modify | `version`/`defaultAddr` become ldflags-overridable vars. `main`, `runCommand`, `runInteractiveMode` and the four handlers use `*cluster` and `describeError`. Help text. |
| `.goreleaser.yaml` | Create | Cross-platform builds, archives and checksums |
| `.github/workflows/ci.yml` | Create | gofmt, vet, and race tests on pushes to `main` and on PRs |
| `.github/workflows/release.yml` | Create | Tests, then `goreleaser release --clean`, on `v*` tags |
| `.gitignore` | Modify | Ignore `dist/` (GoReleaser output) |
| `site/install.sh` | Create | `curl … \| sh` installer |
| `README.md`, `AGENTS.md`, spec §5 | Modify | Document the address list, failover, releases and installer |

---

### Task 1: Failover client and friendly error messages

**Files:**
- Create: `cmd/raftra-cli/client.go`
- Test: `cmd/raftra-cli/client_test.go`

**Interfaces:**
- Consumes (existing, `cmd/raftra-cli/main.go`): `func executeRequest(method, targetURL string, body []byte, initialURL *url.URL) (*http.Response, []byte, error)`. It follows 307/308 leader redirects, translates Docker hostnames for localhost, reads and closes the body, and returns an error on connection failure. Also `type KVResponse struct { …; Error string \`json:"error,omitempty"\` }`.
- Produces (used by Task 2):
  - `type cluster struct { nodes []*url.URL; retryDelay time.Duration; budget time.Duration }`
  - `func newCluster(addrList string) (*cluster, error)`, with defaults `retryDelay` 300 ms and `budget` 3 s
  - `func parseAddrs(list string) ([]*url.URL, error)`
  - `func retryable(status int) bool`
  - `func (c *cluster) do(method, path string, body []byte) (*http.Response, []byte, error)`
  - `func (c *cluster) String() string`
  - `func describeError(status int, body []byte) string`

- [ ] **Step 1: Write the failing tests**

Create `cmd/raftra-cli/client_test.go`:

```go
package main

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// fastCluster builds a cluster with tiny retry timings so tests run quickly.
func fastCluster(t *testing.T, addrs ...string) *cluster {
	t.Helper()
	c, err := newCluster(strings.Join(addrs, ","))
	if err != nil {
		t.Fatalf("newCluster: %v", err)
	}
	c.retryDelay = 10 * time.Millisecond
	c.budget = 200 * time.Millisecond
	return c
}

// closedServerURL returns the URL of a server that is no longer listening.
func closedServerURL() string {
	srv := httptest.NewServer(http.NotFoundHandler())
	u := srv.URL
	srv.Close()
	return u
}

func TestParseAddrs(t *testing.T) {
	nodes, err := parseAddrs(" localhost:8001, http://a:1/ ,https://b.example ,")
	if err != nil {
		t.Fatalf("parseAddrs: %v", err)
	}
	want := []string{"http://localhost:8001", "http://a:1", "https://b.example"}
	if len(nodes) != len(want) {
		t.Fatalf("got %d nodes, want %d", len(nodes), len(want))
	}
	for i, u := range nodes {
		if u.String() != want[i] {
			t.Fatalf("node %d = %q, want %q", i, u.String(), want[i])
		}
	}

	if _, err := parseAddrs(" , "); err == nil {
		t.Fatal("expected an error for an empty address list")
	}
}

func TestDoFailsOverPastBadGateway(t *testing.T) {
	dead := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadGateway) // what Caddy answers while its node is down
	}))
	defer dead.Close()
	alive := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"key":"k","value":"v"}`)
	}))
	defer alive.Close()

	resp, body, err := fastCluster(t, dead.URL, alive.URL).do("GET", "/api/v1/kv/k", nil)
	if err != nil {
		t.Fatalf("do: %v", err)
	}
	if resp.StatusCode != http.StatusOK || !strings.Contains(string(body), `"v"`) {
		t.Fatalf("got HTTP %d %s, want 200 from the alive node", resp.StatusCode, body)
	}
}

func TestDoFailsOverPastUnreachableNode(t *testing.T) {
	alive := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"role":"Leader"}`)
	}))
	defer alive.Close()

	resp, _, err := fastCluster(t, closedServerURL(), alive.URL).do("GET", "/status", nil)
	if err != nil {
		t.Fatalf("do: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("got HTTP %d, want 200", resp.StatusCode)
	}
}

func TestDoRetriesUntilLeaderElected(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) <= 2 {
			w.WriteHeader(http.StatusServiceUnavailable) // election in progress
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	resp, _, err := fastCluster(t, srv.URL).do("PUT", "/api/v1/kv/k", []byte("v"))
	if err != nil {
		t.Fatalf("do: %v", err)
	}
	if resp.StatusCode != http.StatusOK || calls.Load() != 3 {
		t.Fatalf("got HTTP %d after %d calls, want 200 after 3", resp.StatusCode, calls.Load())
	}
}

func TestDoGivesUpWithLastResponse(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()

	start := time.Now()
	resp, _, err := fastCluster(t, srv.URL).do("GET", "/status", nil)
	if err != nil {
		t.Fatalf("expected the last response, got error: %v", err)
	}
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("got HTTP %d, want 503", resp.StatusCode)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("retrying took %v, budget is 200ms", elapsed)
	}
}

func TestDoReturnsErrorWhenNothingReachable(t *testing.T) {
	_, _, err := fastCluster(t, closedServerURL(), closedServerURL()).do("GET", "/status", nil)
	if err == nil || !strings.Contains(err.Error(), "no Raftra node reachable") {
		t.Fatalf("got err = %v, want a 'no Raftra node reachable' error", err)
	}
}

func TestDoDoesNotRetryFinalAnswers(t *testing.T) {
	var secondCalls atomic.Int32
	first := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusRequestEntityTooLarge)
		fmt.Fprint(w, `{"error":"key too large (max 128 bytes)"}`)
	}))
	defer first.Close()
	second := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		secondCalls.Add(1)
	}))
	defer second.Close()

	resp, _, err := fastCluster(t, first.URL, second.URL).do("PUT", "/api/v1/kv/k", []byte("v"))
	if err != nil {
		t.Fatalf("do: %v", err)
	}
	if resp.StatusCode != http.StatusRequestEntityTooLarge || secondCalls.Load() != 0 {
		t.Fatalf("got HTTP %d with %d calls to the second node, want 413 and 0 calls",
			resp.StatusCode, secondCalls.Load())
	}
}

func TestDescribeError(t *testing.T) {
	tests := []struct {
		status int
		body   string
		want   string
	}{
		{413, `{"error":"key too large (max 128 bytes)"}`, "key too large (max 128 bytes)"},
		{429, `{"error":"rate limit exceeded: slow down and retry in a second"}`, "Slow down"},
		{507, `{"error":"store is full (max 10000 keys)"}`, "resets at the top of every hour"},
		{503, `{"error":"cluster currently has no leader (election in progress)"}`, "No elected leader"},
		{500, "boom", "HTTP 500: boom"},
	}
	for _, tc := range tests {
		if got := describeError(tc.status, []byte(tc.body)); !strings.Contains(got, tc.want) {
			t.Errorf("describeError(%d) = %q, want it to contain %q", tc.status, got, tc.want)
		}
	}
}
```

- [ ] **Step 2: User runs the tests to confirm they fail**

Run: `go test ./cmd/raftra-cli/ -v`
Expected: build failure, with errors like `undefined: newCluster`, `undefined: parseAddrs` and `undefined: describeError`.

- [ ] **Step 3: Write the implementation**

Create `cmd/raftra-cli/client.go`:

```go
package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// cluster is the set of Raftra nodes the CLI may contact. Any node can answer:
// followers redirect writes to the leader, and the CLI moves on to the next node
// when one is down (for example, killed by the playground's chaos timer).
type cluster struct {
	nodes      []*url.URL
	retryDelay time.Duration // pause after every node has been tried once
	budget     time.Duration // total time to keep retrying before giving up
}

// newCluster parses a comma-separated address list with the default retry policy.
func newCluster(addrList string) (*cluster, error) {
	nodes, err := parseAddrs(addrList)
	if err != nil {
		return nil, err
	}
	return &cluster{nodes: nodes, retryDelay: 300 * time.Millisecond, budget: 3 * time.Second}, nil
}

// parseAddrs splits a comma-separated address list, defaulting to http:// and
// dropping trailing slashes: "localhost:8001, https://n2.example/" becomes
// [http://localhost:8001 https://n2.example].
func parseAddrs(list string) ([]*url.URL, error) {
	var nodes []*url.URL
	for _, raw := range strings.Split(list, ",") {
		raw = strings.TrimSpace(raw)
		if raw == "" {
			continue
		}
		if !strings.HasPrefix(raw, "http://") && !strings.HasPrefix(raw, "https://") {
			raw = "http://" + raw
		}
		u, err := url.Parse(strings.TrimRight(raw, "/"))
		if err != nil || u.Host == "" {
			return nil, fmt.Errorf("invalid node address %q", raw)
		}
		nodes = append(nodes, u)
	}
	if len(nodes) == 0 {
		return nil, errors.New("no node address given")
	}
	return nodes, nil
}

// String lists the node addresses for display.
func (c *cluster) String() string {
	addrs := make([]string, len(c.nodes))
	for i, u := range c.nodes {
		addrs[i] = u.String()
	}
	return strings.Join(addrs, ", ")
}

// retryable reports whether a status means "this node can't answer right now":
// the proxy couldn't reach a dead node (502, 504) or no leader is elected yet (503).
func retryable(status int) bool {
	return status == http.StatusBadGateway ||
		status == http.StatusServiceUnavailable ||
		status == http.StatusGatewayTimeout
}

// do sends one request to the cluster. It tries each node in order (following
// leader redirects), moves on when a node is unreachable or answers 502/503/504,
// and keeps cycling with a short pause until some node gives a final answer or the
// retry budget runs out. On give-up it returns the last response received, or an
// error if no node answered at all.
func (c *cluster) do(method, path string, body []byte) (*http.Response, []byte, error) {
	start := time.Now()
	var lastResp *http.Response
	var lastBody []byte
	var lastErr error

	for {
		for _, node := range c.nodes {
			resp, respBody, err := executeRequest(method, node.String()+path, body, node)
			if err != nil {
				lastErr = err
				continue
			}
			if !retryable(resp.StatusCode) {
				return resp, respBody, nil
			}
			lastResp, lastBody = resp, respBody
		}
		if time.Since(start)+c.retryDelay > c.budget {
			break
		}
		time.Sleep(c.retryDelay)
	}

	if lastResp != nil {
		return lastResp, lastBody, nil
	}
	return nil, nil, fmt.Errorf("no Raftra node reachable (%s); the cluster may be restarting, try again shortly: %w", c, lastErr)
}

// describeError turns a non-success response into a plain-language message,
// explaining the public playground's limits.
func describeError(status int, body []byte) string {
	msg := strings.TrimSpace(string(body))
	var parsed KVResponse
	if json.Unmarshal(body, &parsed) == nil && parsed.Error != "" {
		msg = parsed.Error
	}

	switch status {
	case http.StatusRequestEntityTooLarge:
		return "Too large: " + msg + "."
	case http.StatusTooManyRequests:
		return "Slow down: too many writes from your address. Try again in a second."
	case http.StatusInsufficientStorage:
		return "The playground is full (" + msg + "). It resets at the top of every hour."
	case http.StatusServiceUnavailable:
		return "No elected leader right now. Try again in a moment."
	default:
		return fmt.Sprintf("HTTP %d: %s", status, msg)
	}
}
```

- [ ] **Step 4: User runs the tests to confirm they pass**

Run: `go test -race ./cmd/raftra-cli/ -v`
Expected: `--- PASS` for `TestParseAddrs`, `TestDoFailsOverPastBadGateway`, `TestDoFailsOverPastUnreachableNode`, `TestDoRetriesUntilLeaderElected`, `TestDoGivesUpWithLastResponse`, `TestDoReturnsErrorWhenNothingReachable`, `TestDoDoesNotRetryFinalAnswers` and `TestDescribeError`, then `ok  github.com/shantanu-1607/raftra/cmd/raftra-cli`.

Then run: `go vet ./cmd/raftra-cli/ && gofmt -l cmd/`
Expected: no output.

- [ ] **Step 5: Hand-off (user commits)**

Files: `cmd/raftra-cli/client.go`, `cmd/raftra-cli/client_test.go`
Suggested message: `feat(cli): add multi-node failover client and friendly playground errors`

---

### Task 2: Wire the failover client into the CLI

**Files:**
- Modify: `cmd/raftra-cli/main.go` (version const at line 18, `runInteractiveMode`, `printUsage`, `main`, `runCommand`, `handleStatus`, `handleGet`, `handleSet`, `handleDelete`)

**Interfaces:**
- Consumes: `newCluster`, `(*cluster).do`, `(*cluster).String`, `describeError` (Task 1).
- Produces: package-level `var version string` and `var defaultAddr string`, which Task 3's GoReleaser ldflags set (`-X main.version=…`, `-X main.defaultAddr=…`).

- [ ] **Step 1: Make `version` and the default address build-time settings**

Replace:

```go
const version = "0.1.0"
```

with:

```go
// Build-time settings. Release builds override them with
// -ldflags "-X main.version=... -X main.defaultAddr=..." (see .goreleaser.yaml);
// a local `make build` keeps these defaults.
var (
	version     = "0.1.0"
	defaultAddr = "http://localhost:8001"
)
```

- [ ] **Step 2: Make interactive mode take a cluster**

Replace:

```go
func runInteractiveMode(addr string, parsedBase *url.URL) {
```

with:

```go
func runInteractiveMode(c *cluster) {
```

Replace:

```go
	fmt.Printf("%s%s%s\n", rgb(r2, g2, b2), addr, colorReset)
```

with:

```go
	fmt.Printf("%s%s%s\n", rgb(r2, g2, b2), c, colorReset)
```

Replace (inside the REPL `switch`):

```go
		default:
			runCommand(args, addr, parsedBase)
```

with:

```go
		default:
			runCommand(args, c)
```

- [ ] **Step 3: Update the help text**

Replace:

```go
	fmt.Printf("    --addr string    Node address (default: http://localhost:8001)\n")
```

with:

```go
	fmt.Printf("    --addr string    Comma-separated node addresses (default: %s)\n", defaultAddr)
```

- [ ] **Step 4: Build the cluster in `main`**

Replace the whole body of `main`:

```go
func main() {
	var addr string
	flag.StringVar(&addr, "addr", "http://localhost:8001", "HTTP address of the Raftra node")
	flag.Usage = printUsage
	flag.Parse()

	// Normalize base URL
	if !strings.HasPrefix(addr, "http://") && !strings.HasPrefix(addr, "https://") {
		addr = "http://" + addr
	}
	addr = strings.TrimRight(addr, "/")

	parsedBase, err := url.Parse(addr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "  %s✗ Error:%s Invalid node address %q: %v\n", colorRed, colorReset, addr, err)
		os.Exit(1)
	}

	args := flag.Args()

	// No arguments → launch interactive REPL
	if len(args) == 0 {
		runInteractiveMode(addr, parsedBase)
		return
	}

	// Otherwise → one-off command execution
	runCommand(args, addr, parsedBase)
}
```

with:

```go
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
```

- [ ] **Step 5: Route commands through the cluster**

In `runCommand`, replace the signature line:

```go
func runCommand(args []string, addr string, parsedBase *url.URL) {
```

with:

```go
func runCommand(args []string, c *cluster) {
```

and replace each handler call:

| Old | New |
|---|---|
| `handleStatus(addr, parsedBase)` | `handleStatus(c)` |
| `handleGet(addr, parsedBase, args[1])` | `handleGet(c, args[1])` |
| `handleSet(addr, parsedBase, args[1], val)` | `handleSet(c, args[1], val)` |
| `handleDelete(addr, parsedBase, args[1])` | `handleDelete(c, args[1])` |

- [ ] **Step 6: Update the four handlers**

6a. `handleStatus`. Replace:

```go
func handleStatus(baseURL string, parsedBase *url.URL) {
	sp := startSpinner("Fetching node status...")
	targetURL := baseURL + "/status"
	resp, body, err := executeRequest("GET", targetURL, nil, parsedBase)
	sp.stop()
```

with:

```go
func handleStatus(c *cluster) {
	sp := startSpinner("Fetching node status...")
	resp, body, err := c.do("GET", "/status", nil)
	sp.stop()
```

and replace:

```go
		fmt.Fprintf(os.Stderr, "  %s✗%s Node returned HTTP %d: %s\n", colorRed, colorReset, resp.StatusCode, string(body))
```

with:

```go
		fmt.Fprintf(os.Stderr, "  %s✗%s %s\n", colorRed, colorReset, describeError(resp.StatusCode, body))
```

6b. `handleGet`. Replace:

```go
func handleGet(baseURL string, parsedBase *url.URL, key string) {
	sp := startSpinner("Reading key...")
	targetURL := fmt.Sprintf("%s/api/v1/kv/%s", baseURL, url.PathEscape(key))
	resp, body, err := executeRequest("GET", targetURL, nil, parsedBase)
	sp.stop()
```

with:

```go
func handleGet(c *cluster, key string) {
	sp := startSpinner("Reading key...")
	resp, body, err := c.do("GET", "/api/v1/kv/"+url.PathEscape(key), nil)
	sp.stop()
```

6c. `handleSet`. Replace:

```go
func handleSet(baseURL string, parsedBase *url.URL, key, value string) {
	sp := startSpinner("Writing to cluster...")
	targetURL := fmt.Sprintf("%s/api/v1/kv/%s", baseURL, url.PathEscape(key))
	resp, body, err := executeRequest("PUT", targetURL, []byte(value), parsedBase)
	sp.stop()
```

with:

```go
func handleSet(c *cluster, key, value string) {
	sp := startSpinner("Writing to cluster...")
	resp, body, err := c.do("PUT", "/api/v1/kv/"+url.PathEscape(key), []byte(value))
	sp.stop()
```

6d. `handleDelete`. Replace:

```go
func handleDelete(baseURL string, parsedBase *url.URL, key string) {
	sp := startSpinner("Deleting from cluster...")
	targetURL := fmt.Sprintf("%s/api/v1/kv/%s", baseURL, url.PathEscape(key))
	resp, body, err := executeRequest("DELETE", targetURL, nil, parsedBase)
	sp.stop()
```

with:

```go
func handleDelete(c *cluster, key string) {
	sp := startSpinner("Deleting from cluster...")
	resp, body, err := c.do("DELETE", "/api/v1/kv/"+url.PathEscape(key), nil)
	sp.stop()
```

6e. In **both** `handleSet` and `handleDelete`, delete this block. A 503 now reaches `describeError` after the retries:

```go
	if resp.StatusCode == http.StatusServiceUnavailable {
		r, g, b := hslToRGB(245, 0.6, 0.65)
		fmt.Fprintf(os.Stderr, "  %s⚠%s No elected leader. Try again in a moment.\n", rgb(r, g, b), colorReset)
		return
	}
```

6f. In `handleGet`, `handleSet` and `handleDelete`, replace every occurrence of:

```go
		fmt.Fprintf(os.Stderr, "  %s✗%s HTTP %d: %s\n", colorRed, colorReset, resp.StatusCode, string(body))
```

with:

```go
		fmt.Fprintf(os.Stderr, "  %s✗%s %s\n", colorRed, colorReset, describeError(resp.StatusCode, body))
```

6g. If the compiler reports an unused import (for example `strings` in `main.go`), remove only that import. Don't remove imports that are still used.

- [ ] **Step 7: User verifies the build and the default addresses**

Run: `go build ./... && go vet ./... && gofmt -l cmd/ && go test -race ./cmd/raftra-cli/`
Expected: only `ok  github.com/shantanu-1607/raftra/cmd/raftra-cli`.

Run: `go run ./cmd/raftra-cli --help 2>&1 | grep -- --addr`
Expected: `--addr string    Comma-separated node addresses (default: http://localhost:8001)`

Run: `go run -ldflags "-X main.defaultAddr=https://raftra-n1.duckdns.org,https://raftra-n2.duckdns.org" ./cmd/raftra-cli --help 2>&1 | grep -- --addr`
Expected: `--addr string    Comma-separated node addresses (default: https://raftra-n1.duckdns.org,https://raftra-n2.duckdns.org)`

- [ ] **Step 8: User rehearses failover on a local cluster**

Start the three nodes from `DEMO_WALKTHROUGH.md` §1 in three terminals (with `-nosync`). Then in a fourth terminal:

```bash
make build
A=http://localhost:8001,http://localhost:8002,http://localhost:8003
./bin/raftra-cli --addr $A set greeting hello
./bin/raftra-cli --addr $A status
```

Note which node is the leader, stop it with Ctrl+C in its terminal, and **immediately** run:

```bash
./bin/raftra-cli --addr $A set greeting still-here
./bin/raftra-cli --addr $A get greeting
```

Expected: both commands succeed (`✔ greeting = still-here`, then `greeting → still-here`). There may be a short pause while the cluster elects a new leader, and possibly a "Redirecting to leader" line. No error.

- [ ] **Step 9: Hand-off (user commits)**

Files: `cmd/raftra-cli/main.go`
Suggested message: `feat(cli): accept multiple node addresses and fail over between them`

---

### Task 3: Release pipeline (GoReleaser + GitHub Actions)

**Files:**
- Create: `.goreleaser.yaml`
- Create: `.github/workflows/ci.yml`
- Create: `.github/workflows/release.yml`
- Modify: `.gitignore`

**Interfaces:**
- Consumes: `main.version` and `main.defaultAddr` string vars in `cmd/raftra-cli` (Task 2).
- Produces: release assets `raftra-cli_{darwin,linux}_{amd64,arm64}.tar.gz`, `raftra-cli_windows_{amd64,arm64}.zip`, `raftra-server_linux_{amd64,arm64}.tar.gz`, `checksums.txt`. Each archive has the binary at its root. Task 4's installer and Plan 3's server setup script download these by name from `https://github.com/shantanu-1607/Raftra/releases/latest/download/`.

- [ ] **Step 1: Create `.goreleaser.yaml`**

```yaml
# GoReleaser v2 config. Push a tag like v0.2.0 and .github/workflows/release.yml
# publishes these archives to GitHub Releases.
# Local dry run: go run github.com/goreleaser/goreleaser/v2@latest release --snapshot --clean
version: 2

project_name: raftra

builds:
  - id: raftra-cli
    main: ./cmd/raftra-cli
    binary: raftra-cli
    env:
      - CGO_ENABLED=0
    goos: [linux, darwin, windows]
    goarch: [amd64, arm64]
    flags:
      - -trimpath
    ldflags:
      - -s -w -X main.version={{ .Version }} -X main.defaultAddr=https://raftra-n1.duckdns.org,https://raftra-n2.duckdns.org,https://raftra-n3.duckdns.org

  - id: raftra-server
    main: ./cmd/raftra-server
    binary: raftra-server
    env:
      - CGO_ENABLED=0
    goos: [linux]
    goarch: [amd64, arm64]
    flags:
      - -trimpath
    ldflags:
      - -s -w

archives:
  - id: raftra-cli
    ids: [raftra-cli]
    name_template: "raftra-cli_{{ .Os }}_{{ .Arch }}"
    formats: [tar.gz]
    format_overrides:
      - goos: windows
        formats: [zip]

  - id: raftra-server
    ids: [raftra-server]
    name_template: "raftra-server_{{ .Os }}_{{ .Arch }}"
    formats: [tar.gz]

checksum:
  name_template: checksums.txt

changelog:
  sort: asc
```

If `goreleaser check` (Step 5) rejects a field name because of the installed GoReleaser version (for example `ids` vs `builds`, or `formats` vs `format` in `archives`), change only that field to the name it asks for, and note the change in your report.

- [ ] **Step 2: Create `.github/workflows/ci.yml`**

```yaml
name: CI

on:
  push:
    branches: [main]
  pull_request:

permissions:
  contents: read

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - uses: actions/setup-go@v5
        with:
          go-version-file: go.mod

      - name: gofmt
        run: |
          unformatted=$(gofmt -l .)
          if [ -n "$unformatted" ]; then
            echo "These files need gofmt:"
            echo "$unformatted"
            exit 1
          fi

      - name: go vet
        run: go vet ./...

      - name: go test (race)
        run: go test -race ./...
```

- [ ] **Step 3: Create `.github/workflows/release.yml`**

```yaml
name: Release

on:
  push:
    tags: ["v*"]

permissions:
  contents: write

jobs:
  goreleaser:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
        with:
          fetch-depth: 0

      - uses: actions/setup-go@v5
        with:
          go-version-file: go.mod

      - name: go test (race)
        run: go test -race ./...

      - uses: goreleaser/goreleaser-action@v6
        with:
          distribution: goreleaser
          version: "~> v2"
          args: release --clean
        env:
          GITHUB_TOKEN: ${{ secrets.GITHUB_TOKEN }}
```

- [ ] **Step 4: Ignore GoReleaser output**

In `.gitignore`, under the `# Binaries` section after the `bin/` line, add:

```
dist/
```

- [ ] **Step 5: User validates the config**

Run: `go run github.com/goreleaser/goreleaser/v2@latest check`
Expected: a line ending in `1 configuration file(s) validated`. The first run downloads and compiles GoReleaser, which takes a minute or two.

- [ ] **Step 6: User does a full dry-run build**

Run: `go run github.com/goreleaser/goreleaser/v2@latest release --snapshot --clean`
Expected: it finishes with `release succeeded`, and `ls dist/*.tar.gz dist/*.zip dist/checksums.txt` lists:

```
dist/checksums.txt
dist/raftra-cli_darwin_amd64.tar.gz
dist/raftra-cli_darwin_arm64.tar.gz
dist/raftra-cli_linux_amd64.tar.gz
dist/raftra-cli_linux_arm64.tar.gz
dist/raftra-cli_windows_amd64.zip
dist/raftra-cli_windows_arm64.zip
dist/raftra-server_linux_amd64.tar.gz
dist/raftra-server_linux_arm64.tar.gz
```

Then run:

```bash
mkdir -p /tmp/raftra-rc && tar -xzf dist/raftra-cli_darwin_arm64.tar.gz -C /tmp/raftra-rc raftra-cli && /tmp/raftra-rc/raftra-cli --help 2>&1 | grep -- --addr
```

Expected: `--addr string    Comma-separated node addresses (default: https://raftra-n1.duckdns.org,https://raftra-n2.duckdns.org,https://raftra-n3.duckdns.org)`. On an Intel Mac, use `darwin_amd64`.

`git status` must not list `dist/`.

- [ ] **Step 7: Hand-off (user commits)**

Files: `.goreleaser.yaml`, `.github/workflows/ci.yml`, `.github/workflows/release.yml`, `.gitignore`
Suggested message: `ci: add GitHub Actions CI and GoReleaser release pipeline`

---

### Task 4: One-line installer and docs

**Files:**
- Create: `site/install.sh`
- Modify: `README.md` (Quick Start §3 "Talk to the cluster")
- Modify: `AGENTS.md` (Repository Structure, Development Workflow)
- Modify: `docs/superpowers/specs/2026-10-03-public-playground-design.md` (§5 installer row)

**Interfaces:**
- Consumes: release asset names and `checksums.txt` from Task 3; `dist/` from the Task 3 snapshot, for local testing.
- Produces: `site/install.sh`, served from `https://raw.githubusercontent.com/shantanu-1607/Raftra/main/site/install.sh` now and from GitHub Pages after Plan 4.

- [ ] **Step 1: Create `site/install.sh`**

```sh
#!/bin/sh
# Raftra CLI installer (macOS and Linux).
#   curl -fsSL https://raw.githubusercontent.com/shantanu-1607/Raftra/main/site/install.sh | sh
# Environment overrides:
#   RAFTRA_INSTALL_DIR     where to install (default: $HOME/.local/bin)
#   RAFTRA_DOWNLOAD_BASE   where to download from (default: the latest GitHub release)
set -eu

REPO="shantanu-1607/Raftra"
BIN="raftra-cli"
INSTALL_DIR="${RAFTRA_INSTALL_DIR:-$HOME/.local/bin}"
BASE="${RAFTRA_DOWNLOAD_BASE:-https://github.com/$REPO/releases/latest/download}"

os=$(uname -s | tr '[:upper:]' '[:lower:]')
case "$os" in
  linux|darwin) ;;
  *)
    echo "Unsupported OS: $os. Windows users: download the .zip from https://github.com/$REPO/releases/latest" >&2
    exit 1
    ;;
esac

arch=$(uname -m)
case "$arch" in
  x86_64|amd64) arch=amd64 ;;
  arm64|aarch64) arch=arm64 ;;
  *)
    echo "Unsupported CPU architecture: $arch" >&2
    exit 1
    ;;
esac

asset="${BIN}_${os}_${arch}.tar.gz"
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT

echo "Downloading $asset ..."
curl -fsSL "$BASE/$asset" -o "$tmp/$asset"
curl -fsSL "$BASE/checksums.txt" -o "$tmp/checksums.txt"

expected=$(grep " $asset\$" "$tmp/checksums.txt" | awk '{print $1}')
if [ -z "$expected" ]; then
  echo "No checksum listed for $asset" >&2
  exit 1
fi
if command -v sha256sum >/dev/null 2>&1; then
  actual=$(sha256sum "$tmp/$asset" | awk '{print $1}')
else
  actual=$(shasum -a 256 "$tmp/$asset" | awk '{print $1}')
fi
if [ "$expected" != "$actual" ]; then
  echo "Checksum mismatch for $asset: refusing to install" >&2
  exit 1
fi

tar -xzf "$tmp/$asset" -C "$tmp" "$BIN"
mkdir -p "$INSTALL_DIR"
mv "$tmp/$BIN" "$INSTALL_DIR/$BIN"
chmod +x "$INSTALL_DIR/$BIN"
echo "Installed $BIN to $INSTALL_DIR/$BIN"

case ":$PATH:" in
  *":$INSTALL_DIR:"*) ;;
  *)
    echo "Note: $INSTALL_DIR is not on your PATH. Add this line to your shell profile:"
    echo "  export PATH=\"$INSTALL_DIR:\$PATH\""
    ;;
esac
echo "Try it:  $BIN status"
```

Then make it executable: `chmod +x site/install.sh`.

- [ ] **Step 2: User checks the script**

Run: `sh -n site/install.sh && echo syntax-ok`
Expected: `syntax-ok`

If `shellcheck` is installed (`brew install shellcheck`), run `shellcheck site/install.sh`. Expected: no output.

- [ ] **Step 3: User tests a real install from the Task 3 snapshot**

Requires `dist/` from Task 3 Step 6.

```bash
rm -rf /tmp/raftra-bin
RAFTRA_DOWNLOAD_BASE="file://$PWD/dist" RAFTRA_INSTALL_DIR=/tmp/raftra-bin sh site/install.sh
/tmp/raftra-bin/raftra-cli --help 2>&1 | grep -- --addr
```

Expected:
- `Downloading raftra-cli_darwin_arm64.tar.gz ...` (or your platform)
- `Installed raftra-cli to /tmp/raftra-bin/raftra-cli`
- the PATH note
- `Try it:  raftra-cli status`
- the `--addr` line showing the three duckdns addresses

- [ ] **Step 4: User tests that a tampered download is refused**

```bash
rm -rf /tmp/raftra-bad && cp -R dist /tmp/raftra-bad
echo "0000000000000000000000000000000000000000000000000000000000000000  raftra-cli_$(uname -s | tr A-Z a-z)_$(uname -m | sed 's/x86_64/amd64/;s/aarch64/arm64/').tar.gz" > /tmp/raftra-bad/checksums.txt
RAFTRA_DOWNLOAD_BASE="file:///tmp/raftra-bad" RAFTRA_INSTALL_DIR=/tmp/raftra-bin2 sh site/install.sh; echo "exit=$?"
```

Expected: `Checksum mismatch for raftra-cli_…: refusing to install`, then `exit=1`, and `/tmp/raftra-bin2/raftra-cli` does not exist.

Clean up: `rm -rf /tmp/raftra-bin /tmp/raftra-bin2 /tmp/raftra-bad /tmp/raftra-rc`

- [ ] **Step 5: Update the docs**

5a. `README.md`, in Quick Start "### 3. Talk to the cluster", directly after the paragraph that starts `One-off commands work too:`, add:

````markdown
`--addr` accepts a comma-separated list, e.g. `--addr http://localhost:8001,http://localhost:8002,http://localhost:8003`. The CLI tries the nodes in order and moves on when one is down or mid-election (HTTP 502/503/504), retrying for up to about 3 seconds. Playground limits come back as plain-language messages.

**Prebuilt CLI.** Every tagged release publishes `raftra-cli` for macOS, Linux and Windows on the [Releases page](https://github.com/shantanu-1607/Raftra/releases). On macOS or Linux:

```bash
curl -fsSL https://raw.githubusercontent.com/shantanu-1607/Raftra/main/site/install.sh | sh
```

Release builds connect to the public playground cluster by default. Pass `--addr` to use your own cluster. On macOS, if you download the archive with a browser instead, clear the quarantine flag first: `xattr -d com.apple.quarantine raftra-cli`.
````

5b. `AGENTS.md`, Repository Structure: after the `` `deployments/` `` bullet, add:

```markdown
- `.github/workflows/`: `ci.yml` (gofmt, vet, race tests on pushes to `main` and PRs) and `release.yml` (GoReleaser on `v*` tags).
- `.goreleaser.yaml`: builds `raftra-cli` (darwin/linux/windows × amd64/arm64) and `raftra-server` (linux × amd64/arm64) archives plus `checksums.txt`. Release builds set `main.defaultAddr` to the public playground nodes (`raftra-n1/n2/n3.duckdns.org`).
- `site/install.sh`: `curl | sh` installer for the CLI (verifies checksums; `RAFTRA_INSTALL_DIR` / `RAFTRA_DOWNLOAD_BASE` overrides).
```

5c. `AGENTS.md`, Development Workflow: after the `**Load generator:**` bullet, add:

```markdown
- **CLI addresses:** `raftra-cli --addr` takes a comma-separated node list (default `main.defaultAddr`: `http://localhost:8001` for local builds). It fails over on connection errors and HTTP 502/503/504, pausing 300 ms between passes, with a 3 s budget (`cmd/raftra-cli/client.go`).
- **Releases:** push a tag like `v0.2.0` to publish binaries. Dry run locally: `go run github.com/goreleaser/goreleaser/v2@latest release --snapshot --clean` (output in `dist/`, git-ignored). GoReleaser is not a module dependency.
```

5d. The spec, `docs/superpowers/specs/2026-10-03-public-playground-design.md`: in the §5 release-pipeline table's `site/install.sh` row, replace `curl -fsSL https://shantanu-1607.github.io/Raftra/install.sh \| sh` with `curl -fsSL https://raw.githubusercontent.com/shantanu-1607/Raftra/main/site/install.sh \| sh` (the GitHub Pages URL also works once Plan 4 publishes `site/`).

- [ ] **Step 6: Hand-off (user commits)**

Files: `site/install.sh`, `README.md`, `AGENTS.md`, `docs/superpowers/specs/2026-10-03-public-playground-design.md`
Suggested message: `feat: add one-line CLI installer and document releases`

**After committing all of Plan 2:** push to GitHub, check that the CI workflow passes on the Actions tab, then publish the first release with:

```bash
git tag v0.2.0 && git push origin v0.2.0
```

Expected: the "Release" workflow succeeds, and https://github.com/shantanu-1607/Raftra/releases/tag/v0.2.0 lists the 8 archives plus `checksums.txt`.

# Playground Server Limits Implementation Plan (Plan 1 of 4)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add opt-in playground protections to the `raftra-server` HTTP gateway: key and value size limits, a key-count cap, per-IP write rate limiting, proxy-aware client IPs, and CORS on `/status`. With every flag at its default, behavior must be identical to today.

**Architecture:** A new `Limits` config struct and a small in-repo token-bucket `rateLimiter` live in `internal/transport/`. `HTTPServer` receives a `Limits` value and enforces it in the write handlers (`PUT`/`POST`/`DELETE`) after the existing follower-redirect check. `cmd/raftra-server/main.go` exposes each limit as a flag that defaults to off.

**Tech Stack:** Go 1.26 standard library only (`net/http`, `net/http/httptest`, `math`, `sync`). No new dependencies.

**Spec:** `docs/superpowers/specs/2026-10-03-public-playground-design.md` §3.

## Global Constraints

- All new flags **default to off**. With `transport.Limits{}` (the zero value), behavior must match today exactly.
- **No new Go module dependencies.** Don't use `golang.org/x/time/rate`.
- Flag names and playground values, verbatim from the spec: `-max-key-bytes` (128), `-max-value-bytes` (1024), `-max-keys` (10000), `-write-rate` (5), `-write-burst` (10), `-trust-proxy` (true), `-cors-origin` (`https://shantanu-1607.github.io`).
- Status codes: oversize key or value → `413`; rate limited → `429` with header `Retry-After: 1`; store full for a new key → `507`. Every error body is JSON `{"error": "..."}`.
- Order of checks on a write: redirect-if-follower → rate limit → key size → key-count cap → body size → `ProposeCommand`.
- Limits apply only to HTTP writes (`PUT`, `POST`, `DELETE`). Reads, `/status`, `/metrics` and gRPC are not limited.
- `-write-burst 0` with `-write-rate` > 0 means burst = `ceil(write-rate)`.
- **The user runs every test command** (`AGENTS.md` interactive testing mode). The implementer provides the command and its expected output.
- **Never commit.** The user makes all commits. "Hand-off" steps list the files and a suggested commit message.
- After writing code, explain it to the user in detail in simple language.

## File Structure

| File | Status | Responsibility |
|---|---|---|
| `internal/transport/ratelimit.go` | Create | `rateLimiter` (per-key token bucket with an injectable clock and idle sweep) and `clientIP` (proxy-aware client address) |
| `internal/transport/ratelimit_test.go` | Create | Unit tests for the limiter and `clientIP`, using a fake clock |
| `internal/transport/limits.go` | Create | `Limits` config struct, `writeJSONError`, `(*HTTPServer).enforceWriteLimits`, `(*HTTPServer).readValue` |
| `internal/transport/http_server.go` | Modify | Store `limits` and `limiter`. New `NewHTTPServer` parameter. Call the limit helpers in the write handlers. CORS header in `handleStatus`. |
| `internal/transport/http_server_test.go` | Create | HTTP-level tests against a real single-node `RaftNode` |
| `cmd/raftra-server/main.go` | Modify | Seven new flags → `transport.Limits`, passed to `NewHTTPServer` |
| `README.md`, `AGENTS.md` | Modify | Document the flags and status codes |

---

### Task 1: Token-bucket rate limiter and client IP helper

**Files:**
- Create: `internal/transport/ratelimit.go`
- Test: `internal/transport/ratelimit_test.go`

**Interfaces:**
- Consumes: nothing.
- Produces:
  - `func newRateLimiter(rate float64, burst int, now func() time.Time) *rateLimiter`
  - `func (rl *rateLimiter) allow(key string) bool`
  - `func clientIP(r *http.Request, trustProxy bool) string`

- [ ] **Step 1: Write the failing tests**

Create `internal/transport/ratelimit_test.go`:

```go
package transport

import (
	"net/http/httptest"
	"testing"
	"time"
)

// fakeClock lets tests move time forward without sleeping.
type fakeClock struct{ t time.Time }

func (c *fakeClock) now() time.Time         { return c.t }
func (c *fakeClock) advance(d time.Duration) { c.t = c.t.Add(d) }

func newFakeClock() *fakeClock {
	return &fakeClock{t: time.Date(2026, 10, 3, 12, 0, 0, 0, time.UTC)}
}

func TestRateLimiterBurstThenRefill(t *testing.T) {
	clock := newFakeClock()
	rl := newRateLimiter(2, 3, clock.now) // 2 tokens/sec, bucket of 3

	for i := 1; i <= 3; i++ {
		if !rl.allow("1.1.1.1") {
			t.Fatalf("request %d within burst was rejected", i)
		}
	}
	if rl.allow("1.1.1.1") {
		t.Fatal("4th immediate request should be rejected")
	}

	clock.advance(500 * time.Millisecond) // refills 1 token at 2/sec
	if !rl.allow("1.1.1.1") {
		t.Fatal("request after 500ms refill was rejected")
	}
	if rl.allow("1.1.1.1") {
		t.Fatal("only one token should have refilled")
	}

	clock.advance(10 * time.Second) // refill is capped at the burst size
	for i := 1; i <= 3; i++ {
		if !rl.allow("1.1.1.1") {
			t.Fatalf("request %d after long idle was rejected", i)
		}
	}
	if rl.allow("1.1.1.1") {
		t.Fatal("bucket must never hold more than the burst size")
	}
}

func TestRateLimiterKeysAreIndependent(t *testing.T) {
	clock := newFakeClock()
	rl := newRateLimiter(1, 1, clock.now)

	if !rl.allow("1.1.1.1") {
		t.Fatal("first request from 1.1.1.1 rejected")
	}
	if rl.allow("1.1.1.1") {
		t.Fatal("second request from 1.1.1.1 should be rejected")
	}
	if !rl.allow("2.2.2.2") {
		t.Fatal("a different client must have its own bucket")
	}
}

func TestRateLimiterDefaultBurstIsCeilOfRate(t *testing.T) {
	clock := newFakeClock()
	rl := newRateLimiter(2.5, 0, clock.now) // burst 0 → ceil(2.5) = 3

	for i := 1; i <= 3; i++ {
		if !rl.allow("1.1.1.1") {
			t.Fatalf("request %d within default burst was rejected", i)
		}
	}
	if rl.allow("1.1.1.1") {
		t.Fatal("default burst should be 3")
	}
}

func TestRateLimiterSweepsIdleBuckets(t *testing.T) {
	clock := newFakeClock()
	rl := newRateLimiter(1, 1, clock.now)

	rl.allow("1.1.1.1")
	clock.advance(2 * time.Minute)
	rl.allow("2.2.2.2") // triggers the sweep

	if _, ok := rl.buckets["1.1.1.1"]; ok {
		t.Fatal("idle bucket for 1.1.1.1 should have been swept")
	}
	if len(rl.buckets) != 1 {
		t.Fatalf("expected 1 bucket after sweep, got %d", len(rl.buckets))
	}
}

func TestClientIP(t *testing.T) {
	tests := []struct {
		name       string
		remoteAddr string
		xff        string
		trustProxy bool
		want       string
	}{
		{"remote addr without proxy", "203.0.113.7:5555", "", false, "203.0.113.7"},
		{"xff ignored when proxy not trusted", "127.0.0.1:5555", "198.51.100.9", false, "127.0.0.1"},
		{"xff used when proxy trusted", "127.0.0.1:5555", "198.51.100.9", true, "198.51.100.9"},
		{"last xff hop wins", "127.0.0.1:5555", "10.9.9.9, 198.51.100.9", true, "198.51.100.9"},
		{"trusted proxy but no xff", "127.0.0.1:5555", "", true, "127.0.0.1"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest("PUT", "/api/v1/kv/k", nil)
			req.RemoteAddr = tc.remoteAddr
			if tc.xff != "" {
				req.Header.Set("X-Forwarded-For", tc.xff)
			}
			if got := clientIP(req, tc.trustProxy); got != tc.want {
				t.Fatalf("clientIP() = %q, want %q", got, tc.want)
			}
		})
	}
}
```

- [ ] **Step 2: User runs the tests to confirm they fail**

Run: `go test ./internal/transport/ -run 'TestRateLimiter|TestClientIP' -v`
Expected: build failure, with errors like `undefined: newRateLimiter` and `undefined: clientIP`.

- [ ] **Step 3: Write the implementation**

Create `internal/transport/ratelimit.go`:

```go
package transport

import (
	"math"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"
)

// rateLimiter is a per-client token bucket. Each client starts with a full
// bucket of `burst` tokens, every write costs one token, and tokens refill
// continuously at `rate` per second up to the burst size.
type rateLimiter struct {
	mu        sync.Mutex
	rate      float64
	burst     float64
	now       func() time.Time
	buckets   map[string]*bucket
	lastSweep time.Time
}

type bucket struct {
	tokens float64
	last   time.Time
}

// newRateLimiter creates a limiter allowing `rate` writes per second per key.
// A burst of 0 or less defaults to ceil(rate).
func newRateLimiter(rate float64, burst int, now func() time.Time) *rateLimiter {
	b := float64(burst)
	if b <= 0 {
		b = math.Ceil(rate)
	}
	return &rateLimiter{
		rate:      rate,
		burst:     b,
		now:       now,
		buckets:   make(map[string]*bucket),
		lastSweep: now(),
	}
}

// allow reports whether the client identified by key may perform one more write.
func (rl *rateLimiter) allow(key string) bool {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	now := rl.now()
	rl.sweepLocked(now)

	b, ok := rl.buckets[key]
	if !ok {
		b = &bucket{tokens: rl.burst, last: now}
		rl.buckets[key] = b
	}

	elapsed := now.Sub(b.last).Seconds()
	b.tokens = math.Min(rl.burst, b.tokens+elapsed*rl.rate)
	b.last = now

	if b.tokens < 1 {
		return false
	}
	b.tokens--
	return true
}

// sweepLocked drops buckets that have been idle long enough to refill completely.
// A fresh bucket would be identical, so memory stays bounded by recently active clients.
// NOTE: Caller MUST hold rl.mu.
func (rl *rateLimiter) sweepLocked(now time.Time) {
	if now.Sub(rl.lastSweep) < time.Minute {
		return
	}
	rl.lastSweep = now
	refillTime := time.Duration(rl.burst / rl.rate * float64(time.Second))
	for key, b := range rl.buckets {
		if now.Sub(b.last) > refillTime {
			delete(rl.buckets, key)
		}
	}
}

// clientIP returns the address used to identify a client for rate limiting.
// Behind a trusted reverse proxy (Caddy), the TCP peer is always the proxy, so the
// real client is the last X-Forwarded-For hop, which the proxy appends itself.
func clientIP(r *http.Request, trustProxy bool) string {
	if trustProxy {
		if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
			hops := strings.Split(xff, ",")
			if ip := strings.TrimSpace(hops[len(hops)-1]); ip != "" {
				return ip
			}
		}
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}
```

- [ ] **Step 4: User runs the tests to confirm they pass**

Run: `go test -race ./internal/transport/ -run 'TestRateLimiter|TestClientIP' -v`
Expected: `--- PASS` for `TestRateLimiterBurstThenRefill`, `TestRateLimiterKeysAreIndependent`, `TestRateLimiterDefaultBurstIsCeilOfRate`, `TestRateLimiterSweepsIdleBuckets`, and all 5 `TestClientIP` subtests, then `ok  github.com/shantanu-1607/raftra/internal/transport`.

- [ ] **Step 5: Hand-off (user commits)**

Files: `internal/transport/ratelimit.go`, `internal/transport/ratelimit_test.go`
Suggested message: `feat(transport): add per-client token-bucket rate limiter`

---

### Task 2: `Limits` config with size limits and key-count cap in the HTTP gateway

**Files:**
- Create: `internal/transport/limits.go`
- Modify: `internal/transport/http_server.go` (struct, `NewHTTPServer`, `handlePut`, `handlePost`, `handleDelete`)
- Modify: `cmd/raftra-server/main.go:138` (temporary `transport.Limits{}` so it compiles; Task 4 adds the flags)
- Test: `internal/transport/http_server_test.go`

**Interfaces:**
- Consumes: nothing from Task 1 yet.
- Produces:
  - `type Limits struct { MaxKeyBytes int; MaxValueBytes int64; MaxKeys int; WriteRate float64; WriteBurst int; TrustProxy bool; CORSOrigin string }`
  - `func NewHTTPServer(node *raft.RaftNode, addr string, peerHTTPAddrs map[string]string, logger *slog.Logger, m *metrics.Metrics, limits Limits) *HTTPServer`
  - `func writeJSONError(w http.ResponseWriter, status int, msg string)`
  - `func (s *HTTPServer) enforceWriteLimits(w http.ResponseWriter, r *http.Request, key string, createsKey bool) bool`
  - `func (s *HTTPServer) readValue(w http.ResponseWriter, r *http.Request) (string, bool)`
  - Test helpers `newTestServer(t, Limits) *HTTPServer`, `newReq(method, path, body string) *http.Request`, `serve(s, req) *httptest.ResponseRecorder`, which Task 3 reuses.

- [ ] **Step 1: Write the failing tests**

Create `internal/transport/http_server_test.go`:

```go
package transport

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/shantanu-1607/raftra/internal/kvstore"
	"github.com/shantanu-1607/raftra/internal/raft"
	"github.com/shantanu-1607/raftra/internal/storage"
)

// newTestServer starts a real single-node Raft cluster (it elects itself and commits
// proposals immediately) and wraps it in an HTTPServer with the given limits.
func newTestServer(t *testing.T, limits Limits) *HTTPServer {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	cfg := raft.DefaultConfig("node1", nil)
	cfg.ElectionTimeoutMin = 20 * time.Millisecond
	cfg.ElectionTimeoutMax = 40 * time.Millisecond

	node, err := raft.NewRaftNode(cfg, storage.NewMemoryStore(), kvstore.NewKVStore(), logger)
	if err != nil {
		t.Fatalf("failed to create raft node: %v", err)
	}
	node.Start()
	t.Cleanup(node.Stop)

	deadline := time.Now().Add(2 * time.Second)
	for !node.IsLeader() {
		if time.Now().After(deadline) {
			t.Fatal("single node never became leader")
		}
		time.Sleep(5 * time.Millisecond)
	}
	return NewHTTPServer(node, "127.0.0.1:0", nil, logger, nil, limits)
}

func newReq(method, path, body string) *http.Request {
	return httptest.NewRequest(method, path, strings.NewReader(body))
}

func serve(s *HTTPServer, req *http.Request) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	s.server.Handler.ServeHTTP(rec, req)
	return rec
}

func expectStatus(t *testing.T, rec *httptest.ResponseRecorder, want int) {
	t.Helper()
	if rec.Code != want {
		t.Fatalf("expected HTTP %d, got %d (body: %s)", want, rec.Code, rec.Body.String())
	}
}

func TestNoLimitsByDefault(t *testing.T) {
	s := newTestServer(t, Limits{})

	longKey := strings.Repeat("k", 300)
	bigValue := strings.Repeat("v", 5000)
	expectStatus(t, serve(s, newReq("PUT", "/api/v1/kv/"+longKey, bigValue)), http.StatusOK)

	for i := 0; i < 50; i++ {
		expectStatus(t, serve(s, newReq("PUT", "/api/v1/kv/fast", "x")), http.StatusOK)
	}

	rec := serve(s, newReq("GET", "/status", ""))
	expectStatus(t, rec, http.StatusOK)
	if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "" {
		t.Fatalf("expected no CORS header by default, got %q", got)
	}
}

func TestKeyTooLarge(t *testing.T) {
	s := newTestServer(t, Limits{MaxKeyBytes: 8})

	rec := serve(s, newReq("PUT", "/api/v1/kv/123456789", "v"))
	expectStatus(t, rec, http.StatusRequestEntityTooLarge)
	if !strings.Contains(rec.Body.String(), "key too large") {
		t.Fatalf("expected 'key too large' error, got %s", rec.Body.String())
	}

	expectStatus(t, serve(s, newReq("PUT", "/api/v1/kv/12345678", "v")), http.StatusOK)
	expectStatus(t, serve(s, newReq("DELETE", "/api/v1/kv/123456789", "")), http.StatusRequestEntityTooLarge)
}

func TestValueTooLarge(t *testing.T) {
	s := newTestServer(t, Limits{MaxValueBytes: 10})

	rec := serve(s, newReq("PUT", "/api/v1/kv/k", strings.Repeat("v", 11)))
	expectStatus(t, rec, http.StatusRequestEntityTooLarge)
	if !strings.Contains(rec.Body.String(), "value too large") {
		t.Fatalf("expected 'value too large' error, got %s", rec.Body.String())
	}

	expectStatus(t, serve(s, newReq("PUT", "/api/v1/kv/k", strings.Repeat("v", 10))), http.StatusOK)
	expectStatus(t, serve(s, newReq("POST", "/api/v1/kv/new", strings.Repeat("v", 11))), http.StatusRequestEntityTooLarge)
}

func TestStoreFullRejectsOnlyNewKeys(t *testing.T) {
	s := newTestServer(t, Limits{MaxKeys: 2})

	expectStatus(t, serve(s, newReq("PUT", "/api/v1/kv/a", "1")), http.StatusOK)
	expectStatus(t, serve(s, newReq("PUT", "/api/v1/kv/b", "2")), http.StatusOK)

	rec := serve(s, newReq("PUT", "/api/v1/kv/c", "3"))
	expectStatus(t, rec, http.StatusInsufficientStorage)
	if !strings.Contains(rec.Body.String(), "store is full") {
		t.Fatalf("expected 'store is full' error, got %s", rec.Body.String())
	}
	expectStatus(t, serve(s, newReq("POST", "/api/v1/kv/c", "3")), http.StatusInsufficientStorage)

	expectStatus(t, serve(s, newReq("PUT", "/api/v1/kv/a", "overwrite")), http.StatusOK)
	expectStatus(t, serve(s, newReq("DELETE", "/api/v1/kv/b", "")), http.StatusOK)
	expectStatus(t, serve(s, newReq("PUT", "/api/v1/kv/c", "3")), http.StatusOK)
}
```

- [ ] **Step 2: User runs the tests to confirm they fail**

Run: `go test ./internal/transport/ -run 'TestNoLimitsByDefault|TestKeyTooLarge|TestValueTooLarge|TestStoreFull' -v`
Expected: build failure, with errors like `undefined: Limits` and `too many arguments in call to NewHTTPServer`.

- [ ] **Step 3: Create `internal/transport/limits.go`**

```go
package transport

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
)

// Limits configures optional playground protections for the HTTP gateway.
// The zero value disables every limit, preserving the original behavior.
type Limits struct {
	MaxKeyBytes   int     // reject writes whose key is longer than this; 0 = unlimited
	MaxValueBytes int64   // reject PUT/POST bodies larger than this; 0 = unlimited
	MaxKeys       int     // reject writes that would add a new key once the store holds this many; 0 = unlimited
	WriteRate     float64 // writes per second allowed per client IP; 0 = unlimited
	WriteBurst    int     // token bucket size for WriteRate; 0 = ceil(WriteRate)
	TrustProxy    bool    // identify clients by X-Forwarded-For (only behind a trusted proxy)
	CORSOrigin    string  // Access-Control-Allow-Origin value for GET /status; "" = no header
}

// writeJSONError sends an error response using the gateway's {"error": "..."} shape.
func writeJSONError(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": msg})
}

// enforceWriteLimits applies the playground limits to a write this node is about to propose.
// createsKey is true for PUT/POST (which may add a key) and false for DELETE.
// It writes the error response and returns false when the write is rejected.
func (s *HTTPServer) enforceWriteLimits(w http.ResponseWriter, r *http.Request, key string, createsKey bool) bool {
	if s.limits.MaxKeyBytes > 0 && len(key) > s.limits.MaxKeyBytes {
		writeJSONError(w, http.StatusRequestEntityTooLarge,
			fmt.Sprintf("key too large (max %d bytes)", s.limits.MaxKeyBytes))
		return false
	}
	if createsKey && s.limits.MaxKeys > 0 && s.node.KVStoreSize() >= s.limits.MaxKeys {
		if _, exists := s.node.Get(key); !exists {
			writeJSONError(w, http.StatusInsufficientStorage,
				fmt.Sprintf("store is full (max %d keys)", s.limits.MaxKeys))
			return false
		}
	}
	return true
}

// readValue reads the request body as the value to store, enforcing MaxValueBytes.
// It writes the error response and returns false when the body is rejected.
func (s *HTTPServer) readValue(w http.ResponseWriter, r *http.Request) (string, bool) {
	body := r.Body
	if s.limits.MaxValueBytes > 0 {
		body = http.MaxBytesReader(w, r.Body, s.limits.MaxValueBytes)
	}
	data, err := io.ReadAll(body)
	if err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			writeJSONError(w, http.StatusRequestEntityTooLarge,
				fmt.Sprintf("value too large (max %d bytes)", s.limits.MaxValueBytes))
			return "", false
		}
		http.Error(w, "failed to read body", http.StatusBadRequest)
		return "", false
	}
	return string(data), true
}
```

- [ ] **Step 4: Wire `Limits` into `internal/transport/http_server.go`**

4a. Add the field to the struct. Replace:

```go
	peerHTTPAddrs map[string]string // nodeID -> "http://localhost:8001"
	metrics       *metrics.Metrics
}
```

with:

```go
	peerHTTPAddrs map[string]string // nodeID -> "http://localhost:8001"
	metrics       *metrics.Metrics
	limits        Limits
}
```

4b. Change the constructor signature and store the limits. Replace:

```go
func NewHTTPServer(node *raft.RaftNode, addr string, peerHTTPAddrs map[string]string, logger *slog.Logger, m *metrics.Metrics) *HTTPServer {
	hs := &HTTPServer{
		node:          node,
		logger:        logger,
		peerHTTPAddrs: peerHTTPAddrs,
		metrics:       m,
	}
```

with:

```go
func NewHTTPServer(node *raft.RaftNode, addr string, peerHTTPAddrs map[string]string, logger *slog.Logger, m *metrics.Metrics, limits Limits) *HTTPServer {
	hs := &HTTPServer{
		node:          node,
		logger:        logger,
		peerHTTPAddrs: peerHTTPAddrs,
		metrics:       m,
		limits:        limits,
	}
```

4c. In `handlePut`, replace:

```go
	if s.redirectIfFollower(w, r) {
		return
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "failed to read body", http.StatusBadRequest)
		return
	}
	value := string(body)
	cmd := kvstore.Command{
		Type:  kvstore.CmdSet,
		Key:   key,
		Value: value,
	}
	encoded, err := cmd.Encode()
```

with:

```go
	if s.redirectIfFollower(w, r) {
		return
	}
	if !s.enforceWriteLimits(w, r, key, true) {
		return
	}
	value, ok := s.readValue(w, r)
	if !ok {
		return
	}
	cmd := kvstore.Command{
		Type:  kvstore.CmdSet,
		Key:   key,
		Value: value,
	}
	encoded, err := cmd.Encode()
```

4d. In `handlePost`, replace:

```go
	if s.redirectIfFollower(w, r) {
		return
	}
	// 1. Check if key already exists (Distributed Lock semantics)
```

with:

```go
	if s.redirectIfFollower(w, r) {
		return
	}
	if !s.enforceWriteLimits(w, r, key, true) {
		return
	}
	// 1. Check if key already exists (Distributed Lock semantics)
```

and further down in `handlePost`, replace:

```go
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "failed to read body", http.StatusBadRequest)
		return
	}
	value := string(body)
	cmd := kvstore.Command{
		Type:  kvstore.CmdSet,
		Key:   key,
		Value: value,
	}
	encoded, _ := cmd.Encode()
	_, err = s.node.ProposeCommand(encoded)
```

with:

```go
	value, ok := s.readValue(w, r)
	if !ok {
		return
	}
	cmd := kvstore.Command{
		Type:  kvstore.CmdSet,
		Key:   key,
		Value: value,
	}
	encoded, _ := cmd.Encode()
	_, err := s.node.ProposeCommand(encoded)
```

4e. In `handleDelete`, replace:

```go
	if s.redirectIfFollower(w, r) {
		return
	}
	cmd := kvstore.Command{
		Type: kvstore.CmdDelete,
```

with:

```go
	if s.redirectIfFollower(w, r) {
		return
	}
	if !s.enforceWriteLimits(w, r, key, false) {
		return
	}
	cmd := kvstore.Command{
		Type: kvstore.CmdDelete,
```

4f. `io` is still used elsewhere in `http_server.go`? After 4c/4d it isn't. **Remove `"io"` from the import block of `http_server.go`**, or the build fails with `"io" imported and not used`.

- [ ] **Step 5: Keep `cmd/raftra-server/main.go` compiling**

Replace:

```go
	httpServer := transport.NewHTTPServer(raftNode, httpServerAddr, peerHTTPMap, logger, m)
```

with:

```go
	httpServer := transport.NewHTTPServer(raftNode, httpServerAddr, peerHTTPMap, logger, m, transport.Limits{})
```

(Task 4 replaces `transport.Limits{}` with the flag values.)

- [ ] **Step 6: User runs the tests to confirm they pass**

Run: `go test -race ./internal/transport/ -v`
Expected: `--- PASS` for `TestNoLimitsByDefault`, `TestKeyTooLarge`, `TestValueTooLarge`, `TestStoreFullRejectsOnlyNewKeys`, and all Task 1 tests, then `ok  github.com/shantanu-1607/raftra/internal/transport`.

Then run: `go build ./... && go vet ./...`
Expected: no output.

- [ ] **Step 7: Hand-off (user commits)**

Files: `internal/transport/limits.go`, `internal/transport/http_server.go`, `internal/transport/http_server_test.go`, `cmd/raftra-server/main.go`
Suggested message: `feat(transport): add key/value size limits and key-count cap to HTTP writes`

---

### Task 3: Per-IP write rate limiting and CORS on `/status`

**Files:**
- Modify: `internal/transport/http_server.go` (struct, `NewHTTPServer`, `handleStatus`)
- Modify: `internal/transport/limits.go` (`enforceWriteLimits`)
- Test: `internal/transport/http_server_test.go` (append)

**Interfaces:**
- Consumes: `newRateLimiter`, `(*rateLimiter).allow`, `clientIP` (Task 1). `Limits`, `enforceWriteLimits`, `writeJSONError`, `newTestServer`, `newReq`, `serve`, `expectStatus` (Task 2).
- Produces: `HTTPServer.limiter *rateLimiter` (nil when `WriteRate <= 0`).

- [ ] **Step 1: Write the failing tests**

Append to `internal/transport/http_server_test.go`:

```go
func TestWriteRateLimitPerClient(t *testing.T) {
	s := newTestServer(t, Limits{WriteRate: 1, WriteBurst: 2})

	put := func(remoteAddr string) *httptest.ResponseRecorder {
		req := newReq("PUT", "/api/v1/kv/k", "v")
		req.RemoteAddr = remoteAddr
		return serve(s, req)
	}

	expectStatus(t, put("203.0.113.1:1000"), http.StatusOK)
	expectStatus(t, put("203.0.113.1:1001"), http.StatusOK)

	rec := put("203.0.113.1:1002")
	expectStatus(t, rec, http.StatusTooManyRequests)
	if got := rec.Header().Get("Retry-After"); got != "1" {
		t.Fatalf("expected Retry-After: 1, got %q", got)
	}

	expectStatus(t, put("203.0.113.2:1000"), http.StatusOK) // different client, own bucket

	read := newReq("GET", "/api/v1/kv/k", "")
	read.RemoteAddr = "203.0.113.1:1003"
	expectStatus(t, serve(s, read), http.StatusOK) // reads are never rate limited
}

func TestRateLimitUsesForwardedForOnlyWhenTrusted(t *testing.T) {
	put := func(s *HTTPServer, xff string) *httptest.ResponseRecorder {
		req := newReq("PUT", "/api/v1/kv/k", "v")
		req.RemoteAddr = "127.0.0.1:9000" // every request arrives from the local proxy
		req.Header.Set("X-Forwarded-For", xff)
		return serve(s, req)
	}

	trusted := newTestServer(t, Limits{WriteRate: 1, WriteBurst: 1, TrustProxy: true})
	expectStatus(t, put(trusted, "198.51.100.1"), http.StatusOK)
	expectStatus(t, put(trusted, "198.51.100.2"), http.StatusOK) // different real client
	expectStatus(t, put(trusted, "198.51.100.1"), http.StatusTooManyRequests)

	untrusted := newTestServer(t, Limits{WriteRate: 1, WriteBurst: 1})
	expectStatus(t, put(untrusted, "198.51.100.1"), http.StatusOK)
	expectStatus(t, put(untrusted, "198.51.100.2"), http.StatusTooManyRequests) // same TCP peer
}

func TestCORSHeaderOnStatus(t *testing.T) {
	s := newTestServer(t, Limits{CORSOrigin: "https://shantanu-1607.github.io"})

	rec := serve(s, newReq("GET", "/status", ""))
	expectStatus(t, rec, http.StatusOK)
	if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "https://shantanu-1607.github.io" {
		t.Fatalf("expected CORS origin header, got %q", got)
	}
}
```

- [ ] **Step 2: User runs the tests to confirm they fail**

Run: `go test ./internal/transport/ -run 'TestWriteRateLimitPerClient|TestRateLimitUsesForwardedFor|TestCORSHeaderOnStatus' -v`
Expected: all three FAIL. The rate tests show `expected HTTP 429, got 200`, and the CORS test shows `expected CORS origin header, got ""`.

- [ ] **Step 3: Add the limiter to `HTTPServer`**

3a. In `internal/transport/http_server.go`, replace:

```go
	metrics       *metrics.Metrics
	limits        Limits
}
```

with:

```go
	metrics       *metrics.Metrics
	limits        Limits
	limiter       *rateLimiter // nil when write rate limiting is disabled
}
```

3b. In `NewHTTPServer`, replace:

```go
		limits:        limits,
	}
```

with:

```go
		limits:        limits,
	}
	if limits.WriteRate > 0 {
		hs.limiter = newRateLimiter(limits.WriteRate, limits.WriteBurst, time.Now)
	}
```

3c. In `handleStatus`, replace:

```go
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(status)
}
```

with:

```go
	w.Header().Set("Content-Type", "application/json")
	if s.limits.CORSOrigin != "" {
		w.Header().Set("Access-Control-Allow-Origin", s.limits.CORSOrigin)
	}
	_ = json.NewEncoder(w).Encode(status)
}
```

- [ ] **Step 4: Check the rate limit first in `enforceWriteLimits`**

In `internal/transport/limits.go`, replace:

```go
func (s *HTTPServer) enforceWriteLimits(w http.ResponseWriter, r *http.Request, key string, createsKey bool) bool {
	if s.limits.MaxKeyBytes > 0 && len(key) > s.limits.MaxKeyBytes {
```

with:

```go
func (s *HTTPServer) enforceWriteLimits(w http.ResponseWriter, r *http.Request, key string, createsKey bool) bool {
	if s.limiter != nil && !s.limiter.allow(clientIP(r, s.limits.TrustProxy)) {
		w.Header().Set("Retry-After", "1")
		writeJSONError(w, http.StatusTooManyRequests, "rate limit exceeded: slow down and retry in a second")
		return false
	}
	if s.limits.MaxKeyBytes > 0 && len(key) > s.limits.MaxKeyBytes {
```

- [ ] **Step 5: User runs the tests to confirm they pass**

Run: `go test -race ./internal/transport/ -v`
Expected: `--- PASS` for all 7 HTTP tests and all limiter/`clientIP` tests, then `ok  github.com/shantanu-1607/raftra/internal/transport`.

- [ ] **Step 6: Hand-off (user commits)**

Files: `internal/transport/http_server.go`, `internal/transport/limits.go`, `internal/transport/http_server_test.go`
Suggested message: `feat(transport): rate-limit HTTP writes per client and add CORS to /status`

---

### Task 4: Server flags, docs, and local rehearsal

**Files:**
- Modify: `cmd/raftra-server/main.go` (flags block lines 23–32, `NewHTTPServer` call)
- Modify: `README.md` (Configuration → `raftra-server` flags table; API Reference HTTP table note)
- Modify: `AGENTS.md` (Development Workflow server flags; Architecture bullet)

**Interfaces:**
- Consumes: `transport.Limits` and the `NewHTTPServer(..., limits Limits)` signature (Task 2).
- Produces: CLI flags `-max-key-bytes`, `-max-value-bytes`, `-max-keys`, `-write-rate`, `-write-burst`, `-trust-proxy`, `-cors-origin`, which Plan 3 (deployment) uses in `raftra.env`.

- [ ] **Step 1: Add the flags**

In `cmd/raftra-server/main.go`, replace:

```go
	noSync := flag.Bool("nosync", false, "Disable bbolt fsync for benchmark mode (faster but less durable)")
	flag.Parse()
```

with:

```go
	noSync := flag.Bool("nosync", false, "Disable bbolt fsync for benchmark mode (faster but less durable)")

	// Playground protections for the HTTP gateway (all off by default)
	maxKeyBytes := flag.Int("max-key-bytes", 0, "Reject writes whose key is longer than this many bytes (0 = unlimited)")
	maxValueBytes := flag.Int64("max-value-bytes", 0, "Reject PUT/POST bodies larger than this many bytes (0 = unlimited)")
	maxKeys := flag.Int("max-keys", 0, "Reject writes that would add a new key once the store holds this many keys (0 = unlimited)")
	writeRate := flag.Float64("write-rate", 0, "Writes per second allowed per client IP (0 = unlimited)")
	writeBurst := flag.Int("write-burst", 0, "Burst size for -write-rate (0 = ceil of -write-rate)")
	trustProxy := flag.Bool("trust-proxy", false, "Identify clients by X-Forwarded-For (only behind a trusted reverse proxy)")
	corsOrigin := flag.String("cors-origin", "", "Access-Control-Allow-Origin value for GET /status (empty = no header)")
	flag.Parse()
```

- [ ] **Step 2: Pass the limits to the HTTP gateway**

Replace:

```go
	httpServer := transport.NewHTTPServer(raftNode, httpServerAddr, peerHTTPMap, logger, m, transport.Limits{})
```

with:

```go
	limits := transport.Limits{
		MaxKeyBytes:   *maxKeyBytes,
		MaxValueBytes: *maxValueBytes,
		MaxKeys:       *maxKeys,
		WriteRate:     *writeRate,
		WriteBurst:    *writeBurst,
		TrustProxy:    *trustProxy,
		CORSOrigin:    *corsOrigin,
	}
	if limits != (transport.Limits{}) {
		logger.Info("playground limits enabled",
			"max_key_bytes", limits.MaxKeyBytes,
			"max_value_bytes", limits.MaxValueBytes,
			"max_keys", limits.MaxKeys,
			"write_rate", limits.WriteRate,
			"write_burst", limits.WriteBurst,
			"trust_proxy", limits.TrustProxy,
			"cors_origin", limits.CORSOrigin,
		)
	}
	httpServer := transport.NewHTTPServer(raftNode, httpServerAddr, peerHTTPMap, logger, m, limits)
```

- [ ] **Step 3: Update the docs**

3a. `README.md`, in the `raftra-server` flags table, insert after the `-nosync` row:

```markdown
| `-max-key-bytes` | `0` (off) | Reject writes whose key is longer than this (→ `413`) |
| `-max-value-bytes` | `0` (off) | Reject `PUT`/`POST` bodies larger than this (→ `413`) |
| `-max-keys` | `0` (off) | Reject writes that would add a new key once the store is full (→ `507`). Overwrites and deletes still work. |
| `-write-rate` | `0` (off) | Writes per second allowed per client IP (→ `429` with `Retry-After: 1`) |
| `-write-burst` | `0` | Burst size for `-write-rate` (`0` = `ceil(write-rate)`) |
| `-trust-proxy` | `false` | Identify clients by `X-Forwarded-For`. Only enable this behind a trusted reverse proxy such as Caddy. |
| `-cors-origin` | — | `Access-Control-Allow-Origin` value for `GET /status`, so a web page can read cluster status |
```

3b. `README.md`, directly under the HTTP gateway table in the API Reference, add:

```markdown
When playground limits are enabled (see [Configuration](#%EF%B8%8F-configuration)), writes can also return `413` (key or value too large), `429` (rate limited) or `507` (store full). Limits are enforced by the node that proposes the write, i.e. the leader.
```

3c. `AGENTS.md`, in the Development Workflow "Run a node locally" bullet, replace the sentence `Server flags are \`-id\`, \`-host\`, \`-port\`, \`-http-port\`, \`-peers\`, \`-http-peers\`, \`-data-dir\` and \`-nosync\`.` with:

```markdown
Server flags are `-id`, `-host`, `-port`, `-http-port`, `-peers`, `-http-peers`, `-data-dir`, `-nosync`, plus the playground limits `-max-key-bytes`, `-max-value-bytes`, `-max-keys`, `-write-rate`, `-write-burst`, `-trust-proxy` and `-cors-origin` (all off by default; implemented in `internal/transport/limits.go` and `ratelimit.go`).
```

- [ ] **Step 4: User runs the full verification**

Run: `make test 2>&1 | grep -E '^(ok|FAIL|---)|DATA RACE'`
Expected: `ok` for `benchmark`, `internal/metrics`, `internal/raft`, `internal/storage`, `internal/transport` and `test/chaos`. No `FAIL`, no `DATA RACE`.

Run: `go vet ./... && gofmt -l .`
Expected: no output.

- [ ] **Step 5: User runs the local rehearsal (size limits, store cap, CORS)**

Run in terminal 1:
```bash
make build && rm -rf /tmp/raftra-rehearsal && ./bin/raftra-server -id solo -port 50061 -http-port 8061 -data-dir /tmp/raftra-rehearsal -max-key-bytes 8 -max-value-bytes 10 -max-keys 2 -cors-origin '*'
```
Expected: the log line `playground limits enabled ... max_key_bytes=8 max_value_bytes=10 max_keys=2 ...`, then `election won: become leader`.

Run in terminal 2:
```bash
code() { curl -s -o /dev/null -w '%{http_code}\n' "$@"; }
code -X PUT localhost:8061/api/v1/kv/123456789 -d v      # key 9 bytes
code -X PUT localhost:8061/api/v1/kv/k -d 12345678901    # value 11 bytes
code -X PUT localhost:8061/api/v1/kv/a -d 1
code -X PUT localhost:8061/api/v1/kv/b -d 2
code -X PUT localhost:8061/api/v1/kv/c -d 3              # third key
code -X PUT localhost:8061/api/v1/kv/a -d again          # overwrite
curl -si localhost:8061/status | grep -i access-control
```
Expected output, in order: `413`, `413`, `200`, `200`, `507`, `200`, then `Access-Control-Allow-Origin: *`.

Stop the server with Ctrl+C.

- [ ] **Step 6: User runs the local rehearsal (rate limit)**

Run in terminal 1:
```bash
rm -rf /tmp/raftra-rehearsal && ./bin/raftra-server -id solo -port 50061 -http-port 8061 -data-dir /tmp/raftra-rehearsal -write-rate 1 -write-burst 3
```

Run in terminal 2:
```bash
for i in 1 2 3 4 5; do curl -s -o /dev/null -w '%{http_code} ' -X PUT localhost:8061/api/v1/kv/k -d $i; done; echo
sleep 2; curl -s -o /dev/null -w '%{http_code}\n' -X PUT localhost:8061/api/v1/kv/k -d after-wait
```
Expected: `200 200 200 429 429`, then `200` after the 2-second wait.

Stop the server with Ctrl+C, then clean up: `rm -rf /tmp/raftra-rehearsal`.

- [ ] **Step 7: Hand-off (user commits)**

Files: `cmd/raftra-server/main.go`, `README.md`, `AGENTS.md`
Suggested message: `feat(server): expose playground limit flags and document them`

---

## Follow-on plans (written after this plan is complete)

- **Plan 2:** CLI changes (address list with failover, ldflags defaults, friendly limit messages), GoReleaser, and the CI and release workflows (spec §5).
- **Plan 3:** Lightsail deployment scripts, systemd units and timers, Caddyfile, and the runbook (spec §4).
- **Plan 4:** Landing page and the Pages workflow (spec §6).

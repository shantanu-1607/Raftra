package transport

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
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

func TestWriteRateLimitPerClient(t *testing.T) {
	s := newTestServer(t, Limits{WriteRate: 1, WriteBurst: 2})
	clock := newFakeClock()
	s.limiter = newRateLimiter(1, 2, clock.now)

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
	trusted.limiter = newRateLimiter(1, 1, newFakeClock().now)
	expectStatus(t, put(trusted, "198.51.100.1"), http.StatusOK)
	expectStatus(t, put(trusted, "198.51.100.2"), http.StatusOK) // different real client
	expectStatus(t, put(trusted, "198.51.100.1"), http.StatusTooManyRequests)

	untrusted := newTestServer(t, Limits{WriteRate: 1, WriteBurst: 1})
	untrusted.limiter = newRateLimiter(1, 1, newFakeClock().now)
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

func TestLeaderRedirectURL(t *testing.T) {
	const leader = "https://leader.example"
	tests := []struct {
		reqURL   string
		want     string
		wantPath string
	}{
		{"/kv/a%3Fb", leader + "/kv/a%3Fb", "/kv/a?b"},
		{"/kv/a%23b", leader + "/kv/a%23b", "/kv/a#b"},
		{"/kv/100%25", leader + "/kv/100%25", "/kv/100%"},
		{"/kv/plain", leader + "/kv/plain", "/kv/plain"},
		{"/kv/x?foo=1", leader + "/kv/x?foo=1", "/kv/x"},
	}
	for _, tc := range tests {
		in, err := url.Parse(tc.reqURL)
		if err != nil {
			t.Fatalf("parse %q: %v", tc.reqURL, err)
		}
		got := leaderRedirectURL(leader, in)
		if got != tc.want {
			t.Errorf("leaderRedirectURL(%q) = %q, want %q", tc.reqURL, got, tc.want)
		}
		out, err := url.Parse(got)
		if err != nil {
			t.Errorf("redirect %q does not parse: %v", got, err)
			continue
		}
		if out.Path != tc.wantPath {
			t.Errorf("redirect %q decodes to path %q, want %q", got, out.Path, tc.wantPath)
		}
	}
}

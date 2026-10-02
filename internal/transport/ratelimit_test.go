package transport

import (
	"net/http/httptest"
	"testing"
	"time"
)

// fakeClock lets tests move time forward without sleeping.
type fakeClock struct{ t time.Time }

func (c *fakeClock) now() time.Time          { return c.t }
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
		{"garbage xff falls back to remote addr", "127.0.0.1:5555", "not-an-ip", true, "127.0.0.1"},
		{"ipv4 remote unchanged", "192.0.2.1:443", "", false, "192.0.2.1"},
		{"ipv4-mapped ipv6 remote is plain ipv4", "[::ffff:192.0.2.1]:443", "", false, "192.0.2.1"},
		{"ipv6 remote keyed by /64", "[2001:db8:1:2::5]:443", "", false, "2001:db8:1:2::"},
		{"ipv6 xff keyed by /64", "127.0.0.1:5555", "2001:db8:1:2:aaaa::1", true, "2001:db8:1:2::"},
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

func TestClientIPIPv6SamePrefixSharesKey(t *testing.T) {
	key := func(remote string) string {
		req := httptest.NewRequest("PUT", "/api/v1/kv/k", nil)
		req.RemoteAddr = remote
		return clientIP(req, false)
	}
	if key("[2001:db8:1:2::5]:443") != key("[2001:db8:1:2::9]:443") {
		t.Fatal("addresses in the same /64 must share a key")
	}
	if key("[2001:db8:1:2::5]:443") == key("[2001:db8:1:3::5]:443") {
		t.Fatal("addresses in different /64s must not share a key")
	}
}

func TestClientIPMultipleXFFLines(t *testing.T) {
	req := httptest.NewRequest("PUT", "/api/v1/kv/k", nil)
	req.RemoteAddr = "127.0.0.1:5555"
	req.Header.Add("X-Forwarded-For", "10.1.1.1, 10.2.2.2")
	req.Header.Add("X-Forwarded-For", "10.3.3.3, 198.51.100.9")
	if got := clientIP(req, true); got != "198.51.100.9" {
		t.Fatalf("clientIP() = %q, want last hop of last line", got)
	}
}

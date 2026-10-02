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

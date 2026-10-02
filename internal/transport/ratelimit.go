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

// clientIP returns the key used to identify a client for rate limiting.
// Behind a trusted reverse proxy (Caddy), the TCP peer is always the proxy, so the
// real client is the last hop of the last X-Forwarded-For header line, which the proxy
// appends itself; it is used only if it parses as an IP, otherwise RemoteAddr is used.
// IPv4 addresses are returned as-is. IPv6 addresses collapse to their /64 network,
// so a client cannot get fresh buckets by rotating addresses inside its prefix.
func clientIP(r *http.Request, trustProxy bool) string {
	addr := ""
	if trustProxy {
		if lines := r.Header.Values("X-Forwarded-For"); len(lines) > 0 {
			hops := strings.Split(lines[len(lines)-1], ",")
			if hop := strings.TrimSpace(hops[len(hops)-1]); net.ParseIP(hop) != nil {
				addr = hop
			}
		}
	}
	if addr == "" {
		host, _, err := net.SplitHostPort(r.RemoteAddr)
		if err != nil {
			host = r.RemoteAddr
		}
		addr = host
	}
	ip := net.ParseIP(addr)
	if ip == nil {
		return addr
	}
	if ip4 := ip.To4(); ip4 != nil {
		return ip4.String()
	}
	return ip.Mask(net.CIDRMask(64, 128)).String()
}

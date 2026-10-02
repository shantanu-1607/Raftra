package transport

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
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

// Validate rejects flag values that would misbehave at runtime.
func (l Limits) Validate() error {
	if l.MaxKeyBytes < 0 {
		return errors.New("-max-key-bytes must be >= 0")
	}
	if l.MaxValueBytes < 0 {
		return errors.New("-max-value-bytes must be >= 0")
	}
	if l.MaxKeys < 0 {
		return errors.New("-max-keys must be >= 0")
	}
	if l.WriteBurst < 0 {
		return errors.New("-write-burst must be >= 0")
	}
	if math.IsNaN(l.WriteRate) || math.IsInf(l.WriteRate, 0) || l.WriteRate < 0 {
		return errors.New("-write-rate must be a finite number >= 0")
	}
	if l.WriteRate > 0 {
		burst := float64(l.WriteBurst)
		if l.WriteBurst == 0 {
			burst = math.Ceil(l.WriteRate)
		}
		// Keeps the limiter sweep's refill-time Duration conversion from overflowing.
		if burst/l.WriteRate > 86400 {
			return errors.New("-write-burst / -write-rate must refill within 24h (raise -write-rate or lower -write-burst)")
		}
	}
	return nil
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
	if s.limiter != nil && !s.limiter.allow(clientIP(r, s.limits.TrustProxy)) {
		w.Header().Set("Retry-After", "1")
		writeJSONError(w, http.StatusTooManyRequests, "rate limit exceeded: slow down and retry in a second")
		return false
	}
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
		writeJSONError(w, http.StatusBadRequest, "failed to read body")
		return "", false
	}
	return string(data), true
}

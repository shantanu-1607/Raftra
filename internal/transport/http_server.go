package transport

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/shantanu-1607/raftra/internal/kvstore"
	"github.com/shantanu-1607/raftra/internal/metrics"
	"github.com/shantanu-1607/raftra/internal/raft"
)

// HTTPServer provides a RESTful HTTP gateway for Raftra
type HTTPServer struct {
	node          *raft.RaftNode
	server        *http.Server
	logger        *slog.Logger
	peerHTTPAddrs map[string]string // nodeID -> "http://localhost:8001"
	metrics       *metrics.Metrics
	limits        Limits
	limiter       *rateLimiter // nil when write rate limiting is disabled
}

// NewHTTPServer creates an HTTPServer instance
func NewHTTPServer(node *raft.RaftNode, addr string, peerHTTPAddrs map[string]string, logger *slog.Logger, m *metrics.Metrics, limits Limits) *HTTPServer {
	hs := &HTTPServer{
		node:          node,
		logger:        logger,
		peerHTTPAddrs: peerHTTPAddrs,
		metrics:       m,
		limits:        limits,
	}
	if limits.WriteRate > 0 {
		hs.limiter = newRateLimiter(limits.WriteRate, limits.WriteBurst, time.Now)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/status", hs.handleStatus)
	mux.HandleFunc("/api/v1/kv/", hs.handleKV)
	mux.Handle("/metrics", promhttp.Handler())
	hs.server = &http.Server{
		Addr:         addr,
		Handler:      mux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}
	return hs
}

// Start runs the HTTP server in a background goroutine
func (s *HTTPServer) Start() error {
	s.logger.Info("starting HTTP REST gateway", "addr", s.server.Addr)
	go func() {
		if err := s.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			s.logger.Error("HTTP server error", "err", err)
		}
	}()
	return nil
}

// Stop gracefully stops the HTTP server
func (s *HTTPServer) Stop(ctx context.Context) error {
	s.logger.Info("stopping HTTP REST gateway")
	return s.server.Shutdown(ctx)
}

// handleStatus returns the current node's cluster status
func (s *HTTPServer) handleStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	status := map[string]interface{}{
		"role":         s.node.Role().String(),
		"term":         s.node.Term(),
		"is_leader":    s.node.IsLeader(),
		"leader_id":    s.node.LeaderID(),
		"commit_index": s.node.CommitIndex(),
		"last_applied": s.node.LastApplied(),
	}
	w.Header().Set("Content-Type", "application/json")
	if s.limits.CORSOrigin != "" {
		w.Header().Set("Access-Control-Allow-Origin", s.limits.CORSOrigin)
	}
	_ = json.NewEncoder(w).Encode(status)
}

// handleKV handles GET, PUT, POST, DELETE for /api/v1/kv/{key}
func (s *HTTPServer) handleKV(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	opType := strings.ToLower(r.Method)

	defer func() {
		if s.metrics != nil {
			s.metrics.IncKVRequests(opType)
			s.metrics.ObserveKVRequestDuration(opType, time.Since(start))
			s.metrics.SetKVStoreSize(s.node.KVStoreSize())
		}
	}()

	key := strings.TrimPrefix(r.URL.Path, "/api/v1/kv/")
	if key == "" {
		http.Error(w, "missing key in path", http.StatusBadRequest)
		return
	}
	switch r.Method {
	case http.MethodGet:
		s.handleGet(w, r, key)
	case http.MethodPut:
		s.handlePut(w, r, key)
	case http.MethodPost:
		s.handlePost(w, r, key)
	case http.MethodDelete:
		s.handleDelete(w, r, key)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

// leaderRedirectURL builds the redirect target for a request URL. It uses the
// escaped path so keys containing ?, # or % survive the redirect.
func leaderRedirectURL(leaderAddr string, u *url.URL) string {
	target := leaderAddr + u.EscapedPath()
	if u.RawQuery != "" {
		target += "?" + u.RawQuery
	}
	return target
}

// redirectIfFollower checks if node is leader; if not, sends HTTP 307 Temporary Redirect
func (s *HTTPServer) redirectIfFollower(w http.ResponseWriter, r *http.Request) bool {
	if s.node.IsLeader() {
		return false
	}
	leaderID := s.node.LeaderID()
	if leaderAddr, ok := s.peerHTTPAddrs[leaderID]; ok && leaderAddr != "" {
		redirectURL := leaderRedirectURL(leaderAddr, r.URL)
		http.Redirect(w, r, redirectURL, http.StatusTemporaryRedirect)
		return true
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusServiceUnavailable)
	_ = json.NewEncoder(w).Encode(map[string]string{
		"error": "cluster currently has no leader (election in progress)",
	})
	return true
}

// handleGet serves fast reads directly from the committed KVStore
func (s *HTTPServer) handleGet(w http.ResponseWriter, r *http.Request, key string) {
	val, found := s.node.Get(key)
	if !found {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		_ = json.NewEncoder(w).Encode(map[string]string{
			"error": "key not found",
			"key":   key,
		})
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]string{
		"key":   key,
		"value": val,
	})
}

// handlePut (Upsert / SET) proposes a write through Raft consensus
func (s *HTTPServer) handlePut(w http.ResponseWriter, r *http.Request, key string) {
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
	if err != nil {
		http.Error(w, "internal encoding error", http.StatusInternalServerError)
		return
	}
	_, err = s.node.ProposeCommand(encoded)
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"success": true,
		"key":     key,
		"value":   value,
	})
}

// handlePost (Create / SETNX) creates a key only if it does not already exist
func (s *HTTPServer) handlePost(w http.ResponseWriter, r *http.Request, key string) {
	if s.redirectIfFollower(w, r) {
		return
	}
	if !s.enforceWriteLimits(w, r, key, true) {
		return
	}
	// 1. Check if key already exists (Distributed Lock semantics)
	if _, exists := s.node.Get(key); exists {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusConflict) // 409 Conflict
		_ = json.NewEncoder(w).Encode(map[string]string{
			"error": "key already exists",
			"key":   key,
		})
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
	encoded, _ := cmd.Encode()
	_, err := s.node.ProposeCommand(encoded)
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated) // 201 Created
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"success": true,
		"key":     key,
		"value":   value,
	})
}

// handleDelete removes a key through Raft consensus
func (s *HTTPServer) handleDelete(w http.ResponseWriter, r *http.Request, key string) {
	if s.redirectIfFollower(w, r) {
		return
	}
	if !s.enforceWriteLimits(w, r, key, false) {
		return
	}
	cmd := kvstore.Command{
		Type: kvstore.CmdDelete,
		Key:  key,
	}
	encoded, _ := cmd.Encode()
	_, err := s.node.ProposeCommand(encoded)
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"success": true,
		"deleted": key,
	})
}

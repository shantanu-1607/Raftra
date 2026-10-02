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

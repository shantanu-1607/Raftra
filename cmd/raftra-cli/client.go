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

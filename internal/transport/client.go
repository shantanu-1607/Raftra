package transport

import (
	"context"
	"fmt"
	"sync"
	"time"

	pb "github.com/shantanu-1607/raftra/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Transport defines the outbound RPC interface for a Raft node to talk to peers
type Transport interface {
	SendRequestVote(peerID string, req *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error)
	SendAppendEntries(peerID string, req *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error)
	Close() error
}

// GRPCTransport manages persistent gRPC client connections to all cluster peers
type GRPCTransport struct {
	mu      sync.RWMutex
	clients map[string]pb.RaftServiceClient // peerID -> gRPC client stub
	conns   map[string]*grpc.ClientConn     // peerID -> underlying TCP connection
	timeout time.Duration                   // Deadline per RPC (e.g. 100ms)
}

// NewGRPCTransport creates and connects outbound gRPC clients to all peers
func NewGRPCTransport(peers map[string]string, timeout time.Duration) (*GRPCTransport, error) {
	t := &GRPCTransport{
		clients: make(map[string]pb.RaftServiceClient),
		conns:   make(map[string]*grpc.ClientConn),
		timeout: timeout,
	}

	for peerID, addr := range peers {
		// Connect to the peer using insecure credentials (no TLS for local dev/testing)
		conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))

		if err != nil {
			t.Close()
			return nil, fmt.Errorf("failed to connect to peer %s: %w", peerID, addr, err)
		}
		t.conns[peerID] = conn
		t.clients[peerID] = pb.NewRaftServiceClient(conn)
	}

	return t, nil
}

// SendRequestVote sends a RequestVote RPC to a specific peer with a strict timeout
func (t *GRPCTransport) SendRequestVote(peerID string, req *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error) {
	t.mu.RLock()
	client, exists := t.clients[peerID]
	t.mu.RUnlock()
	if !exists {
		return nil, fmt.Errorf("peer %s not found in transport", peerID)
	}
	// Set a deadline so a crashed or slow peer doesn't freeze the election
	ctx, cancel := context.WithTimeout(context.Background(), t.timeout)
	defer cancel()
	return client.RequestVote(ctx, req)
}

// SendAppendEntries sends an AppendEntries (heartbeat/replication) RPC to a specific peer with a strict timeout
func (t *GRPCTransport) SendAppendEntries(peerID string, req *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
	t.mu.RLock()
	client, exists := t.clients[peerID]
	t.mu.RUnlock()

	if !exists {
		return nil, fmt.Errorf("peer %s not found in transport", peerID)
	}
	ctx, cancel := context.WithTimeout(context.Background(), t.timeout)
	defer cancel()
	return client.AppendEntries(ctx, req)
}

// Close cleanly shuts down all peer TCP connections
func (t *GRPCTransport) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, conn := range t.conns {
		_ = conn.Close()
	}
	return nil
}

package raft

import (
	"fmt"
	"io"
	"log/slog"
	"path/filepath"
	"testing"
	"time"

	"github.com/shantanu-1607/raftra/internal/kvstore"
	"github.com/shantanu-1607/raftra/internal/storage"
)

// createBboltTestNode creates a RaftNode backed by a real bbolt database on disk.
// If dbPath is empty, it generates a fresh temporary path.
// If dbPath is provided, it opens the existing database (simulating a reboot!).

func createBboltTestNode(t *testing.T, id string, peers []PeerConfig, dbPath string) (*RaftNode, *storage.BboltStore, *kvstore.KVStore, string) {
	t.Helper()

	if dbPath == "" {
		dbPath = filepath.Join(t.TempDir(), fmt.Sprintf("raft-%s.db", id))
	}

	store, err := storage.NewBboltStore(dbPath)
	if err != nil {
		t.Fatalf("failed to create bbolt store: %v", err)
	}

	kv := kvstore.NewKVStore()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	cfg := DefaultConfig(id, peers)
	cfg.ElectionTimeoutMin = 50 * time.Millisecond
	cfg.ElectionTimeoutMax = 100 * time.Millisecond
	cfg.HeartbeatInterval = 20 * time.Millisecond
	node, err := NewRaftNode(cfg, store, kv, logger)
	if err != nil {
		t.Fatalf("failed to create raft node %s: %v", id, err)
	}
	return node, store, kv, dbPath
}

func TestTermAndVoteSurviveCrash(t *testing.T) {
	node1, store1, _, dbPath := createBboltTestNode(t, "node1", nil, "")

	// 1. Advance term and cast vote

}

package storage

import (
	"path/filepath"
	"testing"
)

// helper to create a temporary bbolt store for testing.
// t.TempDir() automatically cleans up the folder when the test finishes!
func createTestBboltStore(t *testing.T) (*BboltStore, string) {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "test_raft.db")
	store, err := NewBboltStore(dbPath)
	if err != nil {
		t.Fatalf("failed to create test bbolt store: %v", err)

	}
	return store, dbPath

}

func TestBboltStore_Init(t *testing.T) {
	store, _ := createTestBboltStore(t)
	defer store.Close()

	// 1. Initial term should be 0
	term, err := store.LoadTerm()
	if err != nil {
		t.Fatalf("unexpected error loading term: %v", err)
	}
	if term != 0 {
		t.Errorf("expected term 0, got %d", term)
	}

	// 2. Initial votedFor should be empty
	votedFor, err := store.LoadVotedFor()
	if err != nil {
		t.Fatalf("unexpected error loading votedFor: %v", err)
	}
	if votedFor != "" {
		t.Errorf("expected empty votedFor, got %q", votedFor)
	}

	// 3. Initial LastIndex and LastTerm should both be 0 (from sentinel entry)
	lastIdx, err := store.LastIndex()
	if err != nil {
		t.Fatalf("unexpected error loading last index: %v", err)
	}
	if lastIdx != 0 {
		t.Errorf("expected lastIndex 0, got %d", lastIdx)
	}
	lastTerm, err := store.LastTerm()
	if err != nil {
		t.Fatalf("unexpected error loading last term: %v", err)
	}
	if lastTerm != 0 {
		t.Errorf("expected lastTerm 0, got %d", lastTerm)
	}
}

func TestBboltStore_TermAndVote(t *testing.T) {
	store, _ := createTestBboltStore(t)
	defer store.Close()

	// Save and verify term
	if err := store.SaveTerm(5); err != nil {
		t.Fatalf("failed to save term: %v", err)
	}
	term, err := store.LoadTerm()
	if err != nil || term != 5 {
		t.Fatalf("expected term 5, got %d (err: %v)", term, err)
	}

	// Save and verify vote
	if err := store.SaveVotedFor("node-2"); err != nil {
		t.Fatalf("failed to save votedFor: %v", err)
	}
	votedFor, err := store.LoadVotedFor()
	if err != nil || votedFor != "node-2" {
		t.Fatalf("expected votedFor 'node-2', got %q (err: %v)", votedFor, err)
	}

	// Overwrite term and vote (e.g. moving to higher term)
	if err := store.SaveTerm(6); err != nil {
		t.Fatalf("failed to update term: %v", err)
	}
	if err := store.SaveVotedFor(""); err != nil {
		t.Fatalf("failed to clear votedFor: %v", err)
	}

	term, _ = store.LoadTerm()
	votedFor, _ = store.LoadVotedFor()
	if term != 6 || votedFor != "" {
		t.Fatalf("expected term 6 and empty vote, got term %d, vote %q", term, votedFor)
	}
}

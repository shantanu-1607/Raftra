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

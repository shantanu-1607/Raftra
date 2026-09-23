package storage

import (
	"path/filepath"
	"testing"

	pb "github.com/shantanu-1607/raftra/proto"
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

func TestBboltStore_LogEntries(t *testing.T) {
	store, _ := createTestBboltStore(t)
	defer store.Close()

	// Create 3 test log entries
	entries := []*pb.LogEntry{
		{Index: 1, Term: 1, Command: []byte("cmd1")},
		{Index: 2, Term: 1, Command: []byte("cmd2")},
		{Index: 3, Term: 2, Command: []byte("cmd3")},
	}

	// 1. Append entries
	if err := store.AppendEntries(entries); err != nil {
		t.Fatalf("failed to append entries: %v", err)
	}

	// 2. Check LastIndex and LastTerm
	lastIdx, err := store.LastIndex()
	if err != nil || lastIdx != 3 {
		t.Fatalf("expected lastIndex 3, got %d", lastIdx)
	}
	lastTerm, err := store.LastTerm()
	if err != nil || lastTerm != 2 {
		t.Fatalf("expected lastTerm 2, got %d", lastTerm)
	}

	// 3. GetEntry: single retrieval
	entry2, err := store.GetEntry(2)
	if err != nil {
		t.Fatalf("failed to get entry 2: %v", err)
	}
	if entry2.Term != 1 || string(entry2.Command) != "cmd2" {
		t.Fatalf("unexpected entry 2 content: %+v", entry2)
	}

	// Non-existent entry should return an error
	if _, err := store.GetEntry(99); err == nil {
		t.Fatalf("expected error for non-existent entry 99, got nil")
	}

	// 4. GetEntriesFrom: range scan from index 2
	rangeEntries, err := store.GetEntriesFrom(2)
	if err != nil {
		t.Fatalf("failed to get entries from index 2: %v", err)
	}
	if len(rangeEntries) != 2 {
		t.Fatalf("expected 2 entries (index 2 and 3), got %d", len(rangeEntries))
	}
	if rangeEntries[0].Index != 2 || rangeEntries[1].Index != 3 {
		t.Fatalf("unexpected entries in range scan: %+v", rangeEntries)
	}

	// 5. LoadAllEntries: should return sentinel (index 0) + 3 entries = 4 total
	all, err := store.LoadAllEntries()
	if err != nil {
		t.Fatalf("failed to load all entries: %v", err)
	}
	if len(all) != 4 {
		t.Fatalf("expected 4 entries including sentinel, got %d", len(all))
	}
	if all[0].Index != 0 || all[1].Index != 1 || all[2].Index != 2 || all[3].Index != 3 {
		t.Fatalf("unexpected ordering in LoadAllEntries")
	}
}

func TestBboltStore_TruncateFrom(t *testing.T) {
	store, _ := createTestBboltStore(t)
	defer store.Close()

	// Append entries 1, 2, 3, 4, 5
	entries := []*pb.LogEntry{
		{Index: 1, Term: 1},
		{Index: 2, Term: 1},
		{Index: 3, Term: 1},
		{Index: 4, Term: 1},
		{Index: 5, Term: 1},
	}
	_ = store.AppendEntries(entries)

	// Truncate from index 4 onwards (should delete 4 and 5)
	if err := store.TruncateFrom(4); err != nil {
		t.Fatalf("failed to truncate from index 4: %v", err)
	}

	// LastIndex should now be 3
	lastIdx, _ := store.LastIndex()
	if lastIdx != 3 {
		t.Fatalf("expected lastIndex 3 after truncation, got %d", lastIdx)
	}

	// Entry 3 must still exist
	if _, err := store.GetEntry(3); err != nil {
		t.Fatalf("expected entry 3 to exist, got err: %v", err)
	}

	// Entries 4 and 5 must be gone
	if _, err := store.GetEntry(4); err == nil {
		t.Fatalf("expected entry 4 to be deleted, but it was found")
	}
	if _, err := store.GetEntry(5); err == nil {
		t.Fatalf("expected entry 5 to be deleted, but it was found")
	}
}

func TestBboltStore_DurabilityAfterClose(t *testing.T) {
	store, dbPath := createTestBboltStore(t)

	// 1. Write state to disk
	_ = store.SaveTerm(10)
	_ = store.SaveVotedFor("node-3")
	_ = store.AppendEntries([]*pb.LogEntry{
		{Index: 1, Term: 10, Command: []byte("durable_cmd")},
	})

	// 2. SIMULATE CRASH: Close the database completely!
	if err := store.Close(); err != nil {
		t.Fatalf("failed to close store: %v", err)
	}

	// 3. SIMULATE REBOOT: Open the exact same file path again!
	reopenedStore, err := NewBboltStore(dbPath)
	if err != nil {
		t.Fatalf("failed to reopen bbolt store: %v", err)
	}
	defer reopenedStore.Close()

	// 4. Verify term survived
	term, err := reopenedStore.LoadTerm()
	if err != nil || term != 10 {
		t.Fatalf("expected term 10 after restart, got %d (err: %v)", term, err)
	}

	// 5. Verify vote survived
	vote, err := reopenedStore.LoadVotedFor()
	if err != nil || vote != "node-3" {
		t.Fatalf("expected vote 'node-3' after restart, got %q (err: %v)", vote, err)
	}

	// 6. Verify log entry survived
	entry, err := reopenedStore.GetEntry(1)
	if err != nil {
		t.Fatalf("failed to read entry 1 after restart: %v", err)
	}
	if entry.Term != 10 || string(entry.Command) != "durable_cmd" {
		t.Fatalf("corrupted entry after restart: %+v", entry)
	}
}

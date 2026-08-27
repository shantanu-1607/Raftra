package storage

import (
	"fmt"
	"sync"

	pb "github.com/shantanu-1607/raftra/proto"
)

// MemoryStore is an in-memory implementation of StorageBackend (used for unit tests)
type MemoryStore struct {
	mu       sync.RWMutex
	term     uint64
	votedFor string
	entries  []*pb.LogEntry //resizble list of the log entries
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		entries: []*pb.LogEntry{{Index: 0, Term: 0}},
	}
}

// SaveTerm stores the current term in memory
func (m *MemoryStore) SaveTerm(term uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.term = term
	return nil
}

// LoadTerm retrieves the stored term
func (m *MemoryStore) LoadTerm() (uint64, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.term, nil
}

// SaveVotedFor stores the candidate ID voted for in the current term
func (m *MemoryStore) SaveVotedFor(candidateID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.votedFor = candidateID
	return nil
}

// LoadVotedFor retrieves the candidate ID voted for
func (m *MemoryStore) LoadVotedFor() (string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.votedFor, nil
}

// AppendEntries adds new log entries to the end of the log
func (m *MemoryStore) AppendEntries(entries []*pb.LogEntry) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.entries = append(m.entries, entries...)
	return nil
}

// GetEntry retrieves a single log entry by its index
func (m *MemoryStore) GetEntry(index uint64) (*pb.LogEntry, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if index >= uint64(len(m.entries)) {
		return nil, fmt.Errorf("entry index %d out of bounds", index)
	}
	return m.entries[index], nil
}

func (m *MemoryStore) GetEntriesFrom(startIndex uint64) ([]*pb.LogEntry, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if startIndex >= uint64(len(m.entries)) {
		return nil, nil
	}
	// Return a copy of the slice so external modifications don't cause race conditions
	return append([]*pb.LogEntry{}, m.entries[startIndex:]...), nil
}

// TruncateFrom removes all log entries from index to the end (used during log conflict resolution)
func (m *MemoryStore) TruncateFrom(index uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if index < uint64(len(m.entries)) {
		m.entries = m.entries[:index]
	}
	return nil
}

// LastIndex returns the index of the very latest log entry
func (m *MemoryStore) LastIndex() (uint64, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return uint64(len(m.entries) - 1), nil
}

// LastTerm returns the term of the very latest log entry
func (m *MemoryStore) LastTerm() (uint64, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if len(m.entries) == 0 {
		return 0, nil
	}
	return m.entries[len(m.entries)-1].Term, nil
}

// LoadAllEntries returns a copy of all log entries
func (m *MemoryStore) LoadAllEntries() ([]*pb.LogEntry, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return append([]*pb.LogEntry{}, m.entries...), nil
}

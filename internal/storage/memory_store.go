package storage

import (
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

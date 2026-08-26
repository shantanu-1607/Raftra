package kvstore

import (
	"sync"
)

// KVStore is the replicated state machine: an in-memory key-value dictionary
type KVStore struct {
	mu   sync.RWMutex
	data map[string]string
}

// NewKVStore creates a new KVStore
func NewKVStore() *KVStore {
	return &KVStore{
		data: make(map[string]string),
	}
}

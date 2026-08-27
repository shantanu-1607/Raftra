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

// Apply executes a committed command against the state machine
func (kv *KVStore) Apply(cmd Command) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	switch cmd.Type {
	case CmdSet:
		kv.data[cmd.Key] = cmd.Value
	case CmdDelete:
		delete(kv.data, cmd.Key)
	}
}

// Get retrieves a value by key (concurrent safe read)
func (kv *KVStore) Get(key string) (string, bool) {
	kv.mu.RLock()
	defer kv.mu.RUnlock()

	val, exists := kv.data[key]
	return val, exists
}

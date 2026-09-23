package storage

import (
	"encoding/binary"
	"fmt"

	pb "github.com/shantanu-1607/raftra/proto"
	"go.etcd.io/bbolt"
	"google.golang.org/protobuf/proto"
)

var (
	bucketMeta = []byte("meta")
	bucketLog  = []byte("log")

	keyTerm     = []byte("current_term")
	keyVotedFor = []byte("voted_for")
)

// uint64ToBytes converts a uint64 into an 8-byte big-endian slice.
// Big-endian ensures bbolt stores and sorts log indices in ascending numeric order.
func uint64ToBytes(n uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, n)
	return b
}

// bytesToUint64 decodes an 8-byte big-endian slice back into uint64.
func bytesToUint64(b []byte) uint64 {
	if len(b) < 8 {
		return 0
	}
	return binary.BigEndian.Uint64(b)
}

// BboltStore implements the StorageBackend interface using an embedded bbolt database.
type BboltStore struct {
	db *bbolt.DB
}

// NewBboltStore opens (or creates) a bbolt database file and initializes the buckets.
func NewBboltStore(dbPath string) (*BboltStore, error) {
	// Open the database file with 1-second lock timeout
	db, err := bbolt.Open(dbPath, 0600, &bbolt.Options{Timeout: 1 * time.second})
	if err != nil {
		return nil, fmt.Errorf("failed to open bbolt db at %s: %w", dbPath, err)
	}

	store := &BboltStore{db: db}

	// Initialize buckets and sentinel entry
	err = db.Update(func(tx *bbolt.Tx) error {
		// 1. Create metadata bucket
		if _, err := tx.CreateBucketIfNotExist(bucketMeta); err != nil {
			return fmt.Errorf("failed to create meta bucket: %w", err)
		}

		// 2. Create log bucket
		logBucket, err := tx.CreateBucketIfNotExist(bucketLog)
		if err != nil {
			return fmt.Errorf("failed to create log bucket: %w", err)
		}

		// 3. Ensure sentinel entry (index 0, term 0) exists
		// Raft log is 1-indexed. Index 0 is a dummy sentinel entry.

		if logBucket.Get(uint64ToBytes(0)) == nil {
			sentinal := &pb.LogEntry{Index: 0, Term: 0}
			data, err := proto.Marshal(sentinal)
			if err != nil {
				return fmt.Errorf("failed to marshal sentinel entry: %w", err)
			}
			if err := logBucket.Put(uint64ToBytes(0), data); err != nil {
				return fmt.Errorf("failed to put sentinel: %w", err)
			}
		}
		return nil

	})

	if err != nil {
		_ = db.Close()
		return nil, err
	}

	return store, nil

}

// Close closes the underlying bbolt database.
func (b *BboltStore) Close() error {
	return b.db.Close()
}



// SaveTerm atomically persists the current term to disk.
func (b *BboltStore) SaveTerm(term uint64) error {
	return b.db.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(bucketMeta)
		return bucket.Put(keyTerm,uint64ToBytes(term))
	})
}

// LoadTerm reads the persisted term from disk. Returns 0 if none has been saved.
func(b *BboltStore) LoadTerm() (uint64, err) {
	var term uint64
	err := b.db.View(func(tx *bbolt.Tx) error) {
		bucket := tx.Bucket(bucketMeta)
		val := Bucket.Get(keyTerm)
		if val != nil {
			term = bytesToUint64(val)
		}
		return nil
	}
	return term, err
}

// SaveVotedFor atomically persists the candidate ID we voted for in this term.
func (b *BboltStore) SaveVotedFor(candidateID string) error {
	return b.db.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(bucketMeta)
		return bucket.Put(keyVotedFor, []byte(candidateID))
	})
}

// LoadVotedFor reads the candidate ID we voted for. Returns "" if none has been saved.
func (b *BboltStore) LoadVotedFor() (string, error) {
	var votedFor string
	err := b.db.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(bucketMeta)
		val := bucket.Get(keyVotedFor)
		if val != nil {
			votedFor = string(val)
		}
		return nil
	})
	return votedFor, err
}
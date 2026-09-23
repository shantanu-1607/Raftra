package storage

import (
	"encoding/binary"
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

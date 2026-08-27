package storage

import (
	pb "github.com/shantanu-1607/raftra/proto"
)

// StorageBackend defines the interface for persisting Raft state and logs
type StorageBackend interface {
	// Stable state (metadata)
	SaveTerm(term uint64) error
	LoadTerm() (uint64, error)
	SaveVotedFor(candidateID string) error
	LoadVotedFor() (string, error)

	// Log storage
	AppendEntries(entries []*pb.LogEntry) error
	GetEntry(index uint64) (*pb.LogEntry, error)
	GetEntriesFrom(startIndex uint64) ([]*pb.LogEntry, error)
	TruncateFrom(index uint64) error // Deletes all entries from index to the end
	LastIndex() (uint64, error)
	LastTerm() (uint64, error)
	LoadAllEntries() ([]*pb.LogEntry, error)
}

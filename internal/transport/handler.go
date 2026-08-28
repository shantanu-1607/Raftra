package transport

import (
	"github.com/shantanu-1607/raftra/internal/raft"
	pb "github.com/shantanu-1607/raftra/proto"
)

// Handler implements both pb.RaftServiceServer and pb.KVServiceServer
type Handler struct {
	pb.UnimplementedRaftServiceServer
	pb.UnimplementedKVServiceServer

	node *raft.RaftNode
}

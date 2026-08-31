package transport

import (
	"context"

	"github.com/shantanu-1607/raftra/internal/raft"
	pb "github.com/shantanu-1607/raftra/proto"
)

// Handler implements both pb.RaftServiceServer and pb.KVServiceServer
type Handler struct {
	pb.UnimplementedRaftServiceServer
	pb.UnimplementedKVServiceServer

	node *raft.RaftNode
}

// NewHandler creates a new gRPC Handler attached to a RaftNode
func NewHandler(node *raft.RaftNode) *Handler {
	return &Handler{
		node: node,
	}
}

// ==========================================
// 1. RaftService RPC Handlers (Node-to-Node)
// ==========================================

// RequestVote is called by candidates asking for votes during an election
func (h *Handler) RequestVote(ctx context.Context, req *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error) {
	// In Phase 1, we return our current term. Full election logic is wired in Phase 2.
	return &pb.RequestVoteResponse{
		Term:        h.node.Term(),
		VoteGranted: false,
	}, nil
}

// AppendEntries is called by the leader for heartbeats and log replication
func (h *Handler) AppendEntries(ctx context.Context, req *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
	//in phase1 we will only return our current term, rest of the logic will be implemented baad me
	return &pb.AppendEntriesResponse{
		Term:    h.node.Term(),
		Success: false,
	}, nil
}

// ==========================================
// 2. KVService RPC Handlers (Client-Facing)
// ==========================================

// Set handles client write requests
func (h *Handler) Set(ctx context.Context, req *pb.SetRequest) (*pb.SetResponse, error) {
	return &pb.SetResponse{
		Success:    false,
		Error:      "cluster starting up", //leader election wala logic will be done baad me
		LeaderHint: "",
	}, nil
}

// Get handles client read requests
func (h *Handler) Get(ctx context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	return &pb.GetResponse{
		Value: "",
		Found: false,
		Error: "cluster starting up (leader election begins in Phase 2)",
	}, nil
}

// Delete handles client delete requests
func (h *Handler) Delete(ctx context.Context, req *pb.DeleteRequest) (*pb.DeleteResponse, error) {
	return &pb.DeleteResponse{
		Success:    false,
		Error:      "cluster starting up (leader election begins in Phase 2)",
		LeaderHint: "",
	}, nil
}

//Timeout / Cancellation Tracker: If the caller disconnects, loses internet, or set a 100ms deadline, ctx allows Go to instantly cancel this request so we don't waste CPU.

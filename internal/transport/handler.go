package transport

import (
	"context"

	"github.com/shantanu-1607/raftra/internal/kvstore"
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
	resp := h.node.HandleRequestVote(req)
	return resp, nil
}

// AppendEntries is called by the leader for heartbeats and log replication
func (h *Handler) AppendEntries(ctx context.Context, req *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
	resp := h.node.HandleAppendEntries(req)
	return resp, nil
}

// ==========================================
// 2. KVService RPC Handlers (Client-Facing)
// ==========================================

// Set handles client write requests
func (h *Handler) Set(ctx context.Context, req *pb.SetRequest) (*pb.SetResponse, error) {
	if !h.node.IsLeader() {
		return &pb.SetResponse{
			Success:    false,
			Error:      "node is not the leader",
			LeaderHint: h.node.LeaderID(),
		}, nil
	}

	// 2. Encode the SET command
	cmd := kvstore.Command{
		Type:  kvstore.CmdSet,
		Key:   req.Key,
		Value: req.Value,
	}
	encoded, err := cmd.Encode()
	if err != nil {
		return &pb.SetResponse{
			Success: false,
			Error:   err.Error(),
		}, nil
	}

	// 3. Propose command to Raft log and wait for majority commit
	_, err = h.node.ProposeCommand(encoded)
	if err != nil {
		return &pb.SetResponse{
			Success: false,
			Error:   err.Error(),
		}, nil
	}

	return &pb.SetResponse{
		Success: true,
		Error:   "",
	}, nil
}

// Get handles client read requests (served from leader's committed state)
func (h *Handler) Get(ctx context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	//reads are served from the leader
	if !h.node.IsLeader() {
		return &pb.GetResponse{
			Value: "",
			Found: false,
			Error: "not leader",
		}, nil
	}

	val, found := h.node.Get(req.Key)
	return &pb.GetResponse{
		Value: val,
		Found: found,
		Error: "",
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

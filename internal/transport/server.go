package transpot

import (
	"fmt"
	"log/slog"
	"net"

	"github.com/shantanu-1607/raftra/internal/raft"
	pb "github.com/shantanu-1607/raftra/proto"
	"google.golang.org/grpc"
)

// Server wraps the gRPC server and network listener
type Server struct {
	addr       string
	grpcServer *grpc.Server
	listener   net.Listener
	logger     *slog.Logger
}

// NewServer creates a new gRPC Server instance attached to a RaftNode
func NewServer(addr string, node *raft.RaftNode, logger *slog.Logger) (*Server, error) {
	//1. open a TCP port listner
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("failed to listen on %s: %w", addr, err)
	}
	// 2. Create the gRPC server instance
	grpcServer := grpc.NewServer()

	// 3. Create and register our handler
	handler := NewHandler(node)
	pb.RegisterRaftServiceServer(grpcServer, handler)
	pb.RegisterKVServiceServer(grpcServer, handler)

	return &Server{
		addr:       addr,
		grpcServer: grpcServer,
		listener:   lis,
		logger:     logger.With("addr", addr),
	}, nil
}

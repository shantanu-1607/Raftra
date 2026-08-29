package transport

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
	// 1. Open a TCP port listener
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

// Start begins accepting incoming gRPC connections in a background goroutine
func (s *Server) Start() {
	go func() {
		s.logger.Info("gRPC server listening", "addr", s.addr)
		if err := s.grpcServer.Serve(s.listener); err != nil {
			s.logger.Error("gRPC server stopped with error", "err", err)
		}
	}()
}

// Stop gracefully shuts down the gRPC server and closes the port listener
func (s *Server) Stop() {
	s.logger.Info("shutting down gRPC server", "addr", s.addr)
	s.grpcServer.GracefulStop()
}

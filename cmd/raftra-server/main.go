package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/shantanu-1607/raftra/internal/kvstore"
	"github.com/shantanu-1607/raftra/internal/raft"
	"github.com/shantanu-1607/raftra/internal/storage"
	"github.com/shantanu-1607/raftra/internal/transport"
)

func main() {
	// 1. Define command line flags
	nodeID := flag.String("id", "node1", "Unique node ID")
	port := flag.Int("port", 50051, "gRPC port to listen on")
	httpPort := flag.Int("http-port", 8001, "HTTP REST gateway port to listen on")
	peerFlag := flag.String("peers", "", "comma-separated list of peer ID:address (e.g. node2:localhost:50052,node3:localhost:50053)")
	httpPeersFlag := flag.String("http-peers", "", "comma-separated list of peer ID:http-address (e.g. node1:http://localhost:8001,node2:http://localhost:8002)")
	dataDir := flag.String("data-dir", "data", "Directory to store Raft persistent state and logs")
	flag.Parse()

	// 2. Setup structured logging
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	logger.Info("starting raftra node",
		"id", *nodeID,
		"grpc_port", *port,
		"http_port", *httpPort,
	)

	// 3. Parse gRPC peer list string
	var peers []raft.PeerConfig
	peerAddressMap := make(map[string]string)

	if *peerFlag != "" {
		peerEntries := strings.Split(*peerFlag, ",")
		for _, entry := range peerEntries {
			parts := strings.Split(entry, ":")
			if len(parts) >= 2 {
				id := parts[0]
				addr := strings.Join(parts[1:], ":")
				peers = append(peers, raft.PeerConfig{
					ID:      id,
					Address: addr,
				})
				peerAddressMap[id] = addr
			}
		}
	}

	// 4. Parse HTTP peer list string (for 307 redirects)
	peerHTTPMap := make(map[string]string)
	if *httpPeersFlag != "" {
		httpEntries := strings.Split(*httpPeersFlag, ",")
		for _, entry := range httpEntries {
			parts := strings.SplitN(entry, ":", 2)
			if len(parts) == 2 {
				peerHTTPMap[parts[0]] = parts[1]
			}
		}
	}

	// 5. Initialize durable bbolt storage and KV state machine
	if err := os.MkdirAll(*dataDir, 0755); err != nil {
		logger.Error("failed to create data directory", "error", err, "path", *dataDir)
		os.Exit(1)

	}

	dbPath := filepath.Join(*dataDir, fmt.Sprintf("%s.db", *nodeID))
	store, err := storage.NewBboltStore(dbPath)
	if err != nil {
		logger.Error("failed to create bbolt store", "error", err, "path", dbPath)
		os.Exit(1)
	}
	kv := kvstore.NewKVStore()

	// 6. Initialize Raft configuration & node
	config := raft.DefaultConfig(*nodeID, peers)
	raftNode, err := raft.NewRaftNode(config, store, kv, logger)
	if err != nil {
		logger.Error("failed to create raft node", "error", err)
		os.Exit(1)
	}

	// 7. Initialize outbound gRPC transport to peers
	trans, err := transport.NewGRPCTransport(peerAddressMap, 100*time.Millisecond)
	if err != nil {
		logger.Error("failed to create outbound transport", "error", err)
		os.Exit(1)
	}
	raftNode.SetTransport(trans)

	// 8. Start the inbound gRPC network server
	serverAddr := fmt.Sprintf("localhost:%d", *port)
	server, err := transport.NewServer(serverAddr, raftNode, logger)
	if err != nil {
		logger.Error("failed to create gRPC server", "error", err)
		os.Exit(1)
	}
	server.Start()

	// 9. Start the HTTP REST gateway
	httpServerAddr := fmt.Sprintf("localhost:%d", *httpPort)
	httpServer := transport.NewHTTPServer(raftNode, httpServerAddr, peerHTTPMap, logger)
	if err := httpServer.Start(); err != nil {
		logger.Error("failed to start HTTP server", "error", err)
		os.Exit(1)
	}

	// 10. Start the Raft consensus engine event loop!
	raftNode.Start()
	logger.Info("raft node running", "id", *nodeID, "role", raftNode.Role().String())

	// 11. Wait for OS termination signal (Ctrl+C / SIGTERM)
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	<-sigCh

	logger.Info("shutting down node", "id", *nodeID)
	raftNode.Stop()
	server.Stop()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = httpServer.Stop(shutdownCtx)

	_ = trans.Close()
	logger.Info("node stopped gracefully", "id", *nodeID)
}

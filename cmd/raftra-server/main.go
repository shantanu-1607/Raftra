package main

import (
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
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
	port := flag.Int("port", 50051, "port to listen on")
	peerFlag := flag.String("peers", "", "comma-separated list of peer ID:address (e.g. node2:localhost:50052,node3:localhost:50053)")
	flag.Parse()

	// 2. Setup structured logging
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	logger.Info("starting raftra node", "id", *nodeID, "port", *port)

	// 3. Parse peer list string into Go structs and map
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

	// 4. Initialize storage and KV state machine
	store := storage.NewMemoryStore()
	kv := kvstore.NewKVStore()

	// 5. Initialize Raft configuration & node
	config := raft.DefaultConfig(*nodeID, peers)
	raftNode, err := raft.NewRaftNode(config, store, kv, logger)
	if err != nil {
		logger.Error("failed to create raft node", "error", err)
		os.Exit(1)
	}

	// 6. Initialize outbound gRPC transport to peers
	trans, err := transport.NewGRPCTransport(peerAddressMap, 100*time.Millisecond)
	if err != nil {
		logger.Error("failed to create outbound transport", "error", err)
		os.Exit(1)
	}
	raftNode.SetTransport(trans)

	// 7. Start the inbound gRPC network server
	serverAddr := fmt.Sprintf("localhost:%d", *port)
	server, err := transport.NewServer(serverAddr, raftNode, logger)
	if err != nil {
		logger.Error("failed to create gRPC server", "error", err)
		os.Exit(1)
	}

	server.Start()

	// 8. Start the Raft consensus engine event loop!
	raftNode.Start()
	logger.Info("raft node started and running as follower", "id", *nodeID, "role", raftNode.Role().String())

	// 9. Wait for OS termination signal (Ctrl+C / SIGTERM)
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	<-sigCh

	logger.Info("shutting down node", "id", *nodeID)
	raftNode.Stop()
	server.Stop()
	_ = trans.Close()
	logger.Info("node stopped gracefully", "id", *nodeID)
}

package main

import (
	"flag"
	"log/slog"
	"os"
	"strings"

	"github.com/shantanu-1607/raftra/internal/kvstore"
	"github.com/shantanu-1607/raftra/internal/raft"
	"github.com/shantanu-1607/raftra/internal/storage"
)

func main() {
	//defining command line flags
	nodeID := flag.String("id", "node1", "Unique node ID")
	port := flag.Int("port", 50051, "port to listen on")
	peerFlag := flag.String("peers", "", "comma-separated list of peer ID:address (e.g. node2:localhost:50052,node3:localhost:50053)")
	flag.Parse()

	// 2. Setup structured logging
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	logger.Info("starting raftra node", "id", *nodeID, "port", *port)

	// 3. Parse peer list string into Go structs
	var peers []raft.PeerConfig
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

}

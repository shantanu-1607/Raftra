.PHONY: proto build test bench bench-failover clean docker-build docker-up docker-down

# Generate Go code from .proto files
proto:
	protoc --go_out=. --go_opt=paths=source_relative \
	       --go-grpc_out=. --go-grpc_opt=paths=source_relative \
	       proto/raft.proto

# Build all binaries
build:
	go build -o bin/raftra-server ./cmd/raftra-server
	go build -o bin/raftra-cli ./cmd/raftra-cli
	go build -o bin/raftra-loadgen ./benchmark/loadgen

# Run unit tests with Go's race detector enabled
test:
	go test -v -race ./...

# Run Go microbenchmarks
bench:
	go test -v -bench=. -benchmem -run=^$$ ./benchmark/...

# Run the 10-trial failover measurement benchmark
bench-failover:
	go test -v -run=TestFailoverTimeMeasurement ./benchmark/...

clean:
	rm -rf bin/

# Docker shortcuts
docker-build:
	docker build -f deployments/Dockerfile -t raftra:latest .

docker-up:
	docker compose -f deployments/docker-compose.yml up --build -d

docker-down:
	docker compose -f deployments/docker-compose.yml down -v


.PHONY: proto build test clean docker-build docker-up docker-down

# Generate Go code from .proto files
proto:
	protoc --go_out=. --go_opt=paths=source_relative \
	       --go-grpc_out=. --go-grpc_opt=paths=source_relative \
	       proto/raft.proto

# Build all binaries
build:
	go build -o bin/raftra-server ./cmd/raftra-server

# Run unit tests with Go's race detector enabled
test:
	go test -v -race ./...

clean:
	rm -rf bin/

# Docker shortcuts
docker-build:
	docker build -f deployments/Dockerfile -t raftra:latest .

docker-up:
	docker compose -f deployments/docker-compose.yml up --build -d

docker-down:
	docker compose -f deployments/docker-compose.yml down -v


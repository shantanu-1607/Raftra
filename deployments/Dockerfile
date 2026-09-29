# Stage 1: Build
FROM golang:alpine AS builder
WORKDIR /app

# Copy dependency specifications first for Docker layer caching
COPY go.mod go.sum ./
RUN go mod download

# Copy the source code
COPY . .

# Compile raftra-server as a static, stripped binary
RUN CGO_ENABLED=0 go build -ldflags="-s -w" -o /raftra-server ./cmd/raftra-server

# Stage 2: Runtime
FROM alpine:latest
RUN apk --no-cache add ca-certificates

# Copy the compiled binary from the builder stage
COPY --from=builder /raftra-server /usr/local/bin/raftra-server

# Expose gRPC port (50051) and HTTP gateway port (8001)
EXPOSE 50051 8001

# Entrypoint for running the server
ENTRYPOINT ["raftra-server"]

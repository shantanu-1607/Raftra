# Stage 1: Build
FROM golang:alpine AS builder
WORKDIR /app

# Copy dependency specifications first for Docker layer caching
COPY go.mod go.sum ./
RUN go mod download

# Copy the source code
COPY . .

# Compile raftra-server and raftra-cli as static, stripped binaries
RUN CGO_ENABLED=0 go build -ldflags="-s -w" -o /raftra-server ./cmd/raftra-server && \
    CGO_ENABLED=0 go build -ldflags="-s -w" -o /raftra-cli ./cmd/raftra-cli

# Stage 2: Runtime
FROM alpine:latest
RUN apk --no-cache add ca-certificates

# Copy the compiled binaries from the builder stage
COPY --from=builder /raftra-server /usr/local/bin/raftra-server
COPY --from=builder /raftra-cli /usr/local/bin/raftra-cli

# Expose gRPC port (50051) and HTTP gateway port (8001)
EXPOSE 50051 8001

# Entrypoint for running the server
ENTRYPOINT ["raftra-server"]

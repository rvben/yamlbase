# Build stage
FROM rust:1.88-alpine AS builder

# Install build dependencies
RUN apk add --no-cache musl-dev

# Create app directory
WORKDIR /app

# Copy manifests
COPY Cargo.toml Cargo.lock ./

# Copy source code
COPY src ./src
COPY benches ./benches

# Build the application
RUN cargo build --release

# Runtime stage
FROM alpine:3.22

# Link container to GitHub repository
LABEL org.opencontainers.image.source=https://github.com/rvben/yamlbase
LABEL org.opencontainers.image.description="YamlBase - A functional SQL server that serves YAML-defined tables over PostgreSQL wire protocol"
LABEL org.opencontainers.image.licenses=MIT

# Install runtime dependencies
RUN apk add --no-cache ca-certificates

# Create non-root user
RUN addgroup -g 1000 -S appuser && \
    adduser -u 1000 -S appuser -G appuser

# Copy the binary from builder
COPY --from=builder /app/target/release/yamlbase /usr/local/bin/yamlbase

# Create data directory
RUN mkdir -p /data && chown appuser:appuser /data

# Switch to non-root user
USER appuser

# Expose default ports for postgres, mysql, and teradata protocols
EXPOSE 5432 3306 1025

# Set working directory
WORKDIR /data

# Run the binary
ENTRYPOINT ["yamlbase"]

# Default arguments
CMD ["-f", "/data/database.yaml"]
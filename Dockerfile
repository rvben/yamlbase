# Build stage
FROM rust:1.88 AS builder

# Create app directory
WORKDIR /app

# Copy manifests
COPY Cargo.toml Cargo.lock ./

# Copy source code
COPY src ./src

# Build the application
RUN cargo build --release

# Runtime stage
FROM alpine:3.22

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

# Expose default ports for postgres and mysql protocols
EXPOSE 5432 3306

# Set working directory
WORKDIR /data

# Run the binary
ENTRYPOINT ["yamlbase"]

# Default arguments
CMD ["-f", "/data/database.yaml"]
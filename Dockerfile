# Build stage
FROM rust:1.87-alpine AS builder

# Install build dependencies
RUN apk add --no-cache musl-dev

WORKDIR /app

# Copy manifests
COPY Cargo.toml Cargo.lock ./

# Copy source code
COPY src ./src
COPY benches ./benches

# Build release binary
RUN cargo build --release --target x86_64-unknown-linux-musl

# Runtime stage
FROM scratch

# Copy the binary
COPY --from=builder /app/target/x86_64-unknown-linux-musl/release/yamlbase /yamlbase

# Set the entrypoint
ENTRYPOINT ["/yamlbase"]

# Default arguments
CMD ["-f", "/data/database.yaml"]
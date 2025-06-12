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

# Detect target platform and build accordingly
ARG TARGETPLATFORM
RUN case "$TARGETPLATFORM" in \
        "linux/amd64") RUST_TARGET="x86_64-unknown-linux-musl" ;; \
        "linux/arm64") RUST_TARGET="aarch64-unknown-linux-musl" ;; \
        *) echo "Unsupported platform: $TARGETPLATFORM" && exit 1 ;; \
    esac && \
    rustup target add $RUST_TARGET && \
    cargo build --release --target $RUST_TARGET && \
    cp target/$RUST_TARGET/release/yamlbase /yamlbase

# Runtime stage
FROM scratch

# Copy the binary
COPY --from=builder /yamlbase /yamlbase

# Set the entrypoint
ENTRYPOINT ["/yamlbase"]

# Default arguments
CMD ["-f", "/data/database.yaml"]
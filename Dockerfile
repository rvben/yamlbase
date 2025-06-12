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

# Set the target based on the build platform
ARG TARGETPLATFORM
RUN case "$TARGETPLATFORM" in \
        "linux/amd64") echo "x86_64-unknown-linux-musl" > /tmp/target.txt ;; \
        "linux/arm64") echo "aarch64-unknown-linux-musl" > /tmp/target.txt ;; \
        *) echo "Unsupported platform: $TARGETPLATFORM" && exit 1 ;; \
    esac

# Add the rust target
RUN rustup target add $(cat /tmp/target.txt)

# Build release binary
RUN cargo build --release --target $(cat /tmp/target.txt)

# Move the binary to a consistent location
RUN cp /app/target/$(cat /tmp/target.txt)/release/yamlbase /app/yamlbase

# Runtime stage
FROM scratch

# Copy the binary
COPY --from=builder /app/yamlbase /yamlbase

# Set the entrypoint
ENTRYPOINT ["/yamlbase"]

# Default arguments
CMD ["-f", "/data/database.yaml"]
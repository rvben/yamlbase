.PHONY: all build test bench clean run docker-build docker-run help

# Default target
all: build

# Build the project in release mode
build:
	cargo build --release

# Build for a specific target
build-target:
	@if [ -z "$(TARGET)" ]; then echo "Usage: make build-target TARGET=x86_64-unknown-linux-gnu"; exit 1; fi
	@echo "Building for target: $(TARGET)"
	cargo build --release --target $(TARGET)

# Build all release targets
build-all-targets:
	@echo "Building for all targets..."
	@$(MAKE) build-target TARGET=x86_64-unknown-linux-gnu
	@$(MAKE) build-target TARGET=x86_64-unknown-linux-musl
	@$(MAKE) build-target TARGET=x86_64-pc-windows-msvc || echo "Skipping Windows build on non-Windows host"
	@$(MAKE) build-target TARGET=x86_64-apple-darwin || echo "Skipping macOS x64 build on non-macOS host"
	@$(MAKE) build-target TARGET=aarch64-apple-darwin || echo "Skipping macOS ARM build on non-macOS host"

# Run all tests
test:
	cargo test --all-features --verbose

# Run unit tests only
test-unit:
	cargo test --lib --all-features --verbose

# Run integration tests only
test-integration:
	cargo test --test '*' --all-features --verbose

# Run tests with no default features
test-no-features:
	cargo test --no-default-features --verbose

# Run tests with coverage
coverage:
	cargo llvm-cov --all-features --workspace --lcov --output-path lcov.info

# Run tests with coverage and generate HTML report
coverage-html:
	cargo llvm-cov --all-features --workspace --html

# Open coverage report
coverage-open: coverage-html
	open target/llvm-cov/html/index.html || xdg-open target/llvm-cov/html/index.html

# Run benchmarks
bench:
	cargo bench --all-features

# Clean build artifacts
clean:
	cargo clean
	rm -rf target/
	rm -rf docker-context/

# Run the server locally
run:
	cargo run -- -f examples/sample_database.yaml --verbose

# Build Docker image (local, current platform only)
docker-build:
	docker build -t yamlbase:latest .

# Setup Docker buildx for multi-platform builds
docker-setup:
	@if ! docker buildx ls | grep -q yamlbase-builder; then \
		docker buildx create --name yamlbase-builder --driver docker-container --bootstrap || true; \
	fi
	docker buildx use yamlbase-builder
	docker buildx inspect --bootstrap

# Build multi-platform Docker image using buildx
docker-buildx: docker-setup
	docker buildx build --platform linux/amd64,linux/arm64 -t yamlbase:latest .


# Login to GitHub Container Registry
docker-login:
	@if [ -z "$$GITHUB_TOKEN" ]; then echo "Error: GITHUB_TOKEN not set"; exit 1; fi
	@echo "$$GITHUB_TOKEN" | docker login ghcr.io -u $$GITHUB_ACTOR --password-stdin

# Push multi-platform image to GitHub Container Registry
docker-push: docker-login docker-buildx
	@if [ -z "$(VERSION)" ]; then echo "Usage: make docker-push VERSION=0.1.0"; exit 1; fi
	docker buildx build --platform linux/amd64,linux/arm64 \
		-t ghcr.io/rvben/yamlbase:$(VERSION) \
		-t ghcr.io/rvben/yamlbase:latest \
		--push .

# Push multi-platform image to Docker Hub
docker-push-dockerhub: docker-login docker-buildx
	# @if [ -z "$(VERSION)" ]; then echo "Usage: make docker-push VERSION=0.1.0"; exit 1; fi
	docker buildx build --platform linux/amd64,linux/arm64 \
		-t docker.io/rvben/yamlbase:$(VERSION) \
		-t docker.io/rvben/yamlbase:latest \
		--push .


# Run with Docker
docker-run:
	docker run -d --name yamlbase -p 5432:5432 -v $$(pwd)/examples/sample_database.yaml:/data/database.yaml yamlbase:latest

# Stop Docker containers
docker-stop:
	docker stop yamlbase && docker rm yamlbase || true

# Run linting
lint:
	cargo clippy --all-targets --all-features -- -D warnings

# Format code
fmt:
	cargo fmt

# Check formatting
fmt-check:
	cargo fmt -- --check

# Type check
check:
	cargo check --all-features

# Run all CI checks (format, lint, type check, test)
ci: fmt-check check lint build test

# Run security audit
audit:
	cargo audit

# Run fuzz tests (requires nightly Rust)
fuzz:
	@echo "Running fuzz tests (requires nightly Rust)..."
	@if ! rustup toolchain list | grep -q nightly; then \
		echo "Installing nightly toolchain..."; \
		rustup install nightly; \
	fi
	@if ! command -v cargo-fuzz >/dev/null 2>&1; then \
		echo "Installing cargo-fuzz..."; \
		cargo install cargo-fuzz; \
	fi
	@echo "Running SQL parser fuzzing for 60 seconds..."
	cargo +nightly fuzz run fuzz_sql_parser -- -max_total_time=60

# Run all fuzz targets for a short time
fuzz-all:
	@echo "Running all fuzz targets for 30 seconds each..."
	cargo +nightly fuzz run fuzz_sql_parser -- -max_total_time=30
	cargo +nightly fuzz run fuzz_yaml_parser -- -max_total_time=30
	cargo +nightly fuzz run fuzz_mysql_protocol -- -max_total_time=30
	cargo +nightly fuzz run fuzz_filter_parser -- -max_total_time=30

# Test with PostgreSQL client
test-postgres:
	@echo "Testing PostgreSQL connectivity..."
	@./test_queries.sh || true

# Test with MySQL client
test-mysql:
	@echo "Testing MySQL connectivity..."
	@./test_mysql_queries.sh || true

# Integration tests with real clients
integration-test:
	@echo "Running integration tests..."
	@cargo build --release
	@./target/release/yamlbase -f examples/sample_database.yaml &
	@sleep 2
	@python3 examples/python_integration.py || true
	@pkill yamlbase || true

# Publish to crates.io
publish-crate:
	@if [ -z "$$CRATES_IO_TOKEN" ]; then echo "Error: CRATES_IO_TOKEN not set"; exit 1; fi
	cargo publish --token $$CRATES_IO_TOKEN

# Dry run publish to crates.io
publish-crate-dry:
	cargo publish --dry-run

# Release preparation
release-prep:
	@if [ -f scripts/prepare-release.sh ]; then \
		./scripts/prepare-release.sh; \
	else \
		echo "Release preparation script not found"; \
	fi

# Check if ready for release
release-check:
	@echo "Checking release readiness..."
	@echo ""
	@echo "1. Running tests..."
	@cargo test --quiet
	@echo "✓ Tests passed"
	@echo ""
	@echo "2. Checking formatting..."
	@cargo fmt -- --check
	@echo "✓ Code is formatted"
	@echo ""
	@echo "3. Running clippy..."
	@cargo clippy --all-targets --all-features -- -D warnings
	@echo "✓ No clippy warnings"
	@echo ""
	@echo "4. Security audit..."
	@cargo audit || echo "⚠ Security audit failed (non-blocking)"
	@echo ""
	@echo "5. Checking documentation..."
	@cargo doc --no-deps --quiet
	@echo "✓ Documentation builds"
	@echo ""
	@echo "6. Dry-run crates.io publish..."
	@cargo publish --dry-run --allow-dirty
	@echo "✓ Package is ready for crates.io"
	@echo ""
	@echo "✅ All checks passed! Ready for release."
	@echo ""
	@echo "Next steps:"
	@echo "  1. Run 'make release-prep' to prepare the release"
	@echo "  2. Push the tag to trigger the release workflow"

# Help target
help:
	@echo "Available targets:"
	@echo ""
	@echo "Building:"
	@echo "  make build                 - Build the project in release mode"
	@echo "  make build-target TARGET=  - Build for a specific target"
	@echo "  make build-all-targets     - Build for all supported targets"
	@echo ""
	@echo "Testing:"
	@echo "  make test                  - Run all tests"
	@echo "  make test-unit            - Run unit tests only"
	@echo "  make test-integration     - Run integration tests only"
	@echo "  make test-no-features     - Run tests without default features"
	@echo "  make coverage             - Run tests with coverage report"
	@echo "  make coverage-html        - Generate HTML coverage report"
	@echo "  make coverage-open        - Open HTML coverage report"
	@echo "  make bench                - Run benchmarks"
	@echo "  make test-postgres        - Test with PostgreSQL client"
	@echo "  make test-mysql           - Test with MySQL client"
	@echo "  make integration-test     - Run integration tests with clients"
	@echo ""
	@echo "Code Quality:"
	@echo "  make lint                 - Run linting with clippy"
	@echo "  make fmt                  - Format code"
	@echo "  make fmt-check            - Check code formatting"
	@echo "  make check                - Type check the code"
	@echo "  make ci                   - Run all CI checks"
	@echo "  make audit                - Run security audit"
	@echo "  make fuzz                 - Run fuzz testing (requires nightly)"
	@echo "  make fuzz-all             - Run all fuzz targets"
	@echo ""
	@echo "Docker:"
	@echo "  make docker-build         - Build Docker image (current platform)"
	@echo "  make docker-buildx        - Build multi-platform image"
	@echo "  make docker-push VERSION= - Push multi-platform image"
	@echo "  make docker-run           - Run with Docker"
	@echo "  make docker-stop          - Stop Docker containers"
	@echo ""
	@echo "Release:"
	@echo "  make release-check        - Check if ready for release"
	@echo "  make release-prep         - Prepare a new release"
	@echo "  make publish-crate        - Publish to crates.io"
	@echo "  make publish-crate-dry    - Dry run crates.io publish"
	@echo ""
	@echo "Other:"
	@echo "  make run                  - Run the server locally"
	@echo "  make clean                - Clean build artifacts"
	@echo "  make help                 - Show this help message"
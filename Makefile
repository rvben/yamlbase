.PHONY: all build test bench clean run docker-build docker-buildx docker-push docker-push-multiplatform docker-login docker-setup docker-run docker-stop help

# Default target
all: build

# Build the project in release mode
build:
	cargo build --release

# Run all tests
test:
	cargo test --all-features -- --nocapture

# Run unit tests only
test-unit:
	cargo test --lib --all-features -- --nocapture

# Run integration tests only
test-integration:
	cargo test --test '*' --all-features -- --nocapture

# Run tests with coverage
coverage:
	cargo tarpaulin --out Html --output-dir coverage --all-features --verbose

# Run tests with coverage and open report
coverage-open: coverage
	open coverage/tarpaulin-report.html

# Check coverage percentage
coverage-check:
	cargo tarpaulin --all-features

# Run benchmarks
bench:
	cargo bench

# Clean build artifacts
clean:
	cargo clean
	rm -rf target/

# Run the server locally with sample data
run:
	cargo run -- -f examples/sample_database.yaml --hot-reload -v

# Run with minimal logging
run-prod:
	cargo run --release -- -f examples/sample_database.yaml

# Build Docker image (local, current platform only)
docker-build:
	docker build -t yamlbase:latest .

# Build Docker image for multiple platforms (requires buildx)
docker-buildx:
	docker buildx build --platform linux/amd64,linux/arm64 -t yamlbase:latest .

# Build and push to GitHub Container Registry
docker-push: docker-login
	@if [ -z "$(VERSION)" ]; then echo "Usage: make docker-push VERSION=0.0.1"; exit 1; fi
	docker buildx build --platform linux/amd64 \
		-t ghcr.io/rvben/yamlbase:$(VERSION) \
		-t ghcr.io/rvben/yamlbase:latest \
		--push .

# Build and push multi-platform to GitHub Container Registry
docker-push-multiplatform: docker-login
	@if [ -z "$(VERSION)" ]; then echo "Usage: make docker-push-multiplatform VERSION=0.0.1"; exit 1; fi
	docker buildx build --platform linux/amd64,linux/arm64 \
		-t ghcr.io/rvben/yamlbase:$(VERSION) \
		-t ghcr.io/rvben/yamlbase:latest \
		--push .

# Login to GitHub Container Registry
docker-login:
	@echo "Logging into GitHub Container Registry..."
	@echo "$$GITHUB_TOKEN" | docker login ghcr.io -u rvben --password-stdin

# Setup Docker buildx for multi-platform builds
docker-setup:
	docker buildx create --name yamlbase-builder --use || true
	docker buildx inspect --bootstrap

# Run with Docker
docker-run:
	docker run -d --name yamlbase -p 5432:5432 -v $(PWD)/examples/sample_database.yaml:/data/database.yaml yamlbase:latest

# Stop Docker container
docker-stop:
	docker stop yamlbase && docker rm yamlbase

# Run linting
lint:
	cargo clippy -- -D warnings

# Format code
fmt:
	cargo fmt

# Check formatting
fmt-check:
	cargo fmt -- --check

# Type check
check:
	cargo check --all-features

# Run all checks (format, lint, type check, test)
ci: fmt-check check lint test

# Test with PostgreSQL client
test-postgres:
	@echo "Starting PostgreSQL server in background..."
	@cargo run -- -f examples/sample_database.yaml --protocol postgres &
	@sleep 3
	@echo "Running PostgreSQL test queries..."
	@./test_queries.sh
	@echo "Stopping server..."
	@pkill -f "cargo run.*yamlbase" || true

# Test with MySQL client
test-mysql:
	@echo "Starting MySQL server in background..."
	@cargo run -- -f examples/sample_database.yaml --protocol mysql &
	@sleep 3
	@echo "Running MySQL test queries..."
	@./test_mysql_queries.sh
	@echo "Stopping server..."
	@pkill -f "cargo run.*yamlbase" || true

# Test both protocols
test-client: test-postgres test-mysql

# Generate documentation
docs:
	cargo doc --no-deps --open

# Help target
help:
	@echo "Available targets:"
	@echo "  make build          - Build the project in release mode"
	@echo "  make test           - Run all tests"
	@echo "  make test-unit      - Run unit tests only"
	@echo "  make test-integration - Run integration tests only"
	@echo "  make coverage       - Run tests with coverage report"
	@echo "  make coverage-open  - Run coverage and open HTML report"
	@echo "  make coverage-check - Check coverage percentage"
	@echo "  make bench          - Run benchmarks"
	@echo "  make clean          - Clean build artifacts"
	@echo "  make run            - Run the server locally with sample data"
	@echo "  make run-prod       - Run in production mode (release build)"
	@echo "  make docker-build   - Build Docker image (local, current platform)"
	@echo "  make docker-buildx  - Build Docker image for multiple platforms"
	@echo "  make docker-push VERSION=x.x.x - Build and push to ghcr.io (AMD64)"
	@echo "  make docker-push-multiplatform VERSION=x.x.x - Push multi-arch to ghcr.io"
	@echo "  make docker-setup   - Setup Docker buildx for multi-platform builds"
	@echo "  make docker-run     - Run with Docker"
	@echo "  make docker-stop    - Stop Docker container"
	@echo "  make lint           - Run linting with clippy"
	@echo "  make fmt            - Format code"
	@echo "  make fmt-check      - Check code formatting"
	@echo "  make check          - Type check the code"
	@echo "  make ci             - Run all checks (format, lint, type check, test)"
	@echo "  make test-client    - Test with PostgreSQL client"
	@echo "  make docs           - Generate and open documentation"
	@echo "  make help           - Show this help message"
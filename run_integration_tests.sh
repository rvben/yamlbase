#!/bin/bash

# Script to run integration tests with real database clients

set -e

echo "=== Running YamlBase Integration Tests ==="
echo

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Function to run a test and report results
run_test() {
    local test_name=$1
    local test_file=$2
    
    echo -n "Running $test_name... "
    
    if cargo test --test $test_file -- --test-threads=1 --nocapture 2>&1 | grep -q "test result: ok"; then
        echo -e "${GREEN}✓ PASSED${NC}"
        return 0
    else
        echo -e "${RED}✗ FAILED${NC}"
        echo "Run with: cargo test --test $test_file -- --nocapture"
        return 1
    fi
}

# Check if we're in the right directory
if [ ! -f "Cargo.toml" ]; then
    echo -e "${RED}Error: Must run from yamlbase root directory${NC}"
    exit 1
fi

# Build the project first
echo "Building yamlbase..."
cargo build --release

echo
echo "=== PostgreSQL Integration Tests ==="

# Run existing PostgreSQL tests
run_test "Basic PostgreSQL tests" "basic_test" || true

# Run new PostgreSQL extended protocol tests
if [ -f "tests/integration/postgres_extended_test.rs" ]; then
    run_test "PostgreSQL Extended Protocol tests" "postgres_extended_test" || true
else
    echo -e "${YELLOW}Skipping PostgreSQL Extended Protocol tests (file not found)${NC}"
fi

echo
echo "=== MySQL 8.0+ Integration Tests ==="

# Run MySQL 8.0+ tests
if [ -f "tests/integration/mysql_8_test.rs" ]; then
    run_test "MySQL 8.0+ with caching_sha2_password" "mysql_8_test" || true
else
    echo -e "${YELLOW}Skipping MySQL 8.0+ tests (file not found)${NC}"
fi

echo
echo "=== Summary ==="
echo "Integration tests require the following dependencies:"
echo "  - PostgreSQL client library (postgres crate)"
echo "  - MySQL client library (mysql crate)"
echo
echo "To run individual tests with output:"
echo "  cargo test --test <test_name> -- --nocapture"
echo
echo "To run all integration tests:"
echo "  cargo test --test '*' -- --test-threads=1"
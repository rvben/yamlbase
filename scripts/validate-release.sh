#!/bin/bash
set -euo pipefail

# validate-release.sh - Pre-release validation script
# This script ensures the repository is in a clean state before creating a release

echo "🔍 Running pre-release validation..."

# Check if git working directory is clean
if ! git diff-index --quiet HEAD --; then
    echo "❌ ERROR: Working directory is not clean. Please commit or stash all changes."
    echo "Modified files:"
    git diff-index --name-only HEAD
    exit 1
fi

# Check if Cargo.lock is committed
if ! git ls-files --error-unmatch Cargo.lock >/dev/null 2>&1; then
    echo "❌ ERROR: Cargo.lock is not committed. Please add and commit Cargo.lock."
    exit 1
fi

# Check if Cargo.lock is up to date
echo "📦 Checking if Cargo.lock is up to date..."
cargo check --quiet
if ! git diff-index --quiet HEAD -- Cargo.lock; then
    echo "❌ ERROR: Cargo.lock is out of date. Please run 'cargo check' and commit the updated Cargo.lock."
    git diff Cargo.lock
    exit 1
fi

# Validate that version in Cargo.toml matches git tag (if tag exists)
if [ "${GITHUB_REF_NAME:-}" != "" ]; then
    TAG_VERSION="${GITHUB_REF_NAME#v}"
    CARGO_VERSION=$(grep '^version = ' Cargo.toml | head -1 | sed 's/version = "\(.*\)"/\1/')
    
    if [ "$TAG_VERSION" != "$CARGO_VERSION" ]; then
        echo "❌ ERROR: Version mismatch between git tag ($TAG_VERSION) and Cargo.toml ($CARGO_VERSION)"
        exit 1
    fi
    echo "✅ Version consistency check passed: $CARGO_VERSION"
fi

# Run a quick build to ensure everything compiles
echo "🔨 Running quick build validation..."
if ! cargo check --all-targets --quiet; then
    echo "❌ ERROR: Build validation failed"
    exit 1
fi

# Run basic tests to ensure nothing is obviously broken
echo "🧪 Running basic test validation..."
if ! cargo test --lib --quiet; then
    echo "❌ ERROR: Basic unit tests failed"
    exit 1
fi

# Validate that crates.io publish would succeed (dry-run)
echo "📦 Validating crates.io publish (dry-run)..."
if ! cargo publish --dry-run --quiet; then
    echo "❌ ERROR: cargo publish dry-run failed"
    exit 1
fi

echo "✅ Pre-release validation passed!"
echo "📋 Summary:"
echo "  - Working directory is clean"
echo "  - Cargo.lock is committed and up to date"
echo "  - Version consistency verified"
echo "  - Build validation passed"
echo "  - Unit tests passed"
echo "  - Crates.io publish dry-run passed"
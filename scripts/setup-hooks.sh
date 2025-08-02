#!/bin/bash
set -euo pipefail

# Setup git hooks for yamlbase development
# This script configures git to use the hooks in .githooks/

echo "🔧 Setting up git hooks for yamlbase..."

# Check if we're in a git repository
if ! git rev-parse --git-dir >/dev/null 2>&1; then
    echo "❌ ERROR: Not in a git repository"
    exit 1
fi

# Set git hooks path to .githooks
git config core.hooksPath .githooks

echo "✅ Git hooks configured!"
echo "📋 Available hooks:"
echo "  - pre-commit: Ensures Cargo.lock is up to date and code passes lint"
echo ""
echo "💡 To run the pre-commit hook manually: .githooks/pre-commit"
echo "💡 To bypass hooks temporarily: git commit --no-verify"
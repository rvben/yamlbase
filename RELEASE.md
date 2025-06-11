# Release Process

This document outlines the release process for yamlbase.

## Prerequisites

- Ensure you have appropriate permissions to:
  - Push tags to the repository
  - Publish to crates.io
  - Push Docker images to Docker Hub and GitHub Container Registry

## Pre-release Checklist

1. **Update Version**
   - Update version in `Cargo.toml`
   - Run `cargo check` to ensure version is valid

2. **Update Documentation**
   - Update CHANGELOG.md with release notes
   - Update README.md if needed
   - Ensure all examples are up to date

3. **Run Tests**
   ```bash
   # Run all tests
   cargo test --all-features
   
   # Run clippy
   cargo clippy --all-targets --all-features -- -D warnings
   
   # Check formatting
   cargo fmt -- --check
   
   # Run benchmarks
   cargo bench
   ```

4. **Test Build**
   ```bash
   # Build release binary
   cargo build --release
   
   # Test the binary
   ./target/release/yamlbase -f examples/sample_database.yaml
   ```

5. **Test Docker Build**
   ```bash
   docker build -t yamlbase:test .
   docker run -p 5432:5432 yamlbase:test
   ```

## Release Steps

1. **Commit Changes**
   ```bash
   git add -A
   git commit -m "chore: prepare release v0.0.1"
   git push origin main
   ```

2. **Create and Push Tag**
   ```bash
   git tag -a v0.0.1 -m "Release version 0.0.1"
   git push origin v0.0.1
   ```

3. **Monitor CI/CD**
   - GitHub Actions will automatically:
     - Create a GitHub release
     - Build binaries for multiple platforms
     - Push Docker images to registries
     - Publish to crates.io

4. **Verify Release**
   - Check GitHub releases page
   - Verify crates.io publication
   - Test Docker images:
     ```bash
     docker pull ghcr.io/rvben/yamlbase:0.0.1
     docker pull ghcr.io/rvben/yamlbase:latest
     ```

## Post-release

1. **Announce Release**
   - Create announcement for relevant channels
   - Update project website/docs if applicable

2. **Prepare for Next Release**
   - Update version in Cargo.toml to next development version
   - Add new "Unreleased" section to CHANGELOG.md

## Versioning Strategy

We follow [Semantic Versioning](https://semver.org/):
- MAJOR version for incompatible API changes
- MINOR version for backwards-compatible functionality additions  
- PATCH version for backwards-compatible bug fixes

## Emergency Procedures

### Yanking a Release

If a critical issue is found:

1. **Yank from crates.io**
   ```bash
   cargo yank --version 0.0.1
   ```

2. **Delete GitHub Release**
   - Go to releases page
   - Delete the problematic release

3. **Remove Docker Tags**
   - Delete tags from GitHub Container Registry via the GitHub UI or API

### Hotfix Process

1. Create hotfix branch from tag
2. Apply fix and test thoroughly  
3. Release as patch version
4. Merge fix back to main branch
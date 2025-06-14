# Contributing to yamlbase

Thank you for your interest in contributing to yamlbase! This document provides guidelines and instructions for contributing.

## Code of Conduct

By participating in this project, you agree to abide by our code of conduct: be respectful, inclusive, and constructive in all interactions.

## How to Contribute

### Reporting Issues

- Use the GitHub issue tracker to report bugs
- Describe the issue clearly and include:
  - Steps to reproduce
  - Expected behavior
  - Actual behavior
  - System information (OS, Rust version)
  - Relevant logs or error messages

### Suggesting Features

- Open an issue with the "feature request" label
- Explain the use case and benefits
- Consider implementation complexity

### Submitting Changes

1. **Fork the Repository**
   ```bash
   git clone https://github.com/rvben/yamlbase.git
   cd yamlbase
   ```

2. **Create a Branch**
   ```bash
   git checkout -b feature/your-feature-name
   # or
   git checkout -b fix/issue-description
   ```

3. **Make Changes**
   - Follow the coding style guide
   - Add tests for new functionality
   - Update documentation as needed

4. **Test Your Changes**
   ```bash
   # Run tests
   cargo test --all-features
   
   # Run clippy
   cargo clippy --all-targets --all-features -- -D warnings
   
   # Format code
   cargo fmt
   
   # Run benchmarks if performance-related
   cargo bench
   ```

5. **Commit Your Changes**
   - Use clear, descriptive commit messages
   - Follow conventional commits format:
     - `feat:` for new features
     - `fix:` for bug fixes
     - `docs:` for documentation
     - `test:` for tests
     - `refactor:` for refactoring
     - `perf:` for performance improvements

6. **Push and Create Pull Request**
   ```bash
   git push origin your-branch-name
   ```
   - Open a pull request against the `main` branch
   - Fill out the PR template completely
   - Link related issues

## Development Setup

### Prerequisites

- Rust 1.70.0 or later
- Git

### Building from Source

```bash
# Clone the repository
git clone https://github.com/rvben/yamlbase.git
cd yamlbase

# Build
cargo build

# Run tests
cargo test

# Run with example database
cargo run -- -f examples/sample_database.yaml
```

### Project Structure

```
yamlbase/
├── src/
│   ├── main.rs           # CLI entry point
│   ├── lib.rs            # Library root
│   ├── server/           # Server implementation
│   ├── protocol/         # Protocol implementations
│   │   ├── postgres.rs   # PostgreSQL wire protocol
│   │   └── mysql_simple.rs # MySQL wire protocol
│   ├── database/         # Database engine
│   ├── sql/              # SQL parsing and execution
│   └── yaml/             # YAML parsing and schema
├── tests/                # Integration tests
├── benches/              # Performance benchmarks
└── examples/             # Example YAML databases
```

## Coding Guidelines

### Style

- Follow Rust standard style guidelines
- Use `cargo fmt` before committing
- Keep line length under 100 characters when practical
- Use descriptive variable and function names

### Documentation

- Add doc comments for public APIs
- Include examples in doc comments
- Update README.md for user-facing changes
- Add inline comments for complex logic

### Error Handling

- Use `Result<T, Error>` for fallible operations
- Provide helpful error messages
- Use `anyhow` for application errors
- Use `thiserror` for library errors

### Performance

- Benchmark performance-critical code
- Avoid unnecessary allocations
- Use appropriate data structures
- Consider async/await for I/O operations

## Testing

### Unit Tests

- Place unit tests in the same file as the code
- Test edge cases and error conditions
- Use descriptive test names

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_feature_description() {
        // Test implementation
    }
}
```

### Integration Tests

- Place in `tests/` directory
- Test complete workflows
- Use realistic test data

### Running Tests

```bash
# All tests
cargo test

# Specific test
cargo test test_name

# With output
cargo test -- --nocapture

# Integration tests only
cargo test --test '*'
```

## Pull Request Process

1. **Before Submitting**
   - Ensure all tests pass
   - Run `cargo clippy` and fix warnings
   - Run `cargo fmt`
   - Update documentation
   - Add tests for new features

2. **PR Requirements**
   - Clear description of changes
   - Link to related issues
   - All CI checks must pass
   - At least one approval from maintainers

3. **Review Process**
   - Address reviewer feedback
   - Keep PR focused and reasonably sized
   - Be patient - reviews may take time

## Release Process

See [RELEASE.md](RELEASE.md) for details on the release process.

## Getting Help

- Check existing issues and documentation
- Ask questions in issues with "question" label
- Join community discussions

## Recognition

Contributors will be recognized in:
- GitHub contributors page
- Release notes for significant contributions
- Project documentation

Thank you for contributing to yamlbase!
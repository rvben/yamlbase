# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.0.2] - 2025-06-12

### Fixed
- Fixed panic when connections were made to the server due to `blocking_read()` in async context
- Removed blocking calls in PostgreSQL and MySQL protocol handlers

### Added
- Docker build targets in Makefile for easier local builds and releases
- Support for building and pushing Docker images to GitHub Container Registry

## [0.0.1] - 2025-06-11

### Added
- Initial release of yamlbase
- PostgreSQL wire protocol support for authentic PostgreSQL client compatibility
- MySQL wire protocol support for MySQL client compatibility
- YAML-based database definition with schema validation
- Support for common SQL data types: INTEGER, VARCHAR, TEXT, TIMESTAMP, BOOLEAN, DECIMAL, etc.
- SELECT query support with WHERE, ORDER BY, and LIMIT clauses
- Basic expression evaluation in WHERE clauses
- Hot-reload functionality for development workflows
- Configurable authentication (username/password)
- Command-line interface with comprehensive options
- Docker support with minimal container images
- Integration examples for Python, Node.js, and Go
- Comprehensive test suite and benchmarks

### Features
- Define database schema and data in simple YAML files
- Serve data over standard SQL protocols (PostgreSQL and MySQL)
- Perfect for local development and testing
- Minimal resource usage and fast startup
- Cross-platform support (Linux, macOS, Windows)

### Known Limitations
- Read-only operations (no INSERT/UPDATE/DELETE support)
- No transaction support
- Basic SQL feature set
- No indexes beyond primary keys
- SQL Server protocol not yet implemented

[0.0.2]: https://github.com/rvben/yamlbase/releases/tag/v0.0.2
[0.0.1]: https://github.com/rvben/yamlbase/releases/tag/v0.0.1
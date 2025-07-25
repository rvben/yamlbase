# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.3.0] - 2025-07-22

### Added
- **Advanced SQL Features**:
  - Subquery support for IN/NOT IN operators (e.g., `WHERE id IN (SELECT ...)`
  - EXISTS and NOT EXISTS subqueries for complex filtering
  - UNION, INTERSECT, and EXCEPT set operations with proper duplicate handling
  - BETWEEN operator for range comparisons
  - Enhanced date/time functions:
    - Extended EXTRACT function with fields: QUARTER, WEEK, DOW, DOY, CENTURY, DECADE, EPOCH, and more
    - DATE_PART function as PostgreSQL-compatible alias for EXTRACT
  - Math functions: ROUND, CEIL/CEILING, FLOOR, ABS, MOD
  - String functions: LEFT, RIGHT, POSITION, CONCAT
  - Improved function evaluation in different contexts (SELECT, WHERE, expressions)

### Fixed
- PostgreSQL protocol SUM aggregate function type encoding (was returning TEXT, now correctly returns DOUBLE)
- Set operation (UNION/INTERSECT/EXCEPT) column type inference
- Function implementations now work correctly in all SQL contexts

### Changed
- Significantly expanded SQL compatibility for more complex queries
- Improved type handling for aggregate functions in PostgreSQL protocol

## [0.2.0] - 2025-07-15

### Added
- **Enhanced SQL Support** for enterprise applications:
  - SQLAlchemy compatibility with `SELECT VERSION()` and transaction command support (START TRANSACTION, COMMIT, ROLLBACK)
  - Full JOIN support including INNER, LEFT, and CROSS JOINs with proper table aliasing
  - Comprehensive date function support:
    - Basic: `CURRENT_DATE`, `NOW()`
    - Advanced: `ADD_MONTHS()`, `EXTRACT()`, `LAST_DAY()`
  - Enhanced aggregate functions:
    - `COUNT(DISTINCT column)` for unique value counting
    - `AVG()`, `MIN()`, `MAX()` with proper NULL handling
  - Complex WHERE clause operators including `NOT IN` and nested conditions
  - Partial CTE/WITH clause support (parsing implemented, execution pending)
- Comprehensive test coverage for all new SQL features
- Upgraded to **Rust 2024 edition** with minimum Rust version 1.85

### Fixed
- Clippy warnings in SQL executor code
- Excessive debug output in integration tests

### Changed
- Improved SQL query execution performance with optimized expression evaluation
- Better error messages for unsupported SQL features

## [0.1.0] - 2025-06-17

### Added
- **PostgreSQL Extended Query Protocol** support for prepared statements and parameterized queries
  - Parse, Bind, Describe, Execute, and Sync message handling
  - Portal and statement management for efficient query execution
  - Proper handling of binary and text parameter formats
  - Full compatibility with PostgreSQL client libraries using extended protocol
- **MySQL 8.0+ Authentication** with caching_sha2_password support
  - Implementation of MySQL's default authentication method since version 8.0
  - RSA public key exchange for secure password transmission
  - Fast authentication path for cached credentials
  - Backward compatibility with mysql_native_password
- **SELECT constant FROM table** syntax support (e.g., `SELECT 1 FROM users`)
  - Commonly used pattern for health checks and connection testing
  - Proper handling of constant expressions in presence of FROM clause
- Comprehensive **fuzz testing** infrastructure for all parsers
  - SQL parser fuzzing to ensure robustness
  - YAML parser fuzzing for configuration safety
  - MySQL protocol fuzzing for security
  - Filter parser fuzzing with UTF-8 boundary fixes
- Enhanced **integration testing** with real database clients
  - PostgreSQL extended protocol flow tests
  - MySQL 8.0+ authentication tests
  - Parameter parsing and binding tests
  - Debug utilities for protocol development

### Fixed
- Critical compatibility issues with real PostgreSQL and MySQL clients
- Protocol message handling for complex query flows
- Binary parameter encoding/decoding in PostgreSQL extended protocol
- Connection state management for prepared statements
- UTF-8 boundary crashes in filter parsing (discovered via fuzzing)

### Changed
- Significant internal refactoring to support stateful protocol features
- Improved error handling and protocol compliance
- Better separation of simple and extended protocol paths
- Enhanced debug logging for protocol troubleshooting

### Developer Experience
- Added example files for new protocol features
- Comprehensive test coverage for authentication flows
- Integration test scripts for real client testing
- Debug tools for protocol message inspection

## [0.0.6] - 2025-06-15

### Added
- Enhanced SQL support for standard SQL functionality:
  - IN and NOT IN operators for list comparisons
  - LIKE and NOT LIKE operators with % and _ wildcards
  - Support for regex special character escaping in LIKE patterns
  - Proper handling of nested expressions with parentheses
  - SELECT without FROM clause for constant expressions (e.g., `SELECT 1`, `SELECT 'hello' AS greeting`)
  - Arithmetic operations in SELECT expressions
- Comprehensive fuzz testing infrastructure using cargo-fuzz
- CI/CD improvements:
  - Unified Makefile-based approach for all CI/CD operations
  - Multi-platform Docker builds using buildx
  - Simplified Docker build process with in-container compilation
- Improved MySQL protocol compatibility:
  - Added EOF packets after column definitions for better client compatibility
  - Enhanced debug logging for protocol troubleshooting

### Fixed
- Critical UTF-8 boundary crash in filter parser discovered through fuzz testing
- MySQL protocol test updated to handle EOF packets correctly

### Changed
- CI/CD workflows now use make targets exclusively, following the principle that all CI actions must be runnable locally
- Docker images now build from source inside containers for better multi-platform support
- Removed obsolete Dockerfile.release files and related make targets

### Developer Experience
- Added comprehensive CI/CD documentation guide
- Improved test infrastructure with better MySQL protocol handling
- Enhanced debugging with more detailed protocol logging

## [0.0.5] - 2025-06-13

### Fixed
- Fixed MySQL protocol to support `SET NAMES` command sent by mysql-connector-python and other MySQL clients during connection initialization
- Server now properly handles various SET commands (SET NAMES, SET CHARACTER SET, SET SESSION variables) by acknowledging them with OK response

### Added
- Comprehensive test coverage for MySQL protocol edge cases:
  - SET NAMES command variations and character sets
  - SET SESSION variables commonly used by MySQL clients
  - MySQL protocol command handling (COM_INIT_DB, COM_PING, unknown commands)
  - Query edge cases (comments, special characters, case sensitivity)
  - MySQL system variables (@@version, @@sql_mode, etc.)

### Improved
- MySQL protocol compatibility with standard MySQL client libraries
- Error handling for unsupported MySQL protocol commands

## [0.0.4] - 2025-06-12

### Added
- Comprehensive test coverage for authentication features
  - Integration tests for PostgreSQL YAML authentication
  - Integration tests for MySQL YAML authentication
  - Unit tests for YAML auth config parsing
  - Unit tests for server auth override behavior
  - Tests for PostgreSQL SSL negotiation handling
  - Async safety tests to prevent blocking operations
- Example YAML file demonstrating authentication configuration (`examples/database_with_auth.yaml`)
- GitHub Actions CI workflow for automated testing
- CONTRIBUTING.md guidelines for contributors

### Improved
- Test infrastructure with better organization and coverage
- Documentation for authentication features in README

### Developer Experience
- Added test utilities for easier testing of new features
- Improved error messages for authentication failures

## [0.0.3] - 2025-06-12

### Added
- YAML-based authentication configuration allowing per-database credentials
- Support for specifying username and password directly in YAML files
- Authentication settings in YAML override command-line arguments

### Fixed
- Fixed PostgreSQL authentication to use clear text password instead of MD5
- Fixed username parsing issue after SSL negotiation in PostgreSQL protocol

### Changed
- Authentication changes require server restart (not hot-reloadable for security)

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

[0.3.0]: https://github.com/rvben/yamlbase/releases/tag/v0.3.0
[0.2.0]: https://github.com/rvben/yamlbase/releases/tag/v0.2.0
[0.1.0]: https://github.com/rvben/yamlbase/releases/tag/v0.1.0
[0.0.6]: https://github.com/rvben/yamlbase/releases/tag/v0.0.6
[0.0.5]: https://github.com/rvben/yamlbase/releases/tag/v0.0.5
[0.0.4]: https://github.com/rvben/yamlbase/releases/tag/v0.0.4
[0.0.3]: https://github.com/rvben/yamlbase/releases/tag/v0.0.3
[0.0.2]: https://github.com/rvben/yamlbase/releases/tag/v0.0.2
[0.0.1]: https://github.com/rvben/yamlbase/releases/tag/v0.0.1
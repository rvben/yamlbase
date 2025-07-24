# YAML SQL Server - Product Requirements Document

## Executive Summary

A lightweight SQL server application that serves YAML-defined tables over standard SQL protocols, designed for local development and testing environments. The application reads table definitions and data from YAML files and exposes them via SQL queries on the standard SQL port (1433 for SQL Server protocol, 3306 for MySQL protocol, or 5432 for PostgreSQL protocol).

## Problem Statement

Developers frequently need to test applications against databases during development, but setting up full database servers with test data is time-consuming and resource-intensive. Existing solutions either require complex database installations or don't provide authentic SQL protocol compatibility for testing database drivers and ORMs.

## Goals

**Primary Goals:**
- Enable instant database setup for local testing using simple YAML files
- Provide authentic SQL protocol compatibility for testing database connections
- Minimize resource usage compared to full database servers
- Support standard SQL SELECT queries for read operations

**Secondary Goals:**
- Future extensibility for write operations (INSERT, UPDATE, DELETE)
- Multiple SQL protocol support (PostgreSQL, MySQL, SQL Server)
- Hot-reloading of YAML files during development

## Target Users

- Application developers testing database integrations
- QA engineers setting up test environments
- DevOps engineers creating lightweight testing infrastructure
- Students learning SQL and database concepts

## Core Features

### Phase 1: Read-Only SQL Server

**YAML Schema Support**
- Parse YAML files with table definitions including column names and data types
- Support common SQL data types: INTEGER, VARCHAR(n), TEXT, TIMESTAMP, BOOLEAN, DECIMAL(p,s)
- Handle multiple tables per YAML file or multiple YAML files
- Validate data against defined schema on startup

**SQL Protocol Server**
- Listen on configurable port (default based on chosen SQL dialect)
- Implement core SQL protocol handshake and authentication
- Support basic authentication (username/password) with configurable credentials
- Handle connection pooling and concurrent client connections

**Query Processing**
- Parse and execute SELECT statements with WHERE clauses
- Support basic SQL operations: filtering, sorting (ORDER BY), limiting (LIMIT/TOP)
- Implement JOIN operations between tables
- Support aggregate functions: COUNT, SUM, AVG, MIN, MAX
- Provide basic SQL functions: string manipulation, date functions

**Data Management**
- Load YAML data into in-memory table structures on startup
- Validate data types and constraints
- Support file watching for hot-reload during development
- Handle large datasets efficiently with lazy loading if needed

### Phase 2: Write Operations (Future)

**Mutation Support**
- INSERT statements with validation against schema
- UPDATE statements with WHERE clause support
- DELETE statements
- Transaction support (BEGIN, COMMIT, ROLLBACK)

**Persistence Options**
- Write changes back to YAML files
- Optional in-memory only mode for temporary testing
- Backup and restore functionality

## Technical Requirements

### Performance
- Handle up to 10,000 records per table efficiently
- Support up to 10 concurrent connections
- Query response time under 100ms for simple queries
- Memory usage under 100MB for typical test datasets

### Compatibility
- Support PostgreSQL wire protocol (recommended for broad compatibility)
- Optional MySQL protocol support
- Standard SQL syntax compliance for core operations
- Compatible with popular database drivers and ORMs

### Configuration
- YAML file path specification via command line or config file
- Port configuration
- Authentication settings
- Logging levels and output options

### Error Handling
- Graceful handling of malformed YAML files
- Clear error messages for SQL syntax errors
- Connection timeout and cleanup
- Detailed logging for debugging

## YAML File Format Specification

```yaml
database:
  name: "test_db"

tables:
  users:
    columns:
      id: "INTEGER PRIMARY KEY"
      name: "VARCHAR(100) NOT NULL"
      email: "VARCHAR(255) UNIQUE"
      created_at: "TIMESTAMP DEFAULT CURRENT_TIMESTAMP"
      is_active: "BOOLEAN DEFAULT true"
    data:
      - id: 1
        name: "John Doe"
        email: "john@example.com"
        created_at: "2024-01-15 10:30:00"
        is_active: true
      - id: 2
        name: "Jane Smith"
        email: "jane@example.com"
        created_at: "2024-01-16 14:22:00"
        is_active: false

  orders:
    columns:
      id: "INTEGER PRIMARY KEY"
      user_id: "INTEGER REFERENCES users(id)"
      amount: "DECIMAL(10,2)"
      status: "VARCHAR(50)"
    data:
      - id: 101
        user_id: 1
        amount: 29.99
        status: "completed"
      - id: 102
        user_id: 2
        amount: 45.50
        status: "pending"
```

## Implementation Architecture

## Rust Implementation Architecture

### Project Structure
```
yaml-sql-server/
├── Cargo.toml
├── src/
│   ├── main.rs              # CLI entry point and server setup
│   ├── lib.rs               # Public API and module exports
│   ├── config.rs            # Configuration management
│   ├── yaml/
│   │   ├── mod.rs           # YAML parsing and validation
│   │   ├── parser.rs        # YAML to internal schema conversion
│   │   └── watcher.rs       # File watching for hot-reload
│   ├── database/
│   │   ├── mod.rs           # Database abstraction layer
│   │   ├── schema.rs        # Table schema definitions
│   │   ├── storage.rs       # In-memory data storage
│   │   └── index.rs         # Basic indexing for performance
│   ├── sql/
│   │   ├── mod.rs           # SQL processing coordination
│   │   ├── parser.rs        # SQL AST parsing wrapper
│   │   ├── executor.rs      # Query execution engine
│   │   └── functions.rs     # SQL function implementations
│   ├── protocol/
│   │   ├── mod.rs           # Protocol abstraction
│   │   ├── postgres.rs      # PostgreSQL wire protocol
│   │   └── connection.rs    # Connection management
│   └── server.rs            # Main server loop and client handling
├── tests/
│   ├── integration/         # End-to-end protocol tests
│   ├── sql/                 # SQL query execution tests
│   └── yaml/                # YAML parsing tests
└── examples/
    ├── sample_data.yaml     # Example database files
    └── client_test.rs       # Example client connections
```

### Key Rust Dependencies

```toml
[dependencies]
# Async runtime and networking
tokio = { version = "1.0", features = ["full"] }
tokio-postgres = "0.7"  # For protocol reference
bytes = "1.0"

# SQL and data processing
sqlparser = "0.40"
serde = { version = "1.0", features = ["derive"] }
serde_yaml = "0.9"

# CLI and configuration
clap = { version = "4.0", features = ["derive"] }
config = "0.14"

# Error handling and logging
anyhow = "1.0"
thiserror = "1.0"
tracing = "0.1"
tracing-subscriber = { version = "0.3", features = ["env-filter"] }

# File watching
notify = "6.0"

# Performance and utilities
dashmap = "5.0"  # Concurrent HashMap for indexing
chrono = { version = "0.4", features = ["serde"] }
uuid = { version = "1.0", features = ["v4"] }

[dev-dependencies]
tokio-test = "0.4"
criterion = "0.5"
tempfile = "3.0"
```

### Core Type Definitions

```rust
// src/database/schema.rs
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SqlType {
    Integer,
    Varchar(usize),
    Text,
    Timestamp,
    Boolean,
    Decimal(u8, u8), // precision, scale
}

#[derive(Debug, Clone)]
pub struct Column {
    pub name: String,
    pub sql_type: SqlType,
    pub primary_key: bool,
    pub nullable: bool,
    pub unique: bool,
    pub default: Option<String>,
}

#[derive(Debug, Clone)]
pub struct Table {
    pub name: String,
    pub columns: Vec<Column>,
    pub data: Vec<HashMap<String, serde_yaml::Value>>,
}

#[derive(Debug)]
pub struct Database {
    pub name: String,
    pub tables: HashMap<String, Table>,
}
```

### Performance Optimizations

**Connection Pooling**
- Use `tokio::sync::Semaphore` to limit concurrent connections
- Implement connection reuse with `Arc<Mutex<Connection>>` patterns
- Graceful connection cleanup on client disconnect

**Query Optimization**
- Basic hash indexing for primary keys using `DashMap`
- Query plan caching for repeated queries
- Lazy evaluation for large result sets

**Memory Management**
- `Arc<RwLock<Database>>` for shared read access, exclusive write access
- Copy-on-write for data modifications in future write support
- Efficient string handling with `Cow<'_, str>` where appropriate

### Technology Stack (Rust Implementation)

**Core Language:** Rust 1.70+
**SQL Parsing:** `sqlparser` crate for robust SQL AST parsing
**PostgreSQL Protocol:** `tokio-postgres` protocol implementation or custom using `bytes`
**Async Runtime:** `tokio` for async I/O and connection handling
**YAML Processing:** `serde_yaml` for parsing, `serde` for serialization
**Configuration:** `clap` for CLI, `config` crate for file-based config
**Logging:** `tracing` with `tracing-subscriber` for structured logging
**Error Handling:** `anyhow` for error chaining, `thiserror` for custom errors
**Testing:** `tokio-test` for async testing, `criterion` for benchmarks

## Command Line Interface

```bash
yaml-sql-server [OPTIONS]

Options:
  -f, --file <YAML_FILE>     Path to YAML database file (required)
  -p, --port <PORT>          Port to listen on (default: 5432)
  --protocol <PROTOCOL>      SQL protocol: postgres, mysql, sqlserver (default: postgres)
  -u, --username <USER>      Authentication username (default: admin)
  -P, --password <PASS>      Authentication password (default: password)
  --hot-reload               Enable hot-reloading of YAML file changes
  -v, --verbose             Enable verbose logging
  --log-level <LEVEL>       Set log level: debug, info, warn, error
  -h, --help                Display help information

Examples:
  yaml-sql-server -f test_data.yaml
  yaml-sql-server -f test_data.yaml -p 3306 --protocol mysql
  yaml-sql-server -f test_data.yaml --hot-reload -v
```

## Success Metrics

**Functionality Metrics**
- Successfully parse and serve 95%+ of valid YAML database files
- Handle all basic SQL SELECT operations correctly
- Maintain connection stability for extended testing sessions

**Performance Metrics**
- Query response time consistently under 100ms
- Support 10+ concurrent connections without degradation
- Memory usage scaling linearly with data size

**Adoption Metrics**
- Integration with popular development frameworks and ORMs
- Positive developer experience feedback
- Usage in CI/CD pipelines for automated testing

## Risks and Mitigations

**Risk: SQL Protocol Complexity**
- Mitigation: Start with PostgreSQL protocol (well-documented), leverage existing libraries

**Risk: Performance with Large Datasets**
- Mitigation: Implement basic indexing, add pagination for large result sets

**Risk: SQL Feature Completeness**
- Mitigation: Focus on core features used in testing, document limitations clearly

**Risk: Protocol Compatibility Issues**
- Mitigation: Test with popular database drivers and ORMs, maintain compatibility matrix

## Future Enhancements

- Web-based admin interface for browsing tables and data
- Multiple YAML file support with schema merging
- Advanced SQL features: subqueries, window functions, CTEs
- Export functionality to real database formats
- Integration with testing frameworks
- Docker container distribution
- Performance monitoring and query optimization tools

## Development Phases (Rust-Specific)

### Phase 1: Core Infrastructure (Weeks 1-3)
- Set up Rust project with async tokio runtime
- Implement YAML parsing with `serde_yaml` and schema validation
- Create in-memory database structures with `Arc<RwLock<Database>>`
- Basic CLI with `clap` for configuration

### Phase 2: SQL Engine (Weeks 4-6)
- Integrate `sqlparser` crate for SQL AST parsing
- Implement query executor for SELECT statements with WHERE clauses
- Add support for JOINs, ORDER BY, and basic aggregate functions
- Create comprehensive test suite for SQL compatibility

### Phase 3: PostgreSQL Protocol (Weeks 7-8)
- Implement PostgreSQL wire protocol message handling
- Add authentication and connection management
- Handle multiple concurrent connections with tokio spawning
- Protocol compliance testing with real PostgreSQL clients

### Phase 4: Polish & Performance (Weeks 9-10)
- Add hot-reload functionality using `notify` crate
- Performance optimization and benchmarking with `criterion`
- Comprehensive documentation and examples
- Integration testing with popular Rust ORMs (diesel, sea-orm, sqlx)

## Rust-Specific Implementation Notes

**Error Handling Strategy**
```rust
#[derive(thiserror::Error, Debug)]
pub enum YamlSqlError {
    #[error("YAML parsing error: {0}")]
    YamlParse(#[from] serde_yaml::Error),

    #[error("SQL parsing error: {0}")]
    SqlParse(#[from] sqlparser::parser::ParserError),

    #[error("Database error: {message}")]
    Database { message: String },

    #[error("Protocol error: {0}")]
    Protocol(#[from] std::io::Error),
}
```

**Async Architecture**
- Use `tokio::spawn` for handling each client connection
- `tokio::sync::broadcast` for notifying connections of schema changes
- `tokio::time::timeout` for connection timeouts and query limits
- `tokio::fs` for async file operations during hot-reload

**Testing Strategy**
- Unit tests for each module with `#[tokio::test]`
- Integration tests using `tokio-postgres` client to verify protocol compliance
- Property-based testing with `proptest` for SQL query generation
- Benchmark suite comparing performance against SQLite for similar datasets
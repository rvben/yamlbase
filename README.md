# yamlbase

A lightweight SQL server that serves YAML-defined tables over standard SQL protocols, designed for local development and testing.

## Features

- 🚀 **Quick Setup** - Define your database schema and data in simple YAML files
- 🐘 **PostgreSQL Protocol** - Compatible with standard PostgreSQL clients and drivers
- 📊 **SQL Support** - SELECT queries with WHERE, ORDER BY, LIMIT, and basic JOINs
- 🔄 **Hot Reload** - Automatically reload data when YAML files change
- 🛠️ **Development Friendly** - Perfect for testing database integrations locally
- ⚡ **Lightweight** - Minimal resource usage, fast startup

## Installation

### From Source
```bash
cargo install --path .
```

### Using Cargo
```bash
cargo build --release
```

## Quick Start

1. Create a YAML file defining your database:
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
```

2. Start the server:
```bash
yamlbase -f database.yaml
```

3. Connect with any PostgreSQL client:
```bash
# Using psql
psql -h localhost -p 5432 -U admin -d test_db

# Password: password (default)
```

4. Run SQL queries:
```sql
SELECT * FROM users WHERE is_active = true;
SELECT name, email FROM users ORDER BY created_at DESC LIMIT 5;
```

## Command Line Options

```
yamlbase [OPTIONS]

Options:
  -f, --file <FILE>          Path to YAML database file
  -p, --port <PORT>          Port to listen on [default: 5432]
      --bind-address <ADDR>  Address to bind to [default: 0.0.0.0]
      --protocol <PROTOCOL>  SQL protocol: postgres, mysql, sqlserver [default: postgres]
  -u, --username <USER>      Authentication username [default: admin]
  -P, --password <PASS>      Authentication password [default: password]
      --hot-reload           Enable hot-reloading of YAML file changes
  -v, --verbose              Enable verbose logging
      --log-level <LEVEL>    Set log level: debug, info, warn, error [default: info]
  -h, --help                 Print help
```

## YAML Database Format

### Supported Data Types

- `INTEGER` / `INT` / `BIGINT` / `SMALLINT`
- `VARCHAR(n)` - Variable-length string with max length
- `TEXT` - Unlimited text
- `TIMESTAMP` / `DATETIME`
- `DATE`
- `TIME`
- `BOOLEAN` / `BOOL`
- `DECIMAL(p,s)` / `NUMERIC(p,s)` - Fixed-point decimal
- `FLOAT` / `REAL`
- `DOUBLE`
- `UUID`
- `JSON` / `JSONB`

### Column Constraints

- `PRIMARY KEY` - Unique identifier for the table
- `NOT NULL` - Column cannot contain NULL values
- `UNIQUE` - All values must be unique
- `DEFAULT <value>` - Default value for new rows
- `REFERENCES table(column)` - Foreign key reference

### Special Default Values

- `CURRENT_TIMESTAMP` - Current date and time
- `true` / `false` - Boolean values
- String, number, or NULL values

## SQL Support

### Currently Supported

- `SELECT` queries with column selection
- `WHERE` clauses with comparison operators (`=`, `!=`, `<`, `>`, `<=`, `>=`)
- `AND` / `OR` logical operators
- `ORDER BY` with `ASC` / `DESC`
- `LIMIT` for result pagination
- Wildcard selection (`SELECT *`)
- Basic table joins (comma-separated tables in FROM)

### Examples

```sql
-- Select with conditions
SELECT * FROM users WHERE is_active = true AND age > 25;

-- Order and limit
SELECT name, email FROM users ORDER BY created_at DESC LIMIT 10;

-- Join tables
SELECT u.name, o.total_amount 
FROM users u, orders o 
WHERE u.id = o.user_id;
```

### Not Yet Supported

- `INSERT`, `UPDATE`, `DELETE` operations
- Aggregate functions (`COUNT`, `SUM`, `AVG`, etc.)
- `GROUP BY` and `HAVING`
- Subqueries
- `JOIN` syntax (use comma-separated tables instead)
- Transactions

## Development

### Running Tests
```bash
cargo test
```

### Building
```bash
cargo build --release
```

### Running with Hot Reload
```bash
cargo run -- -f examples/sample_database.yaml --hot-reload -v
```

## Integration Examples

### Python
```python
import psycopg2

conn = psycopg2.connect(
    host="localhost",
    port=5432,
    database="test_db",
    user="admin",
    password="password"
)

cur = conn.cursor()
cur.execute("SELECT * FROM users WHERE is_active = true")
users = cur.fetchall()
```

### Node.js
```javascript
const { Client } = require('pg');

const client = new Client({
  host: 'localhost',
  port: 5432,
  database: 'test_db',
  user: 'admin',
  password: 'password',
});

await client.connect();
const res = await client.query('SELECT * FROM users');
```

### Go
```go
import (
    "database/sql"
    _ "github.com/lib/pq"
)

db, err := sql.Open("postgres", 
    "host=localhost port=5432 user=admin password=password dbname=test_db sslmode=disable")

rows, err := db.Query("SELECT name, email FROM users")
```

## Use Cases

- **Local Development** - Test database-dependent code without setting up PostgreSQL
- **CI/CD Testing** - Lightweight database for integration tests
- **Prototyping** - Quickly iterate on database schemas
- **Education** - Learn SQL without database installation
- **Demo Applications** - Ship demos with embedded test data

## Performance

- Handles up to 10,000 records per table efficiently
- Supports 10+ concurrent connections
- Query response time typically under 100ms
- Memory usage under 100MB for typical test datasets

## Limitations

- Read-only operations (no INSERT/UPDATE/DELETE yet)
- Basic SQL feature set
- No transaction support
- No indexes beyond primary keys
- PostgreSQL protocol only (MySQL and SQL Server planned)

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is dual-licensed under MIT OR Apache-2.0
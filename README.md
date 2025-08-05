# yamlbase

<div align="center">
  <img src="https://raw.githubusercontent.com/rvben/yamlbase/main/assets/logo.png" alt="yamlbase logo" width="400">

  **A lightweight SQL server that serves YAML-defined tables over standard SQL protocols**

  [![Crates.io](https://img.shields.io/crates/v/yamlbase.svg)](https://crates.io/crates/yamlbase)
  [![Documentation](https://docs.rs/yamlbase/badge.svg)](https://docs.rs/yamlbase)
  [![License](https://img.shields.io/crates/l/yamlbase.svg)](https://github.com/rvben/yamlbase#license)
  [![Build Status](https://img.shields.io/github/actions/workflow/status/rvben/yamlbase/ci.yml?branch=main)](https://github.com/rvben/yamlbase/actions)
</div>

---

Yamlbase is a lightweight SQL server designed for local development and testing. Define your database schema and data in simple YAML files and serve them over standard PostgreSQL or MySQL wire protocols.

## Features

- 🚀 **Quick Setup** - Define your database schema and data in simple YAML files
- 🐘 **PostgreSQL Protocol** - Compatible with standard PostgreSQL clients and drivers
- 🐬 **MySQL Protocol** - Full MySQL wire protocol support for MySQL clients
- 📊 **SQL Support** - SELECT queries with WHERE, ORDER BY, LIMIT, and basic JOINs
- 🔄 **Hot Reload** - Automatically reload data when YAML files change
- 🛠️ **Development Friendly** - Perfect for testing database integrations locally
- ⚡ **Lightweight** - Minimal resource usage, fast startup

## Installation

### From Crates.io
```bash
cargo install yamlbase
```

### From Source
```bash
git clone https://github.com/rvben/yamlbase
cd yamlbase
cargo install --path .
```

### Using Docker
```bash
docker run -p 5432:5432 -v $(pwd)/database.yaml:/data/database.yaml ghcr.io/rvben/yamlbase
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
# PostgreSQL protocol (default)
yamlbase -f database.yaml

# MySQL protocol
yamlbase -f database.yaml --protocol mysql
```

3. Connect with your preferred SQL client:

**PostgreSQL:**
```bash
# Using psql
psql -h localhost -p 5432 -U admin -d test_db
# Password: password (default)
```

**MySQL:**
```bash
# Using mysql client
mysql -h 127.0.0.1 -P 3306 -u admin -ppassword test_db
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
  -p, --port <PORT>          Port to listen on (default: 5432 for postgres, 3306 for mysql)
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

### Authentication

You can specify authentication credentials directly in the YAML file:

```yaml
database:
  name: "my_db"
  auth:
    username: "dbuser"
    password: "dbpassword"

tables:
  # ... your tables
```

When auth is specified in the YAML file, it overrides command-line arguments. This is useful for:
- Different credentials per database file
- Keeping credentials with the test data
- Simplifying connection strings


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
- `LEFT JOIN` with proper NULL handling
- `CROSS JOIN` for Cartesian products
- Window functions:
  - `ROW_NUMBER()` - Sequential row numbering
  - `RANK()` - Ranking with ties
  - `PARTITION BY` clause for grouping
- Common Table Expressions (CTEs):
  - Basic CTEs with `WITH` clause
  - CTE cross-references (CTEs referencing other CTEs)
  - CTEs in `CROSS JOIN` operations
  - `UNION ALL` with CTE results
- `DISTINCT` and `DISTINCT ON` (PostgreSQL-specific):
  - Standard `DISTINCT` for unique rows
  - `DISTINCT ON` for keeping first row per unique column combination
  - Supports expressions in `DISTINCT ON` including `EXTRACT` and comparisons

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

-- LEFT JOIN
SELECT u.name, o.order_id
FROM users u
LEFT JOIN orders o ON u.id = o.user_id;

-- Window functions
SELECT name, 
       ROW_NUMBER() OVER (ORDER BY created_at) as row_num,
       RANK() OVER (PARTITION BY department ORDER BY salary DESC) as dept_rank
FROM users;

-- Common Table Expressions (CTEs)
WITH active_users AS (
    SELECT * FROM users WHERE is_active = true
),
user_orders AS (
    SELECT u.*, o.total_amount
    FROM active_users u
    INNER JOIN orders o ON u.id = o.user_id
)
SELECT * FROM user_orders WHERE total_amount > 100;

-- CTE with CROSS JOIN for date range filtering
WITH DateRange AS (
    SELECT '2025-01-01' AS start_date, '2025-01-31' AS end_date
),
FilteredOrders AS (
    SELECT o.*
    FROM orders o
    CROSS JOIN DateRange d
    WHERE o.order_date BETWEEN d.start_date AND d.end_date
)
SELECT COUNT(*) FROM FilteredOrders;

-- Multiple CTEs with UNION ALL
WITH NewCustomers AS (
    SELECT customer_id, 'new' as type FROM customers WHERE created_at >= '2025-01-01'
),
VipCustomers AS (
    SELECT customer_id, 'vip' as type FROM customers WHERE total_purchases > 10000
)
SELECT * FROM NewCustomers
UNION ALL
SELECT * FROM VipCustomers;

-- DISTINCT ON to get one employee per department (highest paid)
SELECT DISTINCT ON (department) 
    department, name, salary
FROM employees
ORDER BY department, salary DESC;
```

### Not Yet Supported

- `INSERT`, `UPDATE`, `DELETE` operations (by design - read-only)
- Aggregate functions (`COUNT`, `SUM`, `AVG`, etc.)
- `GROUP BY` and `HAVING`
- Subqueries
- Advanced window functions (`DENSE_RANK`, `LAG`, `LEAD`, etc.)
- Transactions (commands accepted but not enforced)

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

**PostgreSQL:**
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

**MySQL:**
```python
import mysql.connector

conn = mysql.connector.connect(
    host="127.0.0.1",
    port=3306,
    database="test_db",
    user="admin",
    password="password"
)

cur = conn.cursor()
cur.execute("SELECT * FROM users WHERE is_active = true")
users = cur.fetchall()
```

### Node.js

**PostgreSQL:**
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

**MySQL:**
```javascript
const mysql = require('mysql2');

const connection = mysql.createConnection({
  host: '127.0.0.1',
  port: 3306,
  user: 'admin',
  password: 'password',
  database: 'test_db'
});

connection.query('SELECT * FROM users', (err, results) => {
  console.log(results);
});
```

### Go

**PostgreSQL:**
```go
import (
    "database/sql"
    _ "github.com/lib/pq"
)

db, err := sql.Open("postgres",
    "host=localhost port=5432 user=admin password=password dbname=test_db sslmode=disable")

rows, err := db.Query("SELECT name, email FROM users")
```

**MySQL:**
```go
import (
    "database/sql"
    _ "github.com/go-sql-driver/mysql"
)

db, err := sql.Open("mysql", "admin:password@tcp(127.0.0.1:3306)/test_db")

rows, err := db.Query("SELECT name, email FROM users")
```

### Python with SQLAlchemy

Yamlbase fully supports SQLAlchemy for both PostgreSQL and MySQL protocols:

**PostgreSQL:**
```python
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

# Create engine
engine = create_engine('postgresql+psycopg2://admin:password@localhost:5432/test_db')

# For metadata reflection
from sqlalchemy import MetaData, Table
metadata = MetaData()
users_table = Table('users', metadata, autoload_with=engine)

# Direct queries
with engine.connect() as conn:
    result = conn.execute("SELECT * FROM users WHERE is_active = true")
    users = result.fetchall()
```

**MySQL:**
```python
from sqlalchemy import create_engine

# Create engine with PyMySQL driver
engine = create_engine('mysql+pymysql://admin:password@127.0.0.1:3306/test_db')

# Query execution
with engine.connect() as conn:
    result = conn.execute("SELECT * FROM users WHERE is_active = true")
    users = result.fetchall()

# Note: Yamlbase handles SQLAlchemy's transaction commands (BEGIN, COMMIT, ROLLBACK)
# transparently, even though actual transaction support is not implemented.
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
- SQL Server protocol not yet implemented

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

### Development

```bash
# Run tests
make test

# Run with hot-reload
make run

# Run benchmarks
make bench

# Run all CI checks
make ci
```

## License

Licensed under either of

 * Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
 * MIT license ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)

at your option.

## Acknowledgments

Yamlbase is inspired by the need for simple, lightweight database solutions for testing and development. Special thanks to all contributors and the Rust community.
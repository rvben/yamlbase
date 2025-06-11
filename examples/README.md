# yamlbase Examples

This directory contains example YAML database definitions and integration examples for various programming languages.

## YAML Database Examples

### 1. sample_database.yaml
A comprehensive e-commerce database example featuring:
- Users table with authentication information
- Products catalog with pricing and inventory
- Orders and order items for transaction tracking
- Demonstrates relationships between tables

### 2. blog_database.yaml
A blog/CMS database example featuring:
- Authors with profile information
- Blog posts with publishing workflow
- Categories with hierarchical structure
- Comments with threading support
- Tags for content organization
- Many-to-many relationships (posts-categories, posts-tags)

### 3. minimal_database.yaml
A minimal example for quick testing:
- Single table with basic columns
- Simple data structure
- Perfect for initial testing

### 4. advanced_types.yaml
Demonstrates all supported data types:
- UUID primary keys
- JSON/JSONB columns for metadata
- Various numeric types (DECIMAL, FLOAT, DOUBLE)
- Date and time types (DATE, TIME, TIMESTAMP)
- Boolean and text types
- Complex business logic with employee and project tracking

## Integration Examples

### Python (python_integration.py)
Demonstrates:
- PostgreSQL connection using psycopg2
- MySQL connection using mysql-connector-python
- Basic queries and joins
- Error handling
- Connection pooling

**Requirements:**
```bash
pip install psycopg2-binary mysql-connector-python
```

**Usage:**
```bash
# Start yamlbase first
yamlbase -f sample_database.yaml

# Run the example
python python_integration.py
```

### Node.js (nodejs_integration.js)
Demonstrates:
- PostgreSQL connection using pg module
- MySQL connection using mysql2
- Async/await patterns
- Connection pooling
- Express.js integration example

**Requirements:**
```bash
npm install pg mysql2
```

**Usage:**
```bash
# Start yamlbase first
yamlbase -f sample_database.yaml

# Run the example
node nodejs_integration.js
```

### Go (go_integration.go)
Demonstrates:
- PostgreSQL connection using lib/pq
- MySQL connection using go-sql-driver/mysql
- Prepared statements
- Connection pooling
- Concurrent queries
- Error handling with sql.ErrNoRows

**Requirements:**
```bash
go get github.com/lib/pq
go get github.com/go-sql-driver/mysql
```

**Usage:**
```bash
# Start yamlbase first
yamlbase -f sample_database.yaml

# Run the example
go run go_integration.go
```

## Quick Start

1. Start yamlbase with one of the example databases:
```bash
# PostgreSQL protocol (default)
yamlbase -f examples/sample_database.yaml

# MySQL protocol
yamlbase -f examples/sample_database.yaml --protocol mysql

# With hot-reload for development
yamlbase -f examples/blog_database.yaml --hot-reload -v
```

2. Connect with SQL clients:
```bash
# PostgreSQL
psql -h localhost -p 5432 -U admin -d test_db

# MySQL
mysql -h 127.0.0.1 -P 3306 -u admin -ppassword test_db
```

3. Run example queries:
```sql
-- Get all active users
SELECT * FROM users WHERE is_active = true;

-- Get orders with user information
SELECT u.username, o.id, o.total_amount, o.status
FROM users u, orders o
WHERE u.id = o.user_id
ORDER BY o.order_date DESC;

-- Get products by category
SELECT name, price, stock_quantity
FROM products
WHERE category = 'Electronics'
ORDER BY price DESC;
```

## Testing Different Scenarios

### Testing with Blog Database
```bash
yamlbase -f examples/blog_database.yaml

# Example queries
SELECT p.title, a.display_name, p.view_count
FROM posts p, authors a
WHERE p.author_id = a.id
  AND p.status = 'published'
ORDER BY p.view_count DESC;
```

### Testing with Advanced Types
```bash
yamlbase -f examples/advanced_types.yaml

# Example queries
SELECT employee_number, first_name, last_name, metadata->>'skills' as skills
FROM employees
WHERE is_active = true;
```

### Performance Testing
```bash
# Run with verbose logging
yamlbase -f examples/sample_database.yaml -v

# In another terminal, run concurrent connections
for i in {1..10}; do
  psql -h localhost -p 5432 -U admin -d test_db \
    -c "SELECT * FROM users" &
done
```

## Docker Usage

You can also run these examples with Docker:

```bash
# Build the image
docker build -t yamlbase .

# Run with PostgreSQL protocol
docker run -p 5432:5432 \
  -v $(pwd)/examples/sample_database.yaml:/data/database.yaml \
  yamlbase

# Run with MySQL protocol
docker run -p 3306:3306 \
  -v $(pwd)/examples/sample_database.yaml:/data/database.yaml \
  yamlbase --protocol mysql
```

## Creating Your Own Examples

To create your own YAML database:

1. Define the database name
2. Create tables with columns and constraints
3. Add sample data
4. Test with yamlbase

Example template:
```yaml
database:
  name: "my_db"

tables:
  my_table:
    columns:
      id: "INTEGER PRIMARY KEY"
      name: "VARCHAR(100) NOT NULL"
      created_at: "TIMESTAMP DEFAULT CURRENT_TIMESTAMP"
    data:
      - id: 1
        name: "Example Item"
```

## Troubleshooting

- **Connection refused**: Make sure yamlbase is running on the correct port
- **Authentication failed**: Default credentials are admin/password
- **Table not found**: Check that your YAML file loaded correctly (use -v flag)
- **SQL syntax error**: yamlbase supports a subset of SQL - see main README for details
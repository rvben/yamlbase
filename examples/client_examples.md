# YamlBase Client Examples

This document shows how to connect to YamlBase using various database clients with the new authentication and protocol features.

## MySQL 8.0+ with caching_sha2_password

### Python (mysql-connector-python)
```python
import mysql.connector

# Connect to YamlBase with MySQL 8.0+ client
# The client will automatically use caching_sha2_password
conn = mysql.connector.connect(
    host='localhost',
    port=3306,
    user='myapp_user',
    password='secure_password_123',
    database='myapp_db',
    auth_plugin='caching_sha2_password'  # Default in MySQL 8.0+
)

cursor = conn.cursor()
cursor.execute("SELECT * FROM users WHERE is_active = TRUE")
for row in cursor.fetchall():
    print(row)

# Use prepared statements
cursor = conn.cursor(prepared=True)
query = "SELECT * FROM products WHERE price < ? AND category = ?"
cursor.execute(query, (50.00, 'Electronics'))
for row in cursor.fetchall():
    print(row)

cursor.close()
conn.close()
```

### Node.js (mysql2)
```javascript
const mysql = require('mysql2/promise');

async function example() {
    // mysql2 supports caching_sha2_password by default
    const connection = await mysql.createConnection({
        host: 'localhost',
        port: 3306,
        user: 'myapp_user',
        password: 'secure_password_123',
        database: 'myapp_db'
    });

    // Simple query
    const [rows] = await connection.execute(
        'SELECT * FROM users WHERE username = ?',
        ['alice']
    );
    console.log(rows);

    // System variables work correctly
    const [version] = await connection.query('SELECT @@version');
    console.log('Server version:', version[0]['@@version']);

    await connection.end();
}

example().catch(console.error);
```

### Java (MySQL Connector/J 8.0+)
```java
import java.sql.*;

public class YamlBaseExample {
    public static void main(String[] args) throws SQLException {
        // MySQL Connector/J 8.0+ uses caching_sha2_password by default
        String url = "jdbc:mysql://localhost:3306/myapp_db";
        
        try (Connection conn = DriverManager.getConnection(url, "myapp_user", "secure_password_123")) {
            // Prepared statement
            String sql = "SELECT * FROM products WHERE price BETWEEN ? AND ?";
            try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
                pstmt.setDouble(1, 10.00);
                pstmt.setDouble(2, 100.00);
                
                try (ResultSet rs = pstmt.executeQuery()) {
                    while (rs.next()) {
                        System.out.println(rs.getString("name") + ": $" + rs.getDouble("price"));
                    }
                }
            }
        }
    }
}
```

## PostgreSQL with Extended Query Protocol

### Python (psycopg2)
```python
import psycopg2

# Connect to YamlBase PostgreSQL
conn = psycopg2.connect(
    host='localhost',
    port=5432,
    user='yamlbase',
    password='password',
    database='analytics_db'
)

cursor = conn.cursor()

# Prepared statements are automatically used with parameterized queries
cursor.execute(
    "SELECT * FROM events WHERE event_type = %s AND timestamp > %s",
    ('page_view', '2024-01-20 00:00:00')
)

for row in cursor.fetchall():
    print(row)

# The same prepared statement can be reused efficiently
cursor.execute(
    "SELECT * FROM events WHERE event_type = %s AND timestamp > %s",
    ('button_click', '2024-01-20 00:00:00')
)

cursor.close()
conn.close()
```

### Node.js (pg)
```javascript
const { Client } = require('pg');

async function example() {
    const client = new Client({
        host: 'localhost',
        port: 5432,
        user: 'yamlbase',
        password: 'password',
        database: 'analytics_db'
    });

    await client.connect();

    // Uses extended query protocol automatically with parameters
    const result = await client.query(
        'SELECT u.name, COUNT(e.id) as event_count ' +
        'FROM users u ' +
        'LEFT JOIN events e ON u.id = e.user_id ' +
        'WHERE u.country = $1 ' +
        'GROUP BY u.id, u.name',
        ['USA']
    );

    console.log(result.rows);

    // Explicitly prepare a statement for reuse
    const prepared = await client.query({
        name: 'get-user-events',
        text: 'SELECT * FROM events WHERE user_id = $1 ORDER BY timestamp',
        values: [101]
    });

    console.log('Events for user 101:', prepared.rows);

    await client.end();
}

example().catch(console.error);
```

### Rust (tokio-postgres)
```rust
use tokio_postgres::{NoTls, Error};

#[tokio::main]
async fn main() -> Result<(), Error> {
    // Connect to YamlBase
    let (client, connection) = tokio_postgres::connect(
        "host=localhost port=5432 user=yamlbase password=password dbname=analytics_db",
        NoTls,
    ).await?;

    // Spawn connection handler
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("Connection error: {}", e);
        }
    });

    // Prepare a statement - uses extended query protocol
    let stmt = client.prepare(
        "SELECT * FROM daily_metrics WHERE revenue > $1 AND active_users > $2"
    ).await?;

    // Execute with parameters - efficient due to index usage
    let rows = client.query(&stmt, &[&10000.0f64, &1000i32]).await?;

    for row in rows {
        let date: chrono::NaiveDate = row.get(1);
        let revenue: f64 = row.get(3);
        println!("Date: {}, Revenue: ${}", date, revenue);
    }

    // Reuse the same prepared statement
    let more_rows = client.query(&stmt, &[&15000.0f64, &1200i32]).await?;
    println!("Found {} more rows", more_rows.len());

    Ok(())
}
```

### Go (pgx)
```go
package main

import (
    "context"
    "fmt"
    "github.com/jackc/pgx/v5"
)

func main() {
    ctx := context.Background()
    
    conn, err := pgx.Connect(ctx, 
        "postgres://yamlbase:password@localhost:5432/analytics_db")
    if err != nil {
        panic(err)
    }
    defer conn.Close(ctx)

    // Prepare once, execute multiple times
    query := `
        SELECT e.event_type, COUNT(*) as count, AVG(e.duration_ms) as avg_duration
        FROM events e
        WHERE e.timestamp >= $1
        GROUP BY e.event_type
        ORDER BY count DESC
    `

    rows, err := conn.Query(ctx, query, "2024-01-20 00:00:00")
    if err != nil {
        panic(err)
    }
    defer rows.Close()

    for rows.Next() {
        var eventType string
        var count int
        var avgDuration *float64
        
        err := rows.Scan(&eventType, &count, &avgDuration)
        if err != nil {
            panic(err)
        }
        
        if avgDuration != nil {
            fmt.Printf("%s: %d events, avg duration: %.2fms\n", 
                eventType, count, *avgDuration)
        } else {
            fmt.Printf("%s: %d events, no duration data\n", 
                eventType, count)
        }
    }
}
```

## Performance Benefits

The new features provide significant performance improvements:

1. **MySQL caching_sha2_password**: Modern authentication that's secure and compatible with MySQL 8.0+ defaults
2. **PostgreSQL Extended Protocol**: 
   - Prepared statements are parsed once and executed many times
   - Parameter binding prevents SQL injection
   - Index optimization provides O(1) lookups for primary key queries
   - 5-10x performance improvement for indexed queries

## Testing the Examples

1. Start YamlBase with one of the example configurations:
   ```bash
   # For MySQL 8.0+ testing
   yamlbase -f examples/mysql_8_auth_example.yaml --protocol mysql

   # For PostgreSQL extended protocol testing  
   yamlbase -f examples/postgres_extended_example.yaml --protocol postgres
   ```

2. Run any of the client examples above to test the new features

3. Monitor the YamlBase logs with `--verbose` flag to see the authentication flow and query optimization in action
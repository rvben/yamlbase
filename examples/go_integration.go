package main

import (
	"database/sql"
	"fmt"
	"log"
	"time"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
)

// User represents a user record
type User struct {
	ID        int
	Username  string
	Email     string
	FullName  sql.NullString
	CreatedAt time.Time
	IsActive  bool
	Age       sql.NullInt32
}

// Product represents a product record
type Product struct {
	ID            int
	Name          string
	Description   sql.NullString
	Price         float64
	StockQuantity int
	Category      sql.NullString
}

// postgresExample demonstrates PostgreSQL client usage
func postgresExample() {
	fmt.Println("=== PostgreSQL Example ===\n")

	// Connect to yamlbase
	connStr := "host=localhost port=5432 user=admin password=password dbname=test_db sslmode=disable"
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		log.Fatal("Failed to connect:", err)
	}
	defer db.Close()

	// Test connection
	err = db.Ping()
	if err != nil {
		log.Fatal("Failed to ping:", err)
	}
	fmt.Println("Connected to yamlbase via PostgreSQL protocol")

	// 1. Simple SELECT
	fmt.Println("\n1. All users:")
	rows, err := db.Query("SELECT id, username, email, full_name, created_at, is_active, age FROM users")
	if err != nil {
		log.Fatal("Query failed:", err)
	}
	defer rows.Close()

	for rows.Next() {
		var user User
		err := rows.Scan(&user.ID, &user.Username, &user.Email, &user.FullName,
			&user.CreatedAt, &user.IsActive, &user.Age)
		if err != nil {
			log.Fatal("Scan failed:", err)
		}
		fmt.Printf("  - %s: %s (Active: %t)\n", user.Username, user.Email, user.IsActive)
	}

	// 2. SELECT with WHERE clause
	fmt.Println("\n2. Active users:")
	rows, err = db.Query("SELECT username, email FROM users WHERE is_active = $1", true)
	if err != nil {
		log.Fatal("Query failed:", err)
	}
	defer rows.Close()

	for rows.Next() {
		var username, email string
		err := rows.Scan(&username, &email)
		if err != nil {
			log.Fatal("Scan failed:", err)
		}
		fmt.Printf("  - %s: %s\n", username, email)
	}

	// 3. Prepared statement
	fmt.Println("\n3. User by ID (prepared statement):")
	stmt, err := db.Prepare("SELECT username, email FROM users WHERE id = $1")
	if err != nil {
		log.Fatal("Prepare failed:", err)
	}
	defer stmt.Close()

	var username, email string
	err = stmt.QueryRow(1).Scan(&username, &email)
	if err != nil {
		log.Fatal("QueryRow failed:", err)
	}
	fmt.Printf("  - User #1: %s (%s)\n", username, email)

	// 4. JOIN query
	fmt.Println("\n4. Orders with user info:")
	rows, err = db.Query(`
		SELECT u.username, o.id, o.total_amount, o.status
		FROM users u, orders o
		WHERE u.id = o.user_id
		ORDER BY o.order_date DESC
	`)
	if err != nil {
		log.Fatal("Query failed:", err)
	}
	defer rows.Close()

	for rows.Next() {
		var username, status string
		var orderID int
		var amount float64
		err := rows.Scan(&username, &orderID, &amount, &status)
		if err != nil {
			log.Fatal("Scan failed:", err)
		}
		fmt.Printf("  - %s: Order #%d - $%.2f (%s)\n", username, orderID, amount, status)
	}
}

// mysqlExample demonstrates MySQL client usage
func mysqlExample() {
	fmt.Println("\n\n=== MySQL Example ===\n")

	// Connect to yamlbase
	dsn := "admin:password@tcp(127.0.0.1:3306)/test_db"
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Fatal("Failed to connect:", err)
	}
	defer db.Close()

	// Test connection
	err = db.Ping()
	if err != nil {
		log.Fatal("Failed to ping:", err)
	}
	fmt.Println("Connected to yamlbase via MySQL protocol")

	// 1. Simple SELECT
	fmt.Println("\n1. All products:")
	rows, err := db.Query("SELECT id, name, price, stock_quantity FROM products")
	if err != nil {
		log.Fatal("Query failed:", err)
	}
	defer rows.Close()

	for rows.Next() {
		var id, stock int
		var name string
		var price float64
		err := rows.Scan(&id, &name, &price, &stock)
		if err != nil {
			log.Fatal("Scan failed:", err)
		}
		fmt.Printf("  - %s: $%.2f (%d in stock)\n", name, price, stock)
	}

	// 2. Products by category
	fmt.Println("\n2. Electronics products:")
	rows, err = db.Query("SELECT name, price FROM products WHERE category = ?", "Electronics")
	if err != nil {
		log.Fatal("Query failed:", err)
	}
	defer rows.Close()

	for rows.Next() {
		var name string
		var price float64
		err := rows.Scan(&name, &price)
		if err != nil {
			log.Fatal("Scan failed:", err)
		}
		fmt.Printf("  - %s: $%.2f\n", name, price)
	}
}

// transactionExample demonstrates transaction usage (note: yamlbase is read-only)
func transactionExample() {
	fmt.Println("\n\n=== Transaction Example ===\n")

	connStr := "host=localhost port=5432 user=admin password=password dbname=test_db sslmode=disable"
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		log.Fatal("Failed to connect:", err)
	}
	defer db.Close()

	// Start transaction
	tx, err := db.Begin()
	if err != nil {
		log.Fatal("Failed to begin transaction:", err)
	}

	// Execute queries in transaction
	var count int
	err = tx.QueryRow("SELECT COUNT(*) FROM users").Scan(&count)
	if err != nil {
		tx.Rollback()
		log.Fatal("Query failed:", err)
	}
	fmt.Printf("Total users: %d\n", count)

	err = tx.QueryRow("SELECT COUNT(*) FROM products").Scan(&count)
	if err != nil {
		tx.Rollback()
		log.Fatal("Query failed:", err)
	}
	fmt.Printf("Total products: %d\n", count)

	// Commit transaction
	err = tx.Commit()
	if err != nil {
		log.Fatal("Failed to commit:", err)
	}
	fmt.Println("Transaction completed successfully")
}

// connectionPoolExample demonstrates connection pooling
func connectionPoolExample() {
	fmt.Println("\n\n=== Connection Pool Example ===\n")

	connStr := "host=localhost port=5432 user=admin password=password dbname=test_db sslmode=disable"
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		log.Fatal("Failed to connect:", err)
	}
	defer db.Close()

	// Configure connection pool
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(time.Hour)

	fmt.Println("Connection pool configured:")
	fmt.Println("  - Max open connections: 10")
	fmt.Println("  - Max idle connections: 5")
	fmt.Println("  - Connection lifetime: 1 hour")

	// Execute concurrent queries
	fmt.Println("\nExecuting concurrent queries...")
	done := make(chan bool, 3)

	// Query 1
	go func() {
		var count int
		err := db.QueryRow("SELECT COUNT(*) FROM users").Scan(&count)
		if err != nil {
			log.Printf("Query 1 failed: %v", err)
		} else {
			fmt.Printf("  - Users count: %d\n", count)
		}
		done <- true
	}()

	// Query 2
	go func() {
		var count int
		err := db.QueryRow("SELECT COUNT(*) FROM products").Scan(&count)
		if err != nil {
			log.Printf("Query 2 failed: %v", err)
		} else {
			fmt.Printf("  - Products count: %d\n", count)
		}
		done <- true
	}()

	// Query 3
	go func() {
		var count int
		err := db.QueryRow("SELECT COUNT(*) FROM orders").Scan(&count)
		if err != nil {
			log.Printf("Query 3 failed: %v", err)
		} else {
			fmt.Printf("  - Orders count: %d\n", count)
		}
		done <- true
	}()

	// Wait for all queries to complete
	for i := 0; i < 3; i++ {
		<-done
	}

	// Check pool stats
	stats := db.Stats()
	fmt.Printf("\nConnection pool stats:\n")
	fmt.Printf("  - Open connections: %d\n", stats.OpenConnections)
	fmt.Printf("  - In use: %d\n", stats.InUse)
	fmt.Printf("  - Idle: %d\n", stats.Idle)
}

// errorHandlingExample demonstrates error handling
func errorHandlingExample() {
	fmt.Println("\n\n=== Error Handling Example ===\n")

	connStr := "host=localhost port=5432 user=admin password=password dbname=test_db sslmode=disable"
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		log.Fatal("Failed to connect:", err)
	}
	defer db.Close()

	// Try to query non-existent table
	_, err = db.Query("SELECT * FROM non_existent_table")
	if err != nil {
		fmt.Printf("Expected error for non-existent table: %v\n", err)
	}

	// Try invalid SQL
	_, err = db.Query("SELECT FROM WHERE")
	if err != nil {
		fmt.Printf("Expected error for invalid SQL: %v\n", err)
	}

	// Handle no rows
	var username string
	err = db.QueryRow("SELECT username FROM users WHERE id = 999").Scan(&username)
	if err == sql.ErrNoRows {
		fmt.Println("No user found with ID 999 (expected)")
	} else if err != nil {
		fmt.Printf("Unexpected error: %v\n", err)
	}
}

func main() {
	fmt.Println("yamlbase Go Integration Examples")
	fmt.Println("================================")
	fmt.Printf("Timestamp: %s\n", time.Now().Format(time.RFC3339))

	// Run examples
	postgresExample()
	mysqlExample()
	transactionExample()
	connectionPoolExample()
	errorHandlingExample()

	fmt.Println("\n✅ All examples completed successfully!")
}
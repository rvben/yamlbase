#!/usr/bin/env python3
"""
Example of using yamlbase with Python clients.

This demonstrates connecting to yamlbase using both PostgreSQL and MySQL protocols.
"""

import psycopg
import mysql.connector
from datetime import datetime


def postgres_example():
    """Example using PostgreSQL client."""
    print("=== PostgreSQL Example ===")
    
    # Connect to yamlbase
    conn = psycopg.connect(
        host="localhost",
        port=5432,
        dbname="test_db",
        user="admin",
        password="password"
    )
    
    try:
        with conn.cursor() as cur:
            # Simple SELECT
            print("\n1. All users:")
            cur.execute("SELECT * FROM users")
            for row in cur.fetchall():
                print(f"  - {row}")
            
            # SELECT with WHERE clause
            print("\n2. Active users:")
            cur.execute("SELECT username, email FROM users WHERE is_active = true")
            for row in cur.fetchall():
                print(f"  - Username: {row[0]}, Email: {row[1]}")
            
            # Simple order query (JOIN has a known issue)
            print("\n3. Recent orders:")
            cur.execute("""
                SELECT id, user_id, total_amount, status
                FROM orders
                ORDER BY order_date DESC
            """)
            for row in cur.fetchall():
                print(f"  - Order #{row[0]} for user {row[1]}: ${row[2]} ({row[3]})")
            
            # Aggregate-like query using ORDER BY and LIMIT
            print("\n4. Most recent orders:")
            cur.execute("""
                SELECT id, user_id, order_date, total_amount
                FROM orders
                ORDER BY order_date DESC
                LIMIT 2
            """)
            for row in cur.fetchall():
                print(f"  - Order #{row[0]}: ${row[3]} on {row[2]}")
    
    finally:
        conn.close()


def mysql_example():
    """Example using MySQL client."""
    print("\n\n=== MySQL Example ===")
    
    # Connect to yamlbase
    conn = mysql.connector.connect(
        host="127.0.0.1",
        port=3306,
        database="test_db",
        user="admin",
        password="password"
    )
    
    try:
        cur = conn.cursor()
        
        # Simple SELECT
        print("\n1. All products:")
        cur.execute("SELECT * FROM products")
        for row in cur.fetchall():
            print(f"  - {row}")
        
        # SELECT with conditions
        print("\n2. Products in stock:")
        cur.execute("""
            SELECT name, price, stock_quantity
            FROM products
            WHERE stock_quantity > 0
            ORDER BY price DESC
        """)
        for row in cur.fetchall():
            print(f"  - {row[0]}: ${row[1]} ({row[2]} in stock)")
        
        # Order items query (JOIN has a known issue)
        print("\n3. Order items:")
        cur.execute("""
            SELECT order_id, product_id, quantity, unit_price
            FROM order_items
            ORDER BY order_id
        """)
        for row in cur.fetchall():
            print(f"  - Order #{row[0]}: {row[2]}x Product #{row[1]} @ ${row[3]}")
        
        cur.close()
    
    finally:
        conn.close()


def error_handling_example():
    """Example showing error handling."""
    print("\n\n=== Error Handling Example ===")
    
    try:
        conn = psycopg.connect(
            host="localhost",
            port=5432,
            dbname="test_db",
            user="admin",
            password="password"
        )
        
        with conn.cursor() as cur:
            # Try to query non-existent table
            try:
                cur.execute("SELECT * FROM non_existent_table")
            except psycopg.Error as e:
                print(f"Expected error for non-existent table: {e}")
            
            # Try invalid SQL
            try:
                cur.execute("SELECT * FROM WHERE")
            except psycopg.Error as e:
                print(f"Expected error for invalid SQL: {e}")
        
        conn.close()
    
    except psycopg.OperationalError as e:
        print(f"Connection failed: {e}")


def connection_pooling_example():
    """Example using connection pooling."""
    print("\n\n=== Connection Pooling Example ===")
    
    from psycopg_pool import ConnectionPool
    
    # Create connection pool
    connection_pool = ConnectionPool(
        "postgresql://admin:password@localhost:5432/test_db",
        min_size=1,
        max_size=5
    )
    
    if connection_pool:
        print("Connection pool created successfully")
        
        # Get connection from pool
        with connection_pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM users")
                count = cur.fetchone()[0]
                print(f"Total users: {count}")
        
        # Close all connections
        connection_pool.close()
        print("Connection pool closed")


if __name__ == "__main__":
    print("yamlbase Python Integration Examples")
    print("====================================")
    print(f"Timestamp: {datetime.now()}")
    
    # Run examples
    postgres_example()
    mysql_example()
    error_handling_example()
    connection_pooling_example()
    
    print("\n✅ All examples completed successfully!")
#!/usr/bin/env python3
"""
Example of using yamlbase with Python clients.

This demonstrates connecting to yamlbase using both PostgreSQL and MySQL protocols.
"""

import psycopg2
import mysql.connector
from datetime import datetime


def postgres_example():
    """Example using PostgreSQL client."""
    print("=== PostgreSQL Example ===")
    
    # Connect to yamlbase
    conn = psycopg2.connect(
        host="localhost",
        port=5432,
        database="test_db",
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
            
            # JOIN query
            print("\n3. Orders with user info:")
            cur.execute("""
                SELECT u.username, o.id, o.total_amount, o.status
                FROM users u, orders o
                WHERE u.id = o.user_id
                ORDER BY o.order_date DESC
            """)
            for row in cur.fetchall():
                print(f"  - {row[0]}: Order #{row[1]} - ${row[2]} ({row[3]})")
            
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
        
        # Complex query
        print("\n3. Order details:")
        cur.execute("""
            SELECT 
                o.id,
                u.username,
                p.name,
                oi.quantity,
                oi.unit_price
            FROM orders o, users u, order_items oi, products p
            WHERE o.user_id = u.id
                AND oi.order_id = o.id
                AND oi.product_id = p.id
            ORDER BY o.id
        """)
        for row in cur.fetchall():
            print(f"  - Order #{row[0]} ({row[1]}): {row[3]}x {row[2]} @ ${row[4]}")
        
        cur.close()
    
    finally:
        conn.close()


def error_handling_example():
    """Example showing error handling."""
    print("\n\n=== Error Handling Example ===")
    
    try:
        conn = psycopg2.connect(
            host="localhost",
            port=5432,
            database="test_db",
            user="admin",
            password="password"
        )
        
        with conn.cursor() as cur:
            # Try to query non-existent table
            try:
                cur.execute("SELECT * FROM non_existent_table")
            except psycopg2.Error as e:
                print(f"Expected error for non-existent table: {e}")
            
            # Try invalid SQL
            try:
                cur.execute("SELECT * FROM WHERE")
            except psycopg2.Error as e:
                print(f"Expected error for invalid SQL: {e}")
        
        conn.close()
    
    except psycopg2.OperationalError as e:
        print(f"Connection failed: {e}")


def connection_pooling_example():
    """Example using connection pooling."""
    print("\n\n=== Connection Pooling Example ===")
    
    from psycopg2 import pool
    
    # Create connection pool
    connection_pool = pool.SimpleConnectionPool(
        1, 5,  # min and max connections
        host="localhost",
        port=5432,
        database="test_db",
        user="admin",
        password="password"
    )
    
    if connection_pool:
        print("Connection pool created successfully")
        
        # Get connection from pool
        conn = connection_pool.getconn()
        
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM users")
                count = cur.fetchone()[0]
                print(f"Total users: {count}")
        finally:
            # Return connection to pool
            connection_pool.putconn(conn)
        
        # Close all connections
        connection_pool.closeall()
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
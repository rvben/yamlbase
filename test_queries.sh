#!/bin/bash

# Test script for YamlBase using psql

echo "Testing YamlBase SQL queries..."
echo "================================"

# Database connection parameters
HOST="localhost"
PORT="5432"
USER="admin"
PASSWORD="password"
DB="test_db"

# Export password to avoid prompt
export PGPASSWORD=$PASSWORD

# Function to run a query
run_query() {
    local query="$1"
    local description="$2"
    
    echo ""
    echo "Test: $description"
    echo "Query: $query"
    echo "---"
    psql -h $HOST -p $PORT -U $USER -d $DB -c "$query" 2>&1
    echo ""
}

# Wait for server to be ready
echo "Waiting for server to be ready..."
sleep 2

# Run test queries
run_query "SELECT * FROM users;" "Select all users"

run_query "SELECT username, email FROM users WHERE is_active = true;" "Select active users"

run_query "SELECT * FROM products WHERE price < 100;" "Select products under $100"

run_query "SELECT * FROM orders WHERE status = 'completed';" "Select completed orders"

run_query "SELECT u.username, o.order_date, o.total_amount 
           FROM users u, orders o 
           WHERE u.id = o.user_id;" "Join users and orders"

run_query "SELECT * FROM users ORDER BY created_at DESC LIMIT 2;" "Select latest 2 users"

run_query "SELECT COUNT(*) FROM products;" "Count products"

echo "================================"
echo "Test queries completed!"
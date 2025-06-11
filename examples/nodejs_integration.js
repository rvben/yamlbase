#!/usr/bin/env node
/**
 * Example of using yamlbase with Node.js clients.
 * 
 * This demonstrates connecting to yamlbase using both PostgreSQL and MySQL protocols.
 * 
 * Prerequisites:
 *   npm install pg mysql2
 */

const { Client } = require('pg');
const mysql = require('mysql2/promise');

// PostgreSQL Example
async function postgresExample() {
    console.log('=== PostgreSQL Example ===\n');
    
    const client = new Client({
        host: 'localhost',
        port: 5432,
        database: 'test_db',
        user: 'admin',
        password: 'password',
    });
    
    try {
        await client.connect();
        console.log('Connected to yamlbase via PostgreSQL protocol');
        
        // Simple SELECT
        console.log('\n1. All users:');
        const usersResult = await client.query('SELECT * FROM users');
        usersResult.rows.forEach(row => {
            console.log(`  - ${row.username}: ${row.email} (Active: ${row.is_active})`);
        });
        
        // SELECT with WHERE clause
        console.log('\n2. Active users with age > 25:');
        const activeUsersResult = await client.query(
            'SELECT username, email, age FROM users WHERE is_active = true AND age > 25'
        );
        activeUsersResult.rows.forEach(row => {
            console.log(`  - ${row.username} (${row.age}): ${row.email}`);
        });
        
        // JOIN query
        console.log('\n3. Orders with product details:');
        const ordersResult = await client.query(`
            SELECT 
                o.id AS order_id,
                u.username,
                p.name AS product_name,
                oi.quantity,
                oi.unit_price
            FROM orders o, users u, order_items oi, products p
            WHERE o.user_id = u.id
                AND oi.order_id = o.id
                AND oi.product_id = p.id
            ORDER BY o.id
        `);
        ordersResult.rows.forEach(row => {
            console.log(`  - Order #${row.order_id} (${row.username}): ${row.quantity}x ${row.product_name} @ $${row.unit_price}`);
        });
        
        // Using prepared statements (parameterized queries)
        console.log('\n4. Products by category:');
        const category = 'Electronics';
        const categoryResult = await client.query(
            'SELECT name, price FROM products WHERE category = $1 ORDER BY price DESC',
            [category]
        );
        console.log(`Products in ${category}:`);
        categoryResult.rows.forEach(row => {
            console.log(`  - ${row.name}: $${row.price}`);
        });
        
    } catch (err) {
        console.error('PostgreSQL error:', err);
    } finally {
        await client.end();
    }
}

// MySQL Example
async function mysqlExample() {
    console.log('\n\n=== MySQL Example ===\n');
    
    const connection = await mysql.createConnection({
        host: '127.0.0.1',
        port: 3306,
        user: 'admin',
        password: 'password',
        database: 'test_db'
    });
    
    try {
        console.log('Connected to yamlbase via MySQL protocol');
        
        // Simple SELECT
        console.log('\n1. All products:');
        const [products] = await connection.execute('SELECT * FROM products');
        products.forEach(row => {
            console.log(`  - ${row.name}: $${row.price} (${row.stock_quantity} in stock)`);
        });
        
        // SELECT with conditions
        console.log('\n2. Recent orders:');
        const [orders] = await connection.execute(`
            SELECT id, user_id, order_date, total_amount, status
            FROM orders
            ORDER BY order_date DESC
            LIMIT 3
        `);
        orders.forEach(row => {
            console.log(`  - Order #${row.id}: $${row.total_amount} - ${row.status} (${row.order_date})`);
        });
        
        // Complex query with multiple joins
        console.log('\n3. Order summary:');
        const [orderSummary] = await connection.execute(`
            SELECT 
                o.id AS order_id,
                u.full_name AS customer,
                o.total_amount,
                o.status
            FROM orders o, users u
            WHERE o.user_id = u.id
            ORDER BY o.order_date DESC
        `);
        orderSummary.forEach(row => {
            console.log(`  - Order #${row.order_id}: ${row.customer} - $${row.total_amount} (${row.status})`);
        });
        
    } catch (err) {
        console.error('MySQL error:', err);
    } finally {
        await connection.end();
    }
}

// Connection Pool Example
async function connectionPoolExample() {
    console.log('\n\n=== Connection Pool Example (PostgreSQL) ===\n');
    
    const { Pool } = require('pg');
    
    const pool = new Pool({
        host: 'localhost',
        port: 5432,
        database: 'test_db',
        user: 'admin',
        password: 'password',
        max: 5, // maximum number of clients in the pool
        idleTimeoutMillis: 30000, // close idle clients after 30 seconds
    });
    
    try {
        // Execute multiple queries concurrently
        const queries = [
            pool.query('SELECT COUNT(*) AS count FROM users'),
            pool.query('SELECT COUNT(*) AS count FROM products'),
            pool.query('SELECT COUNT(*) AS count FROM orders'),
        ];
        
        const results = await Promise.all(queries);
        
        console.log('Table counts:');
        console.log(`  - Users: ${results[0].rows[0].count}`);
        console.log(`  - Products: ${results[1].rows[0].count}`);
        console.log(`  - Orders: ${results[2].rows[0].count}`);
        
    } catch (err) {
        console.error('Pool error:', err);
    } finally {
        await pool.end();
    }
}

// Error Handling Example
async function errorHandlingExample() {
    console.log('\n\n=== Error Handling Example ===\n');
    
    const client = new Client({
        host: 'localhost',
        port: 5432,
        database: 'test_db',
        user: 'admin',
        password: 'password',
    });
    
    try {
        await client.connect();
        
        // Try to query non-existent table
        try {
            await client.query('SELECT * FROM non_existent_table');
        } catch (err) {
            console.log('Expected error for non-existent table:', err.message);
        }
        
        // Try invalid SQL
        try {
            await client.query('SELECT FROM WHERE');
        } catch (err) {
            console.log('Expected error for invalid SQL:', err.message);
        }
        
    } finally {
        await client.end();
    }
}

// Async/Await with Express.js Example
function expressExample() {
    console.log('\n\n=== Express.js Integration Example ===\n');
    console.log('Example Express.js route:');
    console.log(`
const express = require('express');
const { Pool } = require('pg');

const app = express();
const pool = new Pool({
    host: 'localhost',
    port: 5432,
    database: 'test_db',
    user: 'admin',
    password: 'password',
});

// Get all users
app.get('/api/users', async (req, res) => {
    try {
        const result = await pool.query('SELECT * FROM users WHERE is_active = true');
        res.json(result.rows);
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

// Get user by ID
app.get('/api/users/:id', async (req, res) => {
    try {
        const { id } = req.params;
        const result = await pool.query('SELECT * FROM users WHERE id = $1', [id]);
        
        if (result.rows.length === 0) {
            return res.status(404).json({ error: 'User not found' });
        }
        
        res.json(result.rows[0]);
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

app.listen(3000, () => {
    console.log('Server running on http://localhost:3000');
});
`);
}

// Main function
async function main() {
    console.log('yamlbase Node.js Integration Examples');
    console.log('=====================================');
    console.log(`Timestamp: ${new Date().toISOString()}\n`);
    
    try {
        await postgresExample();
        await mysqlExample();
        await connectionPoolExample();
        await errorHandlingExample();
        expressExample();
        
        console.log('\n✅ All examples completed successfully!');
    } catch (err) {
        console.error('Fatal error:', err);
        process.exit(1);
    }
}

// Run the examples
main();
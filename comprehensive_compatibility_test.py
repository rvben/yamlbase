#!/usr/bin/env python3
"""
Comprehensive compatibility test with real PostgreSQL clients.
Tests various SQL features to identify remaining gaps.
"""
import psycopg2
import sys
from typing import List, Tuple

def test_sql_feature(cursor, name: str, query: str) -> Tuple[str, bool, str]:
    """Test a single SQL feature and return result."""
    try:
        cursor.execute(query)
        results = cursor.fetchall()
        return (name, True, f"SUCCESS - {len(results)} rows")
    except Exception as e:
        error_msg = str(e).replace('\n', ' ').strip()
        return (name, False, f"FAILED - {error_msg}")

def run_compatibility_tests():
    """Run comprehensive compatibility tests."""
    try:
        # Connect to yamlbase
        conn = psycopg2.connect(
            host="localhost",
            port=5432,
            user="admin",
            password="password",
            database="yamlbase"
        )
        
        cursor = conn.cursor()
        
        # Define test cases organized by category
        test_categories = {
            "Basic Queries": [
                ("Simple SELECT", "SELECT * FROM users LIMIT 1"),
                ("SELECT with WHERE", "SELECT username FROM users WHERE id = 1"),
                ("COUNT function", "SELECT COUNT(*) FROM users"),
                ("ORDER BY", "SELECT username FROM users ORDER BY username"),
                ("DISTINCT", "SELECT DISTINCT username FROM users"),
            ],
            
            "JOIN Operations": [
                ("INNER JOIN", "SELECT u.username, o.total_amount FROM users u INNER JOIN orders o ON u.id = o.user_id"),
                ("LEFT JOIN", "SELECT u.username, o.total_amount FROM users u LEFT JOIN orders o ON u.id = o.user_id"),
                ("RIGHT JOIN", "SELECT u.username, o.total_amount FROM users u RIGHT JOIN orders o ON u.id = o.user_id"),
                ("FULL OUTER JOIN", "SELECT u.username, o.total_amount FROM users u FULL OUTER JOIN orders o ON u.id = o.user_id"),
                ("CROSS JOIN", "SELECT u.username, p.name FROM users u CROSS JOIN products p LIMIT 5"),
                ("Self JOIN", "SELECT u1.username, u2.username FROM users u1 JOIN users u2 ON u1.id != u2.id"),
            ],
            
            "Aggregate Functions": [
                ("SUM", "SELECT SUM(total_amount) FROM orders"),
                ("AVG", "SELECT AVG(total_amount) FROM orders"),
                ("MAX", "SELECT MAX(total_amount) FROM orders"),
                ("MIN", "SELECT MIN(total_amount) FROM orders"),
                ("GROUP BY", "SELECT user_id, COUNT(*) FROM orders GROUP BY user_id"),
                ("HAVING", "SELECT user_id, COUNT(*) FROM orders GROUP BY user_id HAVING COUNT(*) > 1"),
            ],
            
            "Subqueries": [
                ("EXISTS subquery", "SELECT * FROM users WHERE EXISTS (SELECT 1 FROM orders WHERE orders.user_id = users.id)"),
                ("IN subquery", "SELECT * FROM users WHERE id IN (SELECT user_id FROM orders)"),
                ("Scalar subquery in SELECT", "SELECT username, (SELECT COUNT(*) FROM orders WHERE user_id = users.id) as order_count FROM users"),
                ("Scalar subquery in WHERE", "SELECT * FROM orders WHERE total_amount > (SELECT AVG(total_amount) FROM orders)"),
                ("NOT EXISTS", "SELECT * FROM users WHERE NOT EXISTS (SELECT 1 FROM orders WHERE orders.user_id = users.id)"),
            ],
            
            "Advanced Features": [
                ("CASE expression", "SELECT username, CASE WHEN id = 1 THEN 'First' ELSE 'Other' END FROM users"),
                ("COALESCE", "SELECT username, COALESCE(email, 'no-email') FROM users"),
                ("NULLIF", "SELECT username, NULLIF(username, 'admin') FROM users"),
                ("UNION", "SELECT username FROM users UNION SELECT name FROM products"),
                ("UNION ALL", "SELECT username FROM users UNION ALL SELECT name FROM products"),
            ],
            
            "Window Functions": [
                ("ROW_NUMBER", "SELECT username, ROW_NUMBER() OVER (ORDER BY id) FROM users"),
                ("RANK", "SELECT username, RANK() OVER (ORDER BY id) FROM users"),
                ("DENSE_RANK", "SELECT username, DENSE_RANK() OVER (ORDER BY id) FROM users"),
                ("LAG", "SELECT username, LAG(username) OVER (ORDER BY id) FROM users"),
                ("LEAD", "SELECT username, LEAD(username) OVER (ORDER BY id) FROM users"),
            ],
            
            "String Functions": [
                ("CONCAT", "SELECT CONCAT(username, '@domain.com') FROM users"),
                ("LENGTH", "SELECT username, LENGTH(username) FROM users"),
                ("UPPER", "SELECT UPPER(username) FROM users"),
                ("LOWER", "SELECT LOWER(username) FROM users"),
                ("SUBSTRING", "SELECT SUBSTRING(username, 1, 3) FROM users"),
            ],
            
            "Date/Time Functions": [
                ("NOW", "SELECT NOW()"),
                ("CURRENT_DATE", "SELECT CURRENT_DATE"),
                ("DATE_TRUNC", "SELECT DATE_TRUNC('day', created_at) FROM users"),
                ("EXTRACT", "SELECT EXTRACT(year FROM created_at) FROM users"),
                ("AGE", "SELECT username, AGE(created_at) FROM users"),
            ],
            
            "Math Functions": [
                ("ABS", "SELECT ABS(-42)"),
                ("ROUND", "SELECT ROUND(3.14159, 2)"),
                ("CEIL", "SELECT CEIL(3.14)"),
                ("FLOOR", "SELECT FLOOR(3.14)"),
                ("POWER", "SELECT POWER(2, 3)"),
            ],
            
            "CTE (Common Table Expressions)": [
                ("Simple CTE", "WITH user_stats AS (SELECT COUNT(*) as total FROM users) SELECT * FROM user_stats"),
                ("CTE with JOIN", "WITH order_summary AS (SELECT user_id, COUNT(*) as order_count FROM orders GROUP BY user_id) SELECT u.username, os.order_count FROM users u JOIN order_summary os ON u.id = os.user_id"),
                ("Recursive CTE", "WITH RECURSIVE t(n) AS (VALUES (1) UNION SELECT n+1 FROM t WHERE n < 3) SELECT * FROM t"),
            ],
            
            "Complex Queries": [
                ("Multiple JOINs", "SELECT u.username, o.total_amount, p.name FROM users u JOIN orders o ON u.id = o.user_id JOIN order_items oi ON o.id = oi.order_id JOIN products p ON oi.product_id = p.id"),
                ("Nested subqueries", "SELECT * FROM users WHERE id IN (SELECT user_id FROM orders WHERE total_amount > (SELECT AVG(total_amount) FROM orders))"),
                ("Complex GROUP BY", "SELECT u.username, COUNT(o.id) as orders, SUM(o.total_amount) as total FROM users u LEFT JOIN orders o ON u.id = o.user_id GROUP BY u.id, u.username"),
            ]
        }
        
        # Run all tests
        all_results = []
        category_stats = {}
        
        print("🧪 YAMLBASE POSTGRESQL COMPATIBILITY TEST")
        print("=" * 60)
        
        for category, tests in test_categories.items():
            print(f"\n📂 {category}")
            print("-" * 40)
            
            passed = 0
            total = len(tests)
            
            for test_name, query in tests:
                name, success, message = test_sql_feature(cursor, test_name, query)
                status = "✅" if success else "❌"
                print(f"  {status} {name:<25} {message}")
                
                if success:
                    passed += 1
                
                all_results.append((category, name, success, message))
            
            percentage = (passed / total) * 100
            category_stats[category] = (passed, total, percentage)
            print(f"  📊 Category Score: {passed}/{total} ({percentage:.1f}%)")
        
        # Overall summary
        total_passed = sum(stats[0] for stats in category_stats.values())
        total_tests = sum(stats[1] for stats in category_stats.values())
        overall_percentage = (total_passed / total_tests) * 100
        
        print("\n" + "=" * 60)
        print("📊 OVERALL COMPATIBILITY SUMMARY")
        print("=" * 60)
        print(f"Total Tests: {total_tests}")
        print(f"Passed: {total_passed}")
        print(f"Failed: {total_tests - total_passed}")
        print(f"Success Rate: {overall_percentage:.1f}%")
        
        print("\n📈 CATEGORY BREAKDOWN:")
        for category, (passed, total, percentage) in category_stats.items():
            status = "🟢" if percentage >= 80 else "🟡" if percentage >= 50 else "🔴"
            print(f"  {status} {category:<30} {passed:>2}/{total:<2} ({percentage:>5.1f}%)")
        
        # Identify priority areas for improvement
        print("\n🎯 PRIORITY IMPROVEMENT AREAS:")
        priority_categories = sorted(category_stats.items(), key=lambda x: x[1][2])
        
        for category, (passed, total, percentage) in priority_categories[:3]:
            if percentage < 80:
                failed_tests = [result for result in all_results 
                              if result[0] == category and not result[2]]
                print(f"\n🔧 {category} ({percentage:.1f}% compatibility):")
                for _, test_name, _, error_msg in failed_tests[:3]:  # Show top 3 failures
                    print(f"  • {test_name}: {error_msg}")
                if len(failed_tests) > 3:
                    print(f"  • ... and {len(failed_tests) - 3} more failures")
        
        cursor.close()
        conn.close()
        
        return overall_percentage
        
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        return 0

if __name__ == "__main__":
    compatibility_score = run_compatibility_tests()
    print(f"\n🏆 Final Compatibility Score: {compatibility_score:.1f}%")
    
    if compatibility_score >= 80:
        print("🎉 Excellent PostgreSQL compatibility!")
    elif compatibility_score >= 60:
        print("👍 Good PostgreSQL compatibility with room for improvement")
    else:
        print("⚠️  Limited PostgreSQL compatibility - significant gaps remain")
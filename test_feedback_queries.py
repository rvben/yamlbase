#!/usr/bin/env python3
"""
Test the exact queries mentioned in the feedback to verify our implementation.
"""

import psycopg2
import sys

def test_feedback_queries():
    """Test the exact queries mentioned in the user feedback."""
    print("Testing YamlBase Enhanced SQL Features...")
    
    try:
        # Connect to yamlbase
        conn = psycopg2.connect(
            host="localhost",
            port=5434,
            dbname="sql_features_test",
            user="admin",
            password="password123"
        )
        
        with conn.cursor() as cur:
            # Test 1: UPPER with string literal (reported as working)
            print("\n1. Testing UPPER with string literal:")
            cur.execute("SELECT UPPER('test') AS test_upper")
            result = cur.fetchone()
            print(f"   Result: {result[0]} (Expected: TEST)")
            assert result[0] == "TEST", "UPPER with literal failed"
            
            # Test 2: UPPER with column identifier (reported as failing)
            print("\n2. Testing UPPER with column identifier:")
            try:
                cur.execute("SELECT UPPER(FIRST_NAME) FROM EMPLOYEES")
                results = cur.fetchall()
                print(f"   Success! Found {len(results)} rows")
                for i, row in enumerate(results[:3]):  # Show first 3
                    print(f"   Row {i}: {row[0]}")
            except Exception as e:
                print(f"   FAILED: {e}")
                
            # Test 3: Complex JOIN (reported as failing)
            print("\n3. Testing complex JOIN:")
            try:
                cur.execute("""
                    SELECT p.PROJECT_ID, r.FIRST_NAME 
                    FROM PROJECTS p 
                    LEFT JOIN EMPLOYEES r ON p.MANAGER_ID = r.EMPLOYEE_ID
                """)
                results = cur.fetchall()
                print(f"   Success! Found {len(results)} rows")
                for i, row in enumerate(results[:3]):  # Show first 3
                    print(f"   Row {i}: Project {row[0]}, Manager: {row[1]}")
            except Exception as e:
                print(f"   FAILED: {e}")
                
            # Test 4: UPPER in JOIN condition (reported as failing)
            print("\n4. Testing UPPER in JOIN condition:")
            try:
                cur.execute("""
                    SELECT a.EMPLOYEE_ID, a.PROJECT_ROLE, r.FIRST_NAME, r.LAST_NAME
                    FROM PROJECT_ASSIGNMENTS a
                    LEFT JOIN EMPLOYEES r ON UPPER(a.EMPLOYEE_ID) = UPPER(r.EMPLOYEE_ID)
                    WHERE a.PROJECT_ID = 'PRJ001'
                """)
                results = cur.fetchall()
                print(f"   Success! Found {len(results)} rows")
                for row in results:
                    print(f"   Employee {row[0]}: {row[2]} {row[3]} ({row[1]})")
            except Exception as e:
                print(f"   FAILED: {e}")
                
            # Test 5: LOWER function
            print("\n5. Testing LOWER function:")
            try:
                cur.execute("SELECT EMPLOYEE_ID, LOWER(EMAIL) FROM EMPLOYEES WHERE DEPARTMENT = 'Engineering'")
                results = cur.fetchall()
                print(f"   Success! Found {len(results)} rows")
                for row in results[:2]:
                    print(f"   {row[0]}: {row[1]}")
            except Exception as e:
                print(f"   FAILED: {e}")
                
            # Test 6: TRIM function
            print("\n6. Testing TRIM function:")
            try:
                cur.execute("SELECT TRIM(FIRST_NAME), TRIM(LAST_NAME) FROM EMPLOYEES WHERE EMPLOYEE_ID = 'WBI001'")
                result = cur.fetchone()
                print(f"   Success! {result[0]} {result[1]}")
            except Exception as e:
                print(f"   FAILED: {e}")
                
            # Test 7: Nested functions
            print("\n7. Testing nested functions:")
            try:
                cur.execute("SELECT UPPER(TRIM(FIRST_NAME)) FROM EMPLOYEES WHERE EMPLOYEE_ID = 'WBI001'")
                result = cur.fetchone()
                print(f"   Success! {result[0]}")
            except Exception as e:
                print(f"   FAILED: {e}")
                
            # Test 8: CHAR type support
            print("\n8. Testing CHAR type support:")
            try:
                cur.execute("SELECT EMPLOYEE_ID, ACTIVE_FLAG FROM EMPLOYEES WHERE ACTIVE_FLAG = 'Y'")
                results = cur.fetchall()
                print(f"   Success! Found {len(results)} active employees")
            except Exception as e:
                print(f"   FAILED: {e}")
                
        conn.close()
        print("\n✅ Test completed!")
        
    except psycopg2.OperationalError as e:
        print(f"❌ Connection failed: {e}")
        print("\nMake sure YamlBase is running with:")
        print("cargo run -- -f examples/sql_features_test.yaml --protocol postgres -p 5434")
        sys.exit(1)

if __name__ == "__main__":
    test_feedback_queries()
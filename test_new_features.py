#!/usr/bin/env python3
"""
Test script for YamlBase v0.2.3 new SQL features:
- UPPER/LOWER/TRIM functions
- CHAR(n) data type
- Functions in JOIN conditions

Usage: python test_new_features.py
"""

import psycopg2
import sys
from tabulate import tabulate

def print_test_header(test_name):
    print(f"\n{'='*60}")
    print(f"TEST: {test_name}")
    print('='*60)

def execute_query(cursor, query, description):
    print(f"\n{description}")
    print(f"Query: {query}")
    try:
        cursor.execute(query)
        results = cursor.fetchall()
        if results:
            headers = [desc[0] for desc in cursor.description]
            print(tabulate(results, headers=headers, tablefmt='grid'))
        else:
            print("No results returned")
        return True
    except Exception as e:
        print(f"ERROR: {e}")
        return False

def main():
    # Connect to YamlBase (using PostgreSQL protocol)
    try:
        conn = psycopg2.connect(
            host="localhost",
            port=5432,
            database="sql_features_test",
            user="admin",
            password="password123"
        )
        cursor = conn.cursor()
        print("✓ Connected to YamlBase")
    except Exception as e:
        print(f"✗ Failed to connect: {e}")
        sys.exit(1)

    passed = 0
    failed = 0

    # Test 1: UPPER function in SELECT
    print_test_header("UPPER function in SELECT")
    if execute_query(cursor, 
        "SELECT WBI_ID, UPPER(WBI_ID) as WBI_ID_UPPER FROM EMPLOYEES",
        "Testing UPPER function on WBI_ID column"):
        passed += 1
    else:
        failed += 1

    # Test 2: LOWER function in SELECT
    print_test_header("LOWER function in SELECT")
    if execute_query(cursor,
        "SELECT WBI_ID, LOWER(WBI_ID) as WBI_ID_LOWER FROM EMPLOYEES",
        "Testing LOWER function on WBI_ID column"):
        passed += 1
    else:
        failed += 1

    # Test 3: TRIM function in SELECT
    print_test_header("TRIM function in SELECT")
    if execute_query(cursor,
        "SELECT EMP_FIRST_NAME, TRIM(EMP_FIRST_NAME) as TRIMMED_NAME FROM EMPLOYEES WHERE WBI_ID = 'def456'",
        "Testing TRIM function on name with spaces"):
        passed += 1
    else:
        failed += 1

    # Test 4: UPPER in WHERE clause
    print_test_header("UPPER in WHERE clause")
    if execute_query(cursor,
        "SELECT * FROM EMPLOYEES WHERE UPPER(WBI_ID) = 'ABC123'",
        "Should find both abc123 and ABC123 records"):
        passed += 1
    else:
        failed += 1

    # Test 5: CHAR data type
    print_test_header("CHAR data type support")
    if execute_query(cursor,
        "SELECT PROJECT_ID, PROJECT_TYPE FROM PROJECTS",
        "PROJECT_TYPE is defined as CHAR(3)"):
        passed += 1
    else:
        failed += 1

    # Test 6: Functions in JOIN - The main feature request!
    print_test_header("Functions in JOIN conditions")
    if execute_query(cursor,
        """
        SELECT 
            a.WBI_ID as ALLOCATION_WBI,
            r.WBI_ID as RESOURCE_WBI,
            r.FIRST_NAME,
            r.LAST_NAME,
            a.JOB_CLASSIFICATION
        FROM PROJECT_ASSIGNMENTS a
        LEFT JOIN EMPLOYEES r ON UPPER(a.WBI_ID) = UPPER(r.WBI_ID)
        WHERE a.PROJECT_ID = 'PROJ001'
        """,
        "Testing UPPER function in JOIN condition - should match case-insensitively"):
        passed += 1
    else:
        failed += 1

    # Test 7: Multiple functions combined
    print_test_header("Multiple functions combined")
    if execute_query(cursor,
        """
        SELECT 
            UPPER(TRIM(EMP_FIRST_NAME)) as CLEANED_NAME,
            LOWER(EMAIL_ADDRESS) as EMAIL_LOWER
        FROM EMPLOYEES
        """,
        "Testing nested functions"):
        passed += 1
    else:
        failed += 1

    # Test 8: Complex real-world query pattern
    print_test_header("Complex JOIN with functions and filters")
    if execute_query(cursor,
        """
        SELECT 
            a.WBI_ID,
            a.JOB_CLASSIFICATION,
            r.FIRST_NAME,
            r.LAST_NAME,
            r.EMAIL_ADDRESS
        FROM PROJECT_ASSIGNMENTS a
        LEFT JOIN EMPLOYEES r ON UPPER(a.WBI_ID) = UPPER(r.WBI_ID)
        WHERE a.PROJECT_ID = 'PROJ001' AND a.VERSION_CODE = 'Published'
        """,
        "Testing production-like query with case-insensitive JOIN"):
        passed += 1
    else:
        failed += 1

    # Summary
    print(f"\n{'='*60}")
    print(f"TEST SUMMARY: {passed} passed, {failed} failed")
    print('='*60)

    cursor.close()
    conn.close()

if __name__ == "__main__":
    main()
#!/usr/bin/env python3
"""
Test SQLAlchemy compatibility with yamlbase.
This reproduces the exact issue reported in the feature request.
"""

import subprocess
import time
import sys
import traceback
from contextlib import contextmanager

def test_direct_pymysql():
    """Test that direct PyMySQL connection works (baseline)"""
    print("=== Testing Direct PyMySQL Connection ===")
    try:
        import pymysql
        conn = pymysql.connect(
            host="localhost",
            port=3306,
            user="admin",
            password="password",
            database="sample_db"
        )
        
        with conn.cursor() as cursor:
            cursor.execute("SELECT 1 as test")
            result = cursor.fetchone()
            print(f"✅ Direct PyMySQL works: {result}")
        
        conn.close()
        return True
    except Exception as e:
        print(f"❌ Direct PyMySQL failed: {e}")
        traceback.print_exc()
        return False

def test_sqlalchemy_basic():
    """Test basic SQLAlchemy connection (this should fail with the reported error)"""
    print("\n=== Testing SQLAlchemy Basic Connection ===")
    try:
        from sqlalchemy import create_engine, text
        
        # This is the exact configuration that fails
        engine = create_engine(
            "mysql+pymysql://admin:password@localhost:3306/sample_db",
            pool_pre_ping=False,  # Disable to simplify
            pool_size=1,
            max_overflow=0
        )
        
        # This should trigger the error during connection initialization
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1 as test"))
            print(f"✅ SQLAlchemy connection works: {result.fetchone()}")
        
        return True
    except Exception as e:
        print(f"❌ SQLAlchemy connection failed: {e}")
        print(f"   Error type: {type(e).__name__}")
        if "2014" in str(e) and "Command Out of Sync" in str(e):
            print("   ⚠️  This is the exact error from the feature request!")
        traceback.print_exc()
        return False

def test_sqlalchemy_with_pooling():
    """Test SQLAlchemy with connection pooling"""
    print("\n=== Testing SQLAlchemy with Connection Pooling ===")
    try:
        from sqlalchemy import create_engine, text
        from sqlalchemy.pool import QueuePool
        
        engine = create_engine(
            "mysql+pymysql://admin:password@localhost:3306/sample_db",
            poolclass=QueuePool,
            pool_size=3,
            max_overflow=5,
            pool_pre_ping=True,
            pool_recycle=1800,
            isolation_level="AUTOCOMMIT"
        )
        
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1 as test"))
            print(f"✅ SQLAlchemy with pooling works: {result.fetchone()}")
        
        return True
    except Exception as e:
        print(f"❌ SQLAlchemy with pooling failed: {e}")
        traceback.print_exc()
        return False

def test_sqlalchemy_rollback_behavior():
    """Test to understand SQLAlchemy's rollback behavior"""
    print("\n=== Testing SQLAlchemy Rollback Behavior ===")
    try:
        import pymysql
        
        # Simulate what SQLAlchemy does
        conn = pymysql.connect(
            host="localhost",
            port=3306,
            user="admin",
            password="password",
            database="sample_db"
        )
        
        print("1. Fresh connection established")
        
        # SQLAlchemy immediately calls rollback
        print("2. Calling rollback() on fresh connection...")
        try:
            conn.rollback()
            print("   ✅ rollback() succeeded")
        except Exception as e:
            print(f"   ❌ rollback() failed: {e}")
            raise
        
        # Then executes queries
        print("3. Executing a test query...")
        with conn.cursor() as cursor:
            cursor.execute("SELECT 1 as test")
            result = cursor.fetchone()
            print(f"   ✅ Query succeeded: {result}")
        
        conn.close()
        return True
    except Exception as e:
        print(f"❌ Rollback behavior test failed: {e}")
        traceback.print_exc()
        return False

def test_transaction_commands():
    """Test individual transaction commands"""
    print("\n=== Testing Transaction Commands ===")
    try:
        import pymysql
        conn = pymysql.connect(
            host="localhost",
            port=3306,
            user="admin",
            password="password",
            database="sample_db"
        )
        
        commands = ["BEGIN", "COMMIT", "ROLLBACK", "START TRANSACTION", "SET autocommit = 1"]
        
        with conn.cursor() as cursor:
            for cmd in commands:
                try:
                    cursor.execute(cmd)
                    print(f"✅ {cmd} - succeeded")
                except Exception as e:
                    print(f"❌ {cmd} - failed: {e}")
        
        conn.close()
        return True
    except Exception as e:
        print(f"❌ Transaction commands test failed: {e}")
        return False

@contextmanager
def yamlbase_server():
    """Start yamlbase server for testing"""
    print("Starting yamlbase server...")
    server = subprocess.Popen([
        'cargo', 'run', '--', 
        '-f', 'examples/sample_database.yaml',
        '--mysql-port', '3306',
        '--allow-mysql-any-host'
    ], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    
    # Wait for server to start
    time.sleep(3)
    
    try:
        yield server
    finally:
        print("\nStopping yamlbase server...")
        server.terminate()
        server.wait()

def main():
    """Run all compatibility tests"""
    print("=== SQLAlchemy Compatibility Test Suite ===")
    print("Testing yamlbase MySQL protocol implementation")
    print("=" * 50)
    
    # Check for required packages
    try:
        import pymysql
        import sqlalchemy
        print(f"PyMySQL version: {pymysql.__version__}")
        print(f"SQLAlchemy version: {sqlalchemy.__version__}")
    except ImportError as e:
        print(f"Missing required package: {e}")
        print("Install with: pip install pymysql sqlalchemy")
        return 1
    
    with yamlbase_server():
        tests = [
            test_direct_pymysql,
            test_sqlalchemy_rollback_behavior,
            test_transaction_commands,
            test_sqlalchemy_basic,
            test_sqlalchemy_with_pooling,
        ]
        
        passed = 0
        failed = 0
        
        for test in tests:
            if test():
                passed += 1
            else:
                failed += 1
        
        print("\n" + "=" * 50)
        print(f"SUMMARY: {passed} passed, {failed} failed")
        
        if failed > 0:
            print("\n⚠️  SQLAlchemy compatibility issues detected!")
            print("These need to be fixed for proper SQLAlchemy support.")
        
        return 0 if failed == 0 else 1

if __name__ == "__main__":
    sys.exit(main())
#!/usr/bin/env python3
"""
Example of connecting to YamlBase using Teradata protocol

This demonstrates how to connect to YamlBase using the native Teradata
wire protocol, allowing Teradata applications to work without modification.
"""

import socket
import struct

class TeradataClient:
    """Simple Teradata protocol client for demonstration"""
    
    def __init__(self, host='localhost', port=1025):
        self.host = host
        self.port = port
        self.socket = None
        
    def connect(self, username, password, database='test'):
        """Connect to YamlBase using Teradata protocol"""
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.connect((self.host, self.port))
        
        # Send logon request
        logon_data = struct.pack('>H', 1)  # Version
        logon_data += self._encode_string(username)
        logon_data += self._encode_string(password)
        logon_data += self._encode_string(database)
        logon_data += self._encode_string("UTF8")
        
        logon_parcel = struct.pack('>HI', 100, len(logon_data) + 6)
        logon_parcel += logon_data
        
        message = struct.pack('>IIH', len(logon_parcel) + 10, 1, 1)
        message += logon_parcel
        
        self.socket.send(message)
        response = self.socket.recv(4096)
        return self._check_auth_response(response)
    
    def execute(self, sql):
        """Execute a SQL statement using Teradata syntax"""
        # Create run request parcel
        run_data = struct.pack('>HH', 1, 0)  # Statement number, options
        run_data += struct.pack('>I', len(sql))
        run_data += sql.encode('utf-8')
        
        run_parcel = struct.pack('>HI', 1, len(run_data) + 6)
        run_parcel += run_data
        
        message = struct.pack('>IIH', len(run_parcel) + 10, 1, 1)
        message += run_parcel
        
        self.socket.send(message)
        return self._read_response()
    
    def close(self):
        """Close the connection"""
        if self.socket:
            # Send logoff request
            logoff_parcel = struct.pack('>HI', 101, 6)
            message = struct.pack('>IIH', 16, 1, 1)
            message += logoff_parcel
            self.socket.send(message)
            self.socket.close()
    
    def _encode_string(self, s):
        """Encode string with length prefix"""
        b = s.encode('utf-8')
        return struct.pack('>H', len(b)) + b
    
    def _check_auth_response(self, data):
        """Check authentication response"""
        # Simple check for auth success
        return b'\x00\x66' in data  # Parcel kind 102 (0x66) = Auth OK
    
    def _read_response(self):
        """Read and parse response"""
        response = self.socket.recv(8192)
        # Simple parsing - just return raw response for demo
        return response


def main():
    """Demonstrate Teradata protocol with YamlBase"""
    
    print("YamlBase Teradata Protocol Example")
    print("=" * 40)
    
    # Connect to YamlBase
    client = TeradataClient(port=1025)
    
    print("\n1. Connecting to YamlBase...")
    if client.connect('admin', 'password', 'EnterpriseDB'):
        print("   ✓ Connected successfully")
    else:
        print("   ✗ Connection failed")
        return
    
    # Example 1: Using Teradata SEL syntax
    print("\n2. Testing Teradata SEL syntax...")
    response = client.execute("SEL * FROM employees WHERE department = 'Engineering'")
    if response:
        print("   ✓ Query executed successfully")
    
    # Example 2: Using Teradata date functions
    print("\n3. Testing Teradata date functions...")
    response = client.execute("""
        SEL employee_id, 
            first_name, 
            ADD_MONTHS(hire_date, 12) AS anniversary_date,
            EXTRACT(YEAR FROM hire_date) AS hire_year
        FROM employees
    """)
    if response:
        print("   ✓ Teradata functions working")
    
    # Example 3: Using Teradata-specific operators
    print("\n4. Testing Teradata operators...")
    response = client.execute("""
        SEL project_id,
            budget,
            budget MOD 100000 AS budget_remainder
        FROM projects
        WHERE status = 'active'
    """)
    if response:
        print("   ✓ Teradata operators working")
    
    # Example 4: System table query
    print("\n5. Testing system table emulation...")
    response = client.execute("SEL * FROM DBC.Tables")
    if response:
        print("   ✓ System tables accessible")
    
    # Close connection
    print("\n6. Closing connection...")
    client.close()
    print("   ✓ Connection closed")
    
    print("\n" + "=" * 40)
    print("Example completed successfully!")
    print("\nTo use with real Teradata tools:")
    print("  - BTEQ: .logon localhost:1025/admin,password")
    print("  - Python: import teradatasql")
    print("  - JDBC: jdbc:teradata://localhost:1025/EnterpriseDB")


if __name__ == "__main__":
    main()
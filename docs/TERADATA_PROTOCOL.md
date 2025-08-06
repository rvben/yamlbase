# Teradata Protocol Support in YamlBase

YamlBase now supports the Teradata SQL wire protocol, allowing Teradata clients and applications to connect directly to YamlBase without modification.

## Overview

The Teradata protocol implementation provides:
- Native Teradata parcel-based communication
- Teradata SQL dialect translation
- Authentication compatible with Teradata clients
- Support for common Teradata SQL functions and syntax

## Architecture

The implementation uses a translation layer approach:

```
Teradata Client → Teradata Protocol Handler → SQL Translator → YamlBase SQL Engine
```

### Components

1. **Parcel System** (`src/protocol/teradata/parcels.rs`)
   - Implements Teradata's parcel-based messaging
   - Handles encoding/decoding of messages
   - Supports all essential parcel types

2. **Authentication** (`src/protocol/teradata/auth.rs`)
   - TD2 authentication support
   - Username/password validation
   - Session management

3. **SQL Translator** (`src/protocol/teradata/sql_translator.rs`)
   - Converts Teradata SQL to standard SQL
   - Handles Teradata-specific functions
   - Manages case-insensitive identifiers

4. **Connection Handler** (`src/protocol/teradata/connection.rs`)
   - Manages client connections
   - Routes queries through translator
   - Handles response formatting

## Usage

### Starting YamlBase with Teradata Protocol

```bash
yamlbase --file database.yaml --protocol teradata --port 1025 --username admin --password password
```

Default Teradata port: 1025

### Connecting from Python (teradatasql)

```python
import teradatasql

# Connect to YamlBase using Teradata protocol
conn = teradatasql.connect(
    host='localhost',
    port=1025,
    user='admin',
    password='password',
    database='test'
)

cursor = conn.cursor()
cursor.execute("SEL * FROM employees")  # Teradata SEL syntax
for row in cursor:
    print(row)
```

### Connecting from BTEQ

```bash
.logon localhost:1025/admin,password
.set width 200
sel * from employees;
.logoff
```

## Supported Teradata SQL Features

### SQL Syntax

- **SEL as SELECT alias**: `SEL * FROM table`
- **Date literals**: `DATE '2024-01-01'`, `TIMESTAMP '2024-01-01 12:00:00'`
- **SAMPLE clause**: `SELECT * FROM table SAMPLE 10`
- **MOD operator**: `value1 MOD value2`
- **Power operator**: `base ** exponent`

### Functions

#### Date/Time Functions
- `ADD_MONTHS(date, months)` - Add months to a date
- `LAST_DAY(date)` - Last day of the month
- `EXTRACT(field FROM date)` - Extract date component
- `TRUNC(date, 'MM')` - Truncate date to month/year/day

#### Null Handling
- `ZEROIFNULL(value)` - Return 0 if null
- `NULLIFZERO(value)` - Return null if zero

#### System Tables (Emulated)
- `DBC.Tables` - List of tables
- `DBC.Columns` - Column information
- `HELP TABLE tablename` - Table structure
- `SHOW TABLE tablename` - DDL for table

### Data Types

All Teradata data types are mapped to YamlBase equivalents:
- `BYTEINT` → `Integer`
- `SMALLINT` → `Integer`
- `INTEGER` → `Integer`
- `BIGINT` → `Integer`
- `DECIMAL(p,s)` → `Decimal`
- `FLOAT/REAL` → `Float/Double`
- `CHAR/VARCHAR` → `Text`
- `DATE/TIME/TIMESTAMP` → `Date/Time/Timestamp`

## Testing

A comprehensive test suite is included:

```python
# Run the Teradata protocol test
python tools/test_teradata_protocol.py
```

The test covers:
1. Authentication (logon/logoff)
2. Basic queries
3. Teradata-specific SQL syntax
4. Error handling

## Implementation Details

### Parcel Types

Supported request parcels:
- `LogonRequest` (100) - Authentication
- `LogoffRequest` (101) - Session termination
- `RunRequest` (1) - SQL execution

Supported response parcels:
- `AuthenticationOk` (102)
- `AuthenticationFailed` (103)
- `SuccessParcel` (8)
- `RecordParcel` (10)
- `DataInfoParcel` (15)
- `ErrorParcel` (13)
- `EndStatementParcel` (11)
- `EndRequestParcel` (12)

### Message Format

```
[Message Length: 4 bytes]
[Message Kind: 4 bytes]
[Parcel Count: 2 bytes]
[Parcel 1]
  [Parcel Kind: 2 bytes]
  [Parcel Length: 4 bytes]
  [Parcel Data: variable]
[Parcel 2...]
```

### SQL Translation Examples

| Teradata SQL | Translated SQL |
|--------------|----------------|
| `SEL * FROM table` | `SELECT * FROM table` |
| `DATE '2024-01-01'` | `'2024-01-01'::date` |
| `ADD_MONTHS(dt, 3)` | `ADD_MONTHS(dt, 3)` |
| `value1 MOD value2` | `value1 % value2` |
| `base ** exp` | `base ^ exp` |
| `SAMPLE 10` | `LIMIT 10` |
| `ZEROIFNULL(col)` | `COALESCE(col, 0)` |

## Limitations

1. **QUALIFY clause**: Not fully implemented (complex window function logic)
2. **PERIOD data type**: Not supported
3. **Stored procedures**: Not supported
4. **Prepared statements**: Basic support only
5. **Extended query protocol**: Limited implementation

## Configuration

The Teradata protocol can be configured via command-line arguments:

```bash
yamlbase \
  --protocol teradata \
  --port 1025 \
  --username admin \
  --password password \
  --database EDW
```

Or via configuration file:

```yaml
protocol: teradata
port: 1025
username: admin
password: password
database: EDW
```

## Troubleshooting

### Connection Issues

If clients can't connect:
1. Check the port is not in use: `lsof -i :1025`
2. Verify authentication credentials match
3. Check firewall rules allow port 1025

### SQL Errors

If queries fail:
1. Check table names (case-sensitive in YamlBase)
2. Verify Teradata functions are supported
3. Review logs for translation errors

### Performance

For optimal performance:
1. Use connection pooling in clients
2. Batch queries when possible
3. Index frequently queried columns in YAML

## Future Enhancements

- Full QUALIFY clause support
- PERIOD and INTERVAL data types
- Multi-statement request support
- Enhanced prepared statement handling
- Kerberos authentication
- Data encryption support

## Contributing

To add new Teradata SQL features:

1. Update `sql_translator.rs` with new translations
2. Add test cases to `test_teradata_protocol.py`
3. Document the feature in this file
4. Submit a pull request

## License

The Teradata protocol support is part of YamlBase and is licensed under MIT/Apache-2.0.
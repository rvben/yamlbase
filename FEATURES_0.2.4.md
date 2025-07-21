# YamlBase v0.2.4 - Enhanced SQL Compatibility

This release adds essential SQL features to improve compatibility with production databases and enable more complex query patterns.

## New Features

### 1. String Functions
Added support for essential SQL string functions:

- **`UPPER(string)`** - Converts string to uppercase
- **`LOWER(string)`** - Converts string to lowercase  
- **`TRIM(string)`** - Removes leading and trailing whitespace

These functions work in:
- SELECT clauses
- WHERE clauses
- JOIN conditions (see below)

### 2. CHAR Data Type
Added support for fixed-length character strings:

```yaml
columns:
  FLAG: "CHAR(1)"          # Single character flag
  CODE: "CHAR(3)"          # Three-character code
  LEGACY_ID: "CHAR(10)"    # Fixed 10-character ID
```

### 3. Functions in JOIN Conditions
The most requested feature - you can now use functions in JOIN ON clauses:

```sql
-- Case-insensitive joins
SELECT * FROM table1 t1
LEFT JOIN table2 t2 ON UPPER(t1.field) = UPPER(t2.field)

-- Trimmed field matching
SELECT * FROM orders o
JOIN customers c ON TRIM(o.customer_id) = TRIM(c.id)
```

## Use Case: Enterprise Database Integration

These features enable enterprise systems to use identical queries in both local development (YamlBase) and production databases:

```sql
-- This query now works identically in both environments
SELECT 
    a.EMPLOYEE_ID,
    a.JOB_CLASSIFICATION,
    r.FIRST_NAME,
    r.LAST_NAME,
    r.EMAIL_ADDRESS
FROM PROJECT_ASSIGNMENTS a
LEFT JOIN EMPLOYEES r ON UPPER(a.EMPLOYEE_ID) = UPPER(r.EMPLOYEE_ID)
WHERE a.PROJECT_ID = ? AND a.VERSION_CODE = 'Published'
```

## Testing

Run the included test suite:

```bash
# Start YamlBase with the test data
docker run -d -p 5432:5432 \
  -v $(pwd)/examples/sql_features_test.yaml:/data/database.yaml \
  rvben/yamlbase:0.2.4

# Run the test script
python test_new_features.py
```

## Implementation Details

- String functions are evaluated with proper row context in all query positions
- CHAR type is mapped to PostgreSQL's `bpchar` type (OID 1042)
- Functions in JOINs use a specialized evaluation path that handles multi-table row context
- All functions properly handle NULL values

## Compatibility

- **PostgreSQL wire protocol**: Full support for new features
- **MySQL wire protocol**: String types work; functions limited by simple protocol
- **Backward compatibility**: Existing YAML files and queries continue to work unchanged

## Future Enhancements

While this release focuses on the most critical features for enterprise integration, future releases could add:

- Additional string functions (SUBSTRING, LENGTH, CONCAT)
- Date manipulation functions beyond the existing ADD_MONTHS/LAST_DAY
- Numeric functions (ROUND, CEIL, FLOOR)
- More complex JOIN patterns

## Credits

Thanks to the community for the detailed feature requests and test cases that guided this implementation.
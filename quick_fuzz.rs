use std::time::Instant;

fn main() {
    println!("Quick fuzzing yamlbase components...\n");
    
    let start = Instant::now();
    let mut crash_count = 0;
    
    // Test 1: YAML parsing with random inputs
    println!("=== Testing YAML Parser ===");
    let yaml_inputs = vec![
        // Malformed YAML
        "database:\n  name: test\ntables:\n  users:\n    columns:\n      - - - -",
        "database: &anchor\n  <<: *anchor",  // Circular reference
        "database:\n  name: |\n    !!!#!/bin/bash\n    rm -rf /",
        "{{{{{{{{{",
        "]]]]]]]]",
        "\u{0000}\u{0001}\u{0002}",  // Control characters
        "database:\n  name: \"\n",  // Unclosed quote
        &"a: ".repeat(10000),  // Deep recursion
        "tables:\n  \u{202e}users:  # Right-to-left override",
        "database:\n  name: \"\\x00\\x01\\x02\"",
        
        // Type confusion
        "database:\n  name: 123\ntables:\n  456:\n    columns:\n      789: INTEGER",
        "database:\n  name: [1,2,3]\ntables: null",
        "database:\n  name: true\ntables:\n  false:\n    columns: 123",
        
        // Large inputs
        &format!("database:\n  name: \"{}\"\ntables:", "A".repeat(1000000)),
        &format!("tables:\n{}", "  t:\n    columns:\n      c: INTEGER\n".repeat(1000)),
    ];
    
    for (i, input) in yaml_inputs.iter().enumerate() {
        print!("Test {}: ", i);
        match std::panic::catch_unwind(|| {
            let _ = yamlbase::yaml::parse_yaml_string(input);
        }) {
            Ok(_) => println!("OK"),
            Err(_) => {
                println!("CRASH!");
                crash_count += 1;
            }
        }
    }
    
    // Test 2: SQL parsing with edge cases
    println!("\n=== Testing SQL Parser ===");
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;
    
    let sql_inputs = vec![
        "SELECT * FROM t WHERE",
        "SELECT * FROM t WHERE a =",
        "SELECT * FROM t WHERE a IN",
        "SELECT * FROM t WHERE a IN ()",
        "SELECT * FROM t WHERE a LIKE",
        "SELECT * FROM t WHERE a LIKE '",
        "SELECT * FROM t WHERE a LIKE '%",
        "SELECT * FROM t WHERE a LIKE '_",
        "SELECT * FROM t WHERE (((((",
        "SELECT * FROM t WHERE )))))",
        "SELECT 1/0",
        "SELECT " + &"(".repeat(1000),
        "SELECT " + &"1+".repeat(1000) + "1",
        "SELECT * FROM t WHERE a = '\u{0000}'",
        "SELECT * FROM t WHERE a = '\\",
        "SELECT * FROM t WHERE a LIKE '\\_%'",  // Escape sequences
    ];
    
    let dialect = GenericDialect {};
    for (i, input) in sql_inputs.iter().enumerate() {
        print!("SQL Test {}: ", i);
        match std::panic::catch_unwind(|| {
            let _ = Parser::parse_sql(&dialect, input);
        }) {
            Ok(_) => println!("OK"),
            Err(_) => {
                println!("CRASH!");
                crash_count += 1;
            }
        }
    }
    
    // Test 3: Query execution with edge cases
    println!("\n=== Testing Query Execution ===");
    use std::sync::Arc;
    use tokio::sync::RwLock;
    
    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.block_on(async {
        use yamlbase::database::{Database, Table, Column, Value};
        use yamlbase::yaml::schema::SqlType;
        use yamlbase::sql::executor::QueryExecutor;
        
        let mut db = Database::new("test".to_string());
        let mut table = Table::new(
            "t".to_string(),
            vec![
                Column {
                    name: "a".to_string(),
                    sql_type: SqlType::Text,
                    primary_key: false,
                    nullable: true,
                    unique: false,
                    default: None,
                    references: None,
                },
            ],
        );
        
        // Add some test data with edge cases
        table.rows.push(vec![Value::Text("normal".to_string())]);
        table.rows.push(vec![Value::Text("".to_string())]);  // Empty string
        table.rows.push(vec![Value::Text("\\".to_string())]);  // Backslash
        table.rows.push(vec![Value::Text("%".to_string())]);  // Percent
        table.rows.push(vec![Value::Text("_".to_string())]);  // Underscore
        table.rows.push(vec![Value::Null]);  // NULL
        
        let _ = db.add_table(table);
        let db_arc = Arc::new(RwLock::new(db));
        let executor = QueryExecutor::new(db_arc);
        
        let execution_tests = vec![
            "SELECT * FROM t WHERE a LIKE '%'",
            "SELECT * FROM t WHERE a LIKE '_'",
            "SELECT * FROM t WHERE a LIKE ''",
            "SELECT * FROM t WHERE a LIKE '\\%'",
            "SELECT * FROM t WHERE a LIKE '\\_'",
            "SELECT * FROM t WHERE a LIKE '\\\\%'",
            "SELECT * FROM t WHERE a LIKE NULL",
            "SELECT * FROM t WHERE a IN (NULL)",
            "SELECT * FROM t WHERE a NOT IN ()",
            "SELECT * FROM t WHERE a = ''",
            "SELECT * FROM t WHERE a IS NULL",
            "SELECT * FROM t WHERE a IS NOT NULL",
            "SELECT * FROM t WHERE TRUE",
            "SELECT * FROM t WHERE FALSE",
            "SELECT * FROM t WHERE 1=1 AND 2=2 AND 3=3 AND 4=4 AND 5=5",
        ];
        
        for (i, query) in execution_tests.iter().enumerate() {
            print!("Exec Test {}: ", i);
            match Parser::parse_sql(&dialect, query) {
                Ok(mut statements) if !statements.is_empty() => {
                    match std::panic::catch_unwind(|| {
                        let stmt = statements.remove(0);
                        let exec_future = executor.execute(&stmt);
                        let handle = tokio::runtime::Handle::current();
                        let result = handle.block_on(exec_future);
                        result
                    }) {
                        Ok(Ok(_)) => println!("OK"),
                        Ok(Err(e)) => println!("Error (handled): {}", e),
                        Err(_) => {
                            println!("CRASH!");
                            crash_count += 1;
                        }
                    }
                }
                _ => println!("Parse failed"),
            }
        }
    });
    
    let elapsed = start.elapsed();
    println!("\n=== Summary ===");
    println!("Total crashes found: {}", crash_count);
    println!("Time elapsed: {:.2?}", elapsed);
    
    if crash_count > 0 {
        println!("\n⚠️  Found {} crash(es)! These should be investigated.", crash_count);
    } else {
        println!("\n✅ No crashes found! The code appears to be robust.");
    }
}

// Ensure we can access the yamlbase crate
extern crate yamlbase;
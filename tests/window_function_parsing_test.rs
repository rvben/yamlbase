use yamlbase::sql::parse_sql;

#[test]
fn test_window_function_parsing() {
    // Test basic ROW_NUMBER() window function
    let sql = "SELECT username, ROW_NUMBER() OVER (ORDER BY id) as row_num FROM users";
    
    match parse_sql(sql) {
        Ok(statements) => {
            println!("✅ Successfully parsed ROW_NUMBER(): {:#?}", statements);
        }
        Err(e) => {
            println!("❌ Parse error for ROW_NUMBER(): {}", e);
            panic!("Window function should parse correctly");
        }
    }
    
    // Test RANK() window function
    let sql2 = "SELECT username, RANK() OVER (ORDER BY id) as rank_num FROM users";
    
    match parse_sql(sql2) {
        Ok(statements) => {
            println!("✅ Successfully parsed RANK(): {:#?}", statements);
        }
        Err(e) => {
            println!("❌ Parse error for RANK(): {}", e);
            panic!("RANK window function should parse correctly");
        }
    }
    
    // Test with PARTITION BY
    let sql3 = "SELECT username, ROW_NUMBER() OVER (PARTITION BY department ORDER BY id) as row_num FROM users";
    
    match parse_sql(sql3) {
        Ok(statements) => {
            println!("✅ Successfully parsed PARTITION BY: {:#?}", statements);
        }
        Err(e) => {
            println!("❌ Parse error for PARTITION BY: {}", e);
            panic!("PARTITION BY should parse correctly");
        }
    }
}
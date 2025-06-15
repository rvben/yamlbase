#![no_main]
use libfuzzer_sys::fuzz_target;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

fuzz_target!(|data: &[u8]| {
    // Only fuzz valid UTF-8 strings
    if let Ok(query) = std::str::from_utf8(data) {
        // Try to parse the SQL query
        let dialect = GenericDialect {};
        // We don't care about the result, just that it doesn't panic
        let _ = Parser::parse_sql(&dialect, query);
    }
});
use regex::Regex;

pub struct TeradataSqlTranslator;

impl Default for TeradataSqlTranslator {
    fn default() -> Self {
        Self::new()
    }
}

impl TeradataSqlTranslator {
    pub fn new() -> Self {
        Self
    }
    
    pub fn translate(&self, sql: &str) -> String {
        let mut translated = sql.to_string();
        
        // Convert SEL to SELECT
        translated = self.convert_sel_to_select(&translated);
        
        // Convert Teradata date/time literals
        translated = self.convert_date_literals(&translated);
        
        // Convert Teradata-specific functions
        translated = self.convert_functions(&translated);
        
        // Convert QUALIFY clause (simplified - convert to subquery)
        translated = self.convert_qualify(&translated);
        
        // Convert SAMPLE clause  
        translated = self.convert_sample(&translated);
        
        // Convert Teradata operators
        translated = self.convert_operators(&translated);
        
        // Handle case sensitivity (Teradata is case-insensitive by default)
        translated = self.normalize_identifiers(&translated);
        
        translated
    }
    
    fn convert_sel_to_select(&self, sql: &str) -> String {
        let re = Regex::new(r"(?i)^\s*SEL\s+").unwrap();
        re.replace(sql, "SELECT ").to_string()
    }
    
    fn convert_date_literals(&self, sql: &str) -> String {
        let mut result = sql.to_string();
        
        // Convert DATE 'YYYY-MM-DD' to 'YYYY-MM-DD'::date
        let date_re = Regex::new(r"(?i)DATE\s*'([^']+)'").unwrap();
        result = date_re.replace_all(&result, "'$1'::date").to_string();
        
        // Convert TIMESTAMP 'YYYY-MM-DD HH:MM:SS' to 'YYYY-MM-DD HH:MM:SS'::timestamp
        let ts_re = Regex::new(r"(?i)TIMESTAMP\s*'([^']+)'").unwrap();
        result = ts_re.replace_all(&result, "'$1'::timestamp").to_string();
        
        // Convert TIME 'HH:MM:SS' to 'HH:MM:SS'::time
        let time_re = Regex::new(r"(?i)TIME\s*'([^']+)'").unwrap();
        result = time_re.replace_all(&result, "'$1'::time").to_string();
        
        result
    }
    
    fn convert_functions(&self, sql: &str) -> String {
        let mut result = sql.to_string();
        
        // ADD_MONTHS is already supported in yamlbase PostgreSQL mode
        // Just ensure it's uppercase for consistency
        let add_months_re = Regex::new(r"(?i)add_months").unwrap();
        result = add_months_re.replace_all(&result, "ADD_MONTHS").to_string();
        
        // EXTRACT is standard SQL, ensure proper format
        // Teradata: EXTRACT(YEAR FROM date_col)
        // PostgreSQL: EXTRACT(YEAR FROM date_col) - same!
        
        // LAST_DAY is already supported
        let last_day_re = Regex::new(r"(?i)last_day").unwrap();
        result = last_day_re.replace_all(&result, "LAST_DAY").to_string();
        
        // Convert Teradata TRUNC for dates to DATE_TRUNC
        // Teradata: TRUNC(date_col, 'MM') 
        // PostgreSQL: DATE_TRUNC('month', date_col)
        let trunc_re = Regex::new(r"(?i)TRUNC\s*\(\s*([^,]+),\s*'(MM|DD|YY|YYYY)'\s*\)").unwrap();
        result = trunc_re.replace_all(&result, |caps: &regex::Captures| {
            let col = &caps[1];
            let unit = match &caps[2].to_uppercase()[..] {
                "MM" => "month",
                "DD" => "day", 
                "YY" | "YYYY" => "year",
                _ => "day",
            };
            format!("DATE_TRUNC('{}', {})", unit, col)
        }).to_string();
        
        // Convert Teradata FORMAT function to TO_CHAR
        // Teradata: FORMAT(date_col, 'YYYY-MM-DD')
        // PostgreSQL: TO_CHAR(date_col, 'YYYY-MM-DD')
        let format_re = Regex::new(r"(?i)FORMAT\s*\(").unwrap();
        result = format_re.replace_all(&result, "TO_CHAR(").to_string();
        
        // Convert Teradata ZEROIFNULL to COALESCE
        let zeroifnull_re = Regex::new(r"(?i)ZEROIFNULL\s*\(([^)]+)\)").unwrap();
        result = zeroifnull_re.replace_all(&result, "COALESCE($1, 0)").to_string();
        
        // Convert Teradata NULLIFZERO to NULLIF
        let nullifzero_re = Regex::new(r"(?i)NULLIFZERO\s*\(([^)]+)\)").unwrap();
        result = nullifzero_re.replace_all(&result, "NULLIF($1, 0)").to_string();
        
        result
    }
    
    fn convert_qualify(&self, sql: &str) -> String {
        // QUALIFY is complex - for now, just pass through
        // In a full implementation, this would convert to a subquery with window functions
        sql.to_string()
    }
    
    fn convert_sample(&self, sql: &str) -> String {
        let mut result = sql.to_string();
        
        // Convert SAMPLE n to LIMIT n (simplified)
        let sample_re = Regex::new(r"(?i)\sSAMPLE\s+(\d+)").unwrap();
        result = sample_re.replace_all(&result, " LIMIT $1").to_string();
        
        // Convert SAMPLE n PERCENT to percentage-based sampling
        let sample_pct_re = Regex::new(r"(?i)\sSAMPLE\s+(\d+)\s+PERCENT").unwrap();
        result = sample_pct_re.replace_all(&result, " TABLESAMPLE BERNOULLI ($1)").to_string();
        
        result
    }
    
    fn convert_operators(&self, sql: &str) -> String {
        let mut result = sql.to_string();
        
        // Convert MOD operator to % 
        // Teradata: value1 MOD value2
        // PostgreSQL: value1 % value2
        let mod_re = Regex::new(r"\s+MOD\s+").unwrap();
        result = mod_re.replace_all(&result, " % ").to_string();
        
        // Convert ** (exponentiation) to ^
        // Teradata: base ** exponent
        // PostgreSQL: base ^ exponent
        result = result.replace("**", "^");
        
        result
    }
    
    fn normalize_identifiers(&self, sql: &str) -> String {
        // In Teradata, identifiers are case-insensitive
        // For simplicity, we'll lowercase unquoted identifiers
        // This is a simplified implementation
        sql.to_string()
    }
    
    pub fn is_teradata_system_query(&self, sql: &str) -> bool {
        let sql_upper = sql.to_uppercase();
        sql_upper.contains("DBC.") || 
        sql_upper.contains("HELP ") ||
        sql_upper.contains("SHOW ")
    }
    
    pub fn handle_system_query(&self, sql: &str) -> Option<String> {
        let sql_upper = sql.to_uppercase();
        
        // Convert DBC.Tables query
        if sql_upper.contains("DBC.TABLES") {
            return Some(
                "SELECT table_name as TableName, 'T' as TableKind 
                 FROM information_schema.tables 
                 WHERE table_schema NOT IN ('pg_catalog', 'information_schema')".to_string()
            );
        }
        
        // Convert DBC.Columns query  
        if sql_upper.contains("DBC.COLUMNS") {
            return Some(
                "SELECT column_name as ColumnName, data_type as ColumnType 
                 FROM information_schema.columns 
                 WHERE table_schema NOT IN ('pg_catalog', 'information_schema')".to_string()
            );
        }
        
        // Convert HELP TABLE
        if sql_upper.starts_with("HELP TABLE") {
            let table_name = sql[10..].trim().trim_end_matches(';');
            return Some(format!(
                "SELECT column_name, data_type, is_nullable 
                 FROM information_schema.columns 
                 WHERE table_name = '{}'", 
                table_name
            ));
        }
        
        // Convert SHOW TABLE
        if sql_upper.starts_with("SHOW TABLE") {
            let table_name = sql[10..].trim().trim_end_matches(';');
            return Some(format!(
                "SELECT 'CREATE TABLE ' || table_name || ' (' || 
                 string_agg(column_name || ' ' || data_type, ', ') || ')' as ddl
                 FROM information_schema.columns 
                 WHERE table_name = '{}'
                 GROUP BY table_name",
                table_name
            ));
        }
        
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_sel_to_select() {
        let translator = TeradataSqlTranslator::new();
        
        assert_eq!(
            translator.translate("SEL * FROM users"),
            "SELECT * FROM users"
        );
        
        assert_eq!(
            translator.translate("sel name, id from products"),
            "SELECT name, id from products"
        );
    }
    
    #[test]
    fn test_date_literals() {
        let translator = TeradataSqlTranslator::new();
        
        assert_eq!(
            translator.translate("SELECT * WHERE created_date = DATE '2024-01-01'"),
            "SELECT * WHERE created_date = '2024-01-01'::date"
        );
    }
    
    #[test]
    fn test_functions() {
        let translator = TeradataSqlTranslator::new();
        
        assert_eq!(
            translator.translate("SELECT add_months(current_date, 3)"),
            "SELECT ADD_MONTHS(current_date, 3)"
        );
        
        assert_eq!(
            translator.translate("SELECT ZEROIFNULL(amount)"),
            "SELECT COALESCE(amount, 0)"
        );
    }
}
use crate::yaml::schema::*;

#[test]
fn test_parse_column_type() {
        let col = YamlColumn::parse("id".to_string(), "INTEGER PRIMARY KEY").unwrap();
        assert_eq!(col.name, "id");
        assert!(col.is_primary_key);
        assert!(!col.is_nullable);
        
        let col = YamlColumn::parse("name".to_string(), "VARCHAR(100) NOT NULL").unwrap();
        assert_eq!(col.get_base_type().unwrap(), SqlType::Varchar(100));
        assert!(!col.is_nullable);
        
        let col = YamlColumn::parse("created".to_string(), "TIMESTAMP DEFAULT CURRENT_TIMESTAMP").unwrap();
        assert_eq!(col.default_value, Some("CURRENT_TIMESTAMP".to_string()));
    }

    #[test]
    fn test_extract_size() {
        assert_eq!(extract_size("VARCHAR(255)"), Some(255));
        assert_eq!(extract_size("DECIMAL(10,2)"), Some(10));
        assert_eq!(extract_size("INTEGER"), None);
    }

    #[test]
    fn test_sql_type_parsing() {
        assert_eq!(YamlColumn::parse("col".to_string(), "INTEGER").unwrap().get_base_type().unwrap(), SqlType::Integer);
        assert_eq!(YamlColumn::parse("col".to_string(), "BOOLEAN").unwrap().get_base_type().unwrap(), SqlType::Boolean);
        assert_eq!(YamlColumn::parse("col".to_string(), "UUID").unwrap().get_base_type().unwrap(), SqlType::Uuid);
    }
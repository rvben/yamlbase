use dashmap::DashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::database::{Database, Value};

pub struct Storage {
    database: Arc<RwLock<Database>>,
    primary_key_index: Arc<DashMap<String, DashMap<Value, usize>>>, // table -> pk_value -> row_idx
}

impl Storage {
    pub fn new(database: Database) -> Self {
        let storage = Self {
            database: Arc::new(RwLock::new(database)),
            primary_key_index: Arc::new(DashMap::new()),
        };

        // Build initial indexes - try to spawn if in tokio context, otherwise do it synchronously
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            handle.spawn({
                let storage = storage.clone();
                async move {
                    storage.rebuild_indexes().await;
                }
            });
        } else {
            // We're not in a tokio runtime, build indexes synchronously
            // This is mainly for benchmarks and tests that don't run in async context
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                storage.rebuild_indexes().await;
            });
        }

        storage
    }

    pub fn database(&self) -> Arc<RwLock<Database>> {
        Arc::clone(&self.database)
    }

    pub async fn rebuild_indexes(&self) {
        let db = self.database.read().await;

        for (table_name, table) in &db.tables {
            if let Some(pk_idx) = table.primary_key_index {
                let table_index = self
                    .primary_key_index
                    .entry(table_name.clone())
                    .or_default();

                table_index.clear();

                for (row_idx, row) in table.rows.iter().enumerate() {
                    let pk_value = row[pk_idx].clone();
                    table_index.insert(pk_value, row_idx);
                }
            }
        }
    }

    pub async fn find_by_primary_key(
        &self,
        table_name: &str,
        pk_value: &Value,
    ) -> Option<Vec<Value>> {
        if let Some(table_index) = self.primary_key_index.get(table_name) {
            if let Some(row_idx) = table_index.get(pk_value) {
                let db = self.database.read().await;
                if let Some(table) = db.get_table(table_name) {
                    return table.rows.get(*row_idx).cloned();
                }
            }
        }
        None
    }
}

impl Clone for Storage {
    fn clone(&self) -> Self {
        Self {
            database: Arc::clone(&self.database),
            primary_key_index: Arc::clone(&self.primary_key_index),
        }
    }
}

// Implementation of RECURSIVE CTE support for yamlbase
use crate::YamlBaseError;
use crate::database::Database;
use crate::sql::executor::{QueryExecutor, QueryResult};
use sqlparser::ast::{Cte, SetExpr, SetOperator};
use std::collections::{HashMap, HashSet};

impl QueryExecutor {
    /// Execute a RECURSIVE CTE
    ///
    /// RECURSIVE CTEs work by:
    /// 1. Executing the base case (non-recursive part)
    /// 2. Iteratively executing the recursive part using previous results
    /// 3. Continuing until no new rows are produced
    /// 4. Combining all results
    pub async fn execute_recursive_cte(
        &self,
        db: &Database,
        cte: &Cte,
        cte_results: &HashMap<String, QueryResult>,
    ) -> crate::Result<QueryResult> {
        let cte_name = cte.alias.name.value.clone();
        eprintln!("DEBUG: Executing RECURSIVE CTE '{}'", cte_name);

        // Parse the CTE query - should be a UNION or UNION ALL
        let (base_query, recursive_query, is_union_all) = match &cte.query.body.as_ref() {
            SetExpr::SetOperation {
                op: SetOperator::Union,
                set_quantifier,
                left,
                right,
            } => {
                let is_all = matches!(set_quantifier, sqlparser::ast::SetQuantifier::All);
                (left.as_ref(), right.as_ref(), is_all)
            }
            _ => {
                return Err(YamlBaseError::NotImplemented(
                    "RECURSIVE CTE must use UNION or UNION ALL".to_string(),
                ));
            }
        };

        // Execute base case
        let mut all_rows = Vec::new();
        let mut working_table = match base_query {
            SetExpr::Select(select) => {
                let result = self
                    .execute_select_with_cte_context(db, select, &cte.query, cte_results)
                    .await?;
                all_rows.extend(result.rows.clone());
                result
            }
            _ => {
                return Err(YamlBaseError::NotImplemented(
                    "Base case of RECURSIVE CTE must be SELECT".to_string(),
                ));
            }
        };

        // Set up for recursive execution with enhanced protection
        let mut iteration = 0;
        let max_iterations = 1000; // Prevent infinite loops
        let max_memory_bytes = 100_000_000; // 100MB memory limit for CTE results
        let mut estimated_memory_usage = 0usize;
        let mut seen_rows = if !is_union_all {
            let mut set = HashSet::new();
            for row in &all_rows {
                set.insert(format!("{:?}", row));
            }
            Some(set)
        } else {
            None
        };

        // Recursive execution
        loop {
            iteration += 1;
            if iteration > max_iterations {
                return Err(YamlBaseError::Database {
                    message: format!("RECURSIVE CTE '{}' exceeded maximum iterations", cte_name),
                });
            }

            // Create temporary CTE results including the working table
            let mut temp_cte_results = cte_results.clone();
            temp_cte_results.insert(cte_name.clone(), working_table.clone());

            // Execute recursive part
            let recursive_result = match recursive_query {
                SetExpr::Select(select) => {
                    self.execute_select_with_cte_context(db, select, &cte.query, &temp_cte_results)
                        .await?
                }
                _ => {
                    return Err(YamlBaseError::NotImplemented(
                        "Recursive part of RECURSIVE CTE must be SELECT".to_string(),
                    ));
                }
            };

            // Check if we got any new rows
            if recursive_result.rows.is_empty() {
                break; // No new rows, recursion complete
            }

            // Add new rows to results with memory tracking
            let mut new_rows = Vec::new();
            for row in recursive_result.rows {
                // Estimate memory usage for this row (rough calculation)
                let row_memory = row.iter().map(|value| match value {
                    crate::database::Value::Text(s) => s.len(),
                    crate::database::Value::Integer(_) => 8,
                    crate::database::Value::Float(_) => 4,
                    crate::database::Value::Double(_) => 8,
                    crate::database::Value::Boolean(_) => 1,
                    crate::database::Value::Date(_) => 12, // NaiveDate size
                    crate::database::Value::Timestamp(_) => 16, // NaiveDateTime size
                    crate::database::Value::Time(_) => 8, // NaiveTime size
                    crate::database::Value::Uuid(_) => 16, // UUID size
                    crate::database::Value::Decimal(_) => 16, // Decimal size
                    crate::database::Value::Json(json) => json.to_string().len(),
                    crate::database::Value::Null => 1,
                }).sum::<usize>();
                
                estimated_memory_usage = estimated_memory_usage.saturating_add(row_memory);
                
                // Check memory limit
                if estimated_memory_usage > max_memory_bytes {
                    return Err(YamlBaseError::Database {
                        message: format!(
                            "RECURSIVE CTE '{}' exceeded memory limit of {}MB (estimated usage: {}MB)",
                            cte_name,
                            max_memory_bytes / 1_000_000,
                            estimated_memory_usage / 1_000_000
                        ),
                    });
                }
                
                if let Some(ref mut seen) = seen_rows {
                    // UNION (distinct) - check if we've seen this row
                    let row_key = format!("{:?}", row);
                    if !seen.contains(&row_key) {
                        seen.insert(row_key);
                        new_rows.push(row.clone());
                        all_rows.push(row);
                    }
                } else {
                    // UNION ALL - add all rows
                    new_rows.push(row.clone());
                    all_rows.push(row);
                }
            }

            // Update working table for next iteration
            if new_rows.is_empty() {
                break; // No new unique rows
            }
            working_table.rows = new_rows;
        }

        // Return combined results
        Ok(QueryResult {
            columns: working_table.columns,
            column_types: working_table.column_types,
            rows: all_rows,
        })
    }
}

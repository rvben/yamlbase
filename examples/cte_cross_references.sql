-- CTE Cross-Reference Examples for yamlbase 0.4.8
-- These examples demonstrate the new CTE features

-- Example 1: Simple CTE referencing another CTE
-- Use case: Building derived values from base calculations
WITH BaseValues AS (
    SELECT 10 as base_amount, 0.15 as tax_rate
),
Calculations AS (
    SELECT 
        base_amount,
        tax_rate,
        base_amount * tax_rate as tax_amount,
        base_amount + (base_amount * tax_rate) as total_amount
    FROM BaseValues
)
SELECT * FROM Calculations;

-- Example 2: CTE with CROSS JOIN for date range filtering
-- Use case: Finding all records within a dynamic date range
WITH CurrentPeriod AS (
    SELECT 
        DATE_SUB(CURRENT_DATE, INTERVAL DAY(CURRENT_DATE) - 1 DAY) as period_start,
        LAST_DAY(CURRENT_DATE) as period_end
),
FilteredTransactions AS (
    SELECT t.*, p.period_start, p.period_end
    FROM transactions t
    CROSS JOIN CurrentPeriod p
    WHERE t.transaction_date BETWEEN p.period_start AND p.period_end
)
SELECT 
    COUNT(*) as transaction_count,
    SUM(amount) as total_amount,
    MIN(period_start) as period_start,
    MAX(period_end) as period_end
FROM FilteredTransactions;

-- Example 3: Hierarchical data with UNION ALL
-- Use case: Combining direct and indirect relationships
WITH DirectReports AS (
    SELECT employee_id, manager_id, 1 as level
    FROM employees
    WHERE manager_id = 100
),
IndirectReports AS (
    SELECT e.employee_id, e.manager_id, 2 as level
    FROM employees e
    INNER JOIN DirectReports d ON e.manager_id = d.employee_id
)
SELECT * FROM DirectReports
UNION ALL
SELECT * FROM IndirectReports
ORDER BY level, employee_id;

-- Example 4: Multiple CTEs with complex joins
-- Use case: Budget analysis with allocations
WITH BudgetPeriod AS (
    SELECT 2025 as budget_year, 'Q1' as quarter
),
ProjectBudgets AS (
    SELECT p.project_id, p.project_name, b.budget_amount
    FROM projects p
    INNER JOIN budgets b ON p.project_id = b.project_id
    CROSS JOIN BudgetPeriod bp
    WHERE b.year = bp.budget_year AND b.quarter = bp.quarter
),
ProjectAllocations AS (
    SELECT 
        pb.project_id,
        pb.project_name,
        pb.budget_amount,
        COUNT(a.allocation_id) as allocation_count,
        SUM(a.hours) as total_hours
    FROM ProjectBudgets pb
    LEFT JOIN allocations a ON pb.project_id = a.project_id
    GROUP BY pb.project_id, pb.project_name, pb.budget_amount
)
SELECT * FROM ProjectAllocations
WHERE total_hours > 0 OR allocation_count = 0;

-- Example 5: Recursive-style pattern with UNION ALL
-- Use case: Building a complete list from multiple sources
WITH ActiveProjects AS (
    SELECT project_id, 'active' as status FROM projects WHERE status = 'Active'
),
PendingProjects AS (
    SELECT project_id, 'pending' as status FROM projects WHERE status = 'Pending'
),
SpecialProjects AS (
    SELECT project_id, 'special' as status FROM special_projects
),
AllProjectStatuses AS (
    SELECT * FROM ActiveProjects
    UNION ALL
    SELECT * FROM PendingProjects  
    UNION ALL
    SELECT * FROM SpecialProjects
)
SELECT 
    status,
    COUNT(*) as project_count
FROM AllProjectStatuses
GROUP BY status
ORDER BY project_count DESC;
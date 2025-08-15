-- SUGGESTED FIXES FOR MONITORING SQL
-- Based on data quality analysis

-- Fix 1: Handle negative utilization
-- Replace negative utilization calculation with bounds check
CASE 
  WHEN credit_limit_open_accounts > 0 
  THEN GREATEST(0, total_balance_open_accounts / credit_limit_open_accounts)
  ELSE 0 
END as utilization_fixed

-- Fix 2: Add data validation
-- Add WHERE clause to filter out problematic records
WHERE total_balance_open_accounts >= 0
  AND credit_limit_open_accounts > 0
  AND statements_since_clip > 0

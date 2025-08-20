-- This script calculates various metrics for account statements. It first creates a temporary
-- table to hold consolidated, preprocessed data, which is then used by three separate
-- queries, each tailored for a specific analytical purpose with its own filters.

-- Step 1: Create a temporary table with the consolidated base data.
-- This table will be accessible to all subsequent queries in the session.
CREATE OR REPLACE TEMP TABLE account_statements_base_temp AS (
    SELECT
        a.account_id,
        a.statement_num,
        a.statement_end_dt,
        -- Bucket statement number into cohorts.
        CASE
            WHEN a.statement_num = 7 THEN 'Y1-S7'
            WHEN a.statement_num = 11 THEN 'Y1-S11'
            WHEN a.statement_num BETWEEN 13 AND 24 THEN 'Y2'
            WHEN a.statement_num > 24 THEN 'Y3'
            ELSE 'other'
        END AS clip_cohort,
        -- Determine account status.
        CASE
            WHEN a.account_closed_cnt = 1 OR a.account_closed_in_stmt_cnt = 1 OR a.account_open_cnt = 0 THEN 'Closed'
            WHEN a.account_charge_off_in_stmt_cnt = 1 THEN 'CO'
            ELSE 'Open'
        END AS acct_status,
        -- Bucket credit limit.
        CASE
            WHEN a.credit_limit_stmt_usd <= 1000 THEN '1000'
            WHEN a.credit_limit_stmt_usd <= 2000 THEN '2000'
            WHEN a.credit_limit_stmt_usd <= 3000 THEN '3000'
            WHEN a.credit_limit_stmt_usd <= 4000 THEN '4000'
            WHEN a.credit_limit_stmt_usd <= 5000 THEN '5000'
            WHEN a.credit_limit_stmt_usd <= 6000 THEN '6000'
            WHEN a.credit_limit_stmt_usd <= 7000 THEN '7000'
            WHEN a.credit_limit_stmt_usd < 8000 THEN '8000'
            ELSE '8000_plus'
        END AS credit_line_bucket,
        -- Bucket delinquency.
        CASE
            WHEN a.statement_delinquency_bucket_num = 0 THEN '0'
            WHEN a.statement_delinquency_bucket_num = 1 THEN '1'
            WHEN a.statement_delinquency_bucket_num = 2 THEN '2'
            ELSE '3+'
        END AS dq_bucket,
        a.purchase_apr_pct,
        a.avg_outstanding_principal_balance_stmt_usd,
        -- Get eligibility flags. COALESCE ensures we handle accounts not in the clip table.
        COALESCE(c.assumptions_delinquent_last_6_months_passed, FALSE) as assumptions_delinquent_last_6_months_passed,
        COALESCE(c.assumptions_delinquent_last_12_months_passed, FALSE) as assumptions_delinquent_last_12_months_passed,
        COALESCE(c.assumptions_active_last_6_months_passed, FALSE) as assumptions_active_last_6_months_passed,
        COALESCE(c.assumptions_active_last_12_months_passed, FALSE) as assumptions_active_last_12_months_passed,
        COALESCE(c.assumptions_overlimit_passed, FALSE) as assumptions_overlimit_passed,
        COALESCE(c.assumptions_block_cd_passed, FALSE) as assumptions_block_cd_passed,
        COALESCE(c.transunion_fico_08_score_passed, FALSE) as transunion_fico_08_score_passed,
        COALESCE(c.transunion_vantage_30_score_passed, FALSE) as transunion_vantage_30_score_passed,
        COALESCE(c.transunion_total_revolving_debt_passed, FALSE) as transunion_total_revolving_debt_passed,
        COALESCE(c.transunion_total_revolving_debt_to_income_passed, FALSE) as transunion_total_revolving_debt_to_income_passed,
        COALESCE(c.transunion_non_mortgage_debt_to_income_passed, FALSE) as transunion_non_mortgage_debt_to_income_passed,
        COALESCE(c.transunion_bankcard_trades_passed, FALSE) as transunion_bankcard_trades_passed,
        COALESCE(c.transunion_total_trades_passed, FALSE) as transunion_total_trades_passed,
        COALESCE(c.transunion_revolving_debt_velocity_passed, FALSE) as transunion_revolving_debt_velocity_passed,
        COALESCE(c.account_review_hardcuts_passed, FALSE) AS account_review_hardcuts_passed,
        COALESCE(c.assumptions_eligibility_passed, FALSE) AS assumptions_eligibility_passed,
        (COALESCE(c.account_review_hardcuts_passed, FALSE) AND COALESCE(c.assumptions_eligibility_passed, FALSE))::BOOLEAN as passed_both,
        c.is_account_with_rewards,
        b.clip_g_risk_group,
        b.balkan_v2_util_group,
        (COALESCE(c.account_review_hardcuts_passed, FALSE) AND COALESCE(c.assumptions_eligibility_passed, FALSE))::INT AS eligibility_flag,
        CONCAT(RIGHT(CAST(YEAR(a.statement_end_dt) AS VARCHAR), 4), 'Q', QUARTER(a.statement_end_dt)) AS cohort
    FROM
        edw_db.public.account_statements a
    LEFT JOIN
        ds_db.modeling.clip_g_retroscores_calibrated_all_stmts__view b
        ON a.account_id = b.account_id AND a.statement_num = b.statement_num
    LEFT JOIN
        datamart_db.dm_consolidated.account_statements_clip c
        ON a.account_id = c.account_id AND a.statement_num = c.statement_number
    WHERE
        a.statement_end_dt >= '2019-01-01' -- Base filter updated to include data since 2019
);

-- Query 1: Calculate Average APR for eligible accounts since 2024.
SELECT
    clip_cohort,
    is_account_with_rewards,
    eligibility_flag,
    acct_status,
    CASE WHEN purchase_apr_pct <= 30 THEN 'below 2250' ELSE 'above 2250' END AS apr_grp,
    AVG(purchase_apr_pct) AS avg_apr,
    COUNT(*) AS number_of_accounts
FROM
    account_statements_base_temp
WHERE
 statement_end_dt >= DATEADD(month, -6, CURRENT_DATE()) -- Changed to dynamically reflect the past 6 months
AND eligibility_flag = 1
GROUP BY ALL;

-- Query 2: Calculate hardcuts and eligibility pass rates since 2019.
SELECT
    cohort,
    credit_line_bucket,
    rule_name,
    SUM(pass_status::INT) AS pass_count,
    AVG(pass_status::INT) * 100 AS pass_rate_percent,
    COUNT(*) AS total_evaluated
FROM
    account_statements_base_temp
    -- The UNPIVOT operator transforms columns into rows for easier aggregation.
    UNPIVOT(pass_status FOR rule_name IN (
         assumptions_delinquent_last_6_months_passed,
        assumptions_delinquent_last_12_months_passed,
        assumptions_active_last_6_months_passed,
        assumptions_active_last_12_months_passed,
        assumptions_overlimit_passed,
        assumptions_block_cd_passed,
        transunion_fico_08_score_passed,
        transunion_vantage_30_score_passed,
        transunion_total_revolving_debt_passed,
        transunion_total_revolving_debt_to_income_passed,
        transunion_non_mortgage_debt_to_income_passed,
        transunion_bankcard_trades_passed,
        transunion_total_trades_passed,
        transunion_revolving_debt_velocity_passed,
        account_review_hardcuts_passed,
        assumptions_eligibility_passed,
        passed_both
    )) AS unpivoted_rules
GROUP BY ALL
ORDER BY cohort, credit_line_bucket, rule_name;

-- Query 3: Generate aggregated data for performance charts (leaner output).
-- This query pre-aggregates data for eligible accounts from statement 3 onwards.
WITH future_performance AS (
    -- This CTE calculates future performance metrics by joining the base temp table to itself.
    SELECT
        a.account_id,
        a.statement_num,
        -- 8-month forward performance
        MAX(CASE WHEN b.statement_num = a.statement_num + 8 THEN b.dq_bucket END) AS dq_bucket_8,
        MAX(CASE WHEN b.statement_num = a.statement_num + 8 THEN b.acct_status END) AS acct_status_8,
        MAX(CASE WHEN b.statement_num = a.statement_num + 8 THEN b.avg_outstanding_principal_balance_stmt_usd END) AS principal_os_8,
        -- 12-month forward performance
        MAX(CASE WHEN b.statement_num = a.statement_num + 12 THEN b.dq_bucket END) AS dq_bucket_12,
        MAX(CASE WHEN b.statement_num = a.statement_num + 12 THEN b.acct_status END) AS acct_status_12,
        MAX(CASE WHEN b.statement_num = a.statement_num + 12 THEN b.avg_outstanding_principal_balance_stmt_usd END) AS principal_os_12,
        -- 18-month forward performance
        MAX(CASE WHEN b.statement_num = a.statement_num + 18 THEN b.dq_bucket END) AS dq_bucket_18,
        MAX(CASE WHEN b.statement_num = a.statement_num + 18 THEN b.acct_status END) AS acct_status_18,
        MAX(CASE WHEN b.statement_num = a.statement_num + 18 THEN b.avg_outstanding_principal_balance_stmt_usd END) AS principal_os_18
    FROM
        account_statements_base_temp a
    LEFT JOIN
        account_statements_base_temp b
        ON a.account_id = b.account_id AND b.statement_num IN (a.statement_num + 8, a.statement_num + 12, a.statement_num + 18)
    WHERE a.statement_num >= 3 AND a.eligibility_flag = 1 -- Pre-filter for efficiency
    GROUP BY
        1, 2
)
SELECT
    a.cohort,
    a.clip_g_risk_group,
    a.balkan_v2_util_group,
    a.credit_line_bucket,
    case when a.statement_num <12 then 'e_clip'
         else 'l_clip'
         end as clip_stage,
    -- Metric 1: Population counts for distribution charts
    COUNT(*) AS population_count,
    
    -- Metric 2: Components for DQ / Open Rate (8 months)
    SUM(CASE WHEN f.dq_bucket_8 > '1' THEN 1 ELSE 0 END) as delinquent_8_mth_count,
    SUM(CASE WHEN f.acct_status_8 = 'Open' THEN 1 ELSE 0 END) as open_8_mth_count,
    
    -- Metric 3: Components for DQ$ / Open$ Rate (8 months)
    SUM(CASE WHEN f.dq_bucket_8 > '1' THEN f.principal_os_8 ELSE 0 END) as delinquent_principal_8,
    SUM(CASE WHEN f.acct_status_8 = 'Open' THEN f.principal_os_8 ELSE 0 END) as open_principal_8,

    -- Metric 4: Components for DQ / Open Rate (12 months)
    SUM(CASE WHEN f.dq_bucket_12 > '1' THEN 1 ELSE 0 END) as delinquent_12_mth_count,
    SUM(CASE WHEN f.acct_status_12 = 'Open' THEN 1 ELSE 0 END) as open_12_mth_count,

    -- Metric 5: Components for DQ$ / Open$ Rate (12 months)
    SUM(CASE WHEN f.dq_bucket_12 > '1' THEN f.principal_os_12 ELSE 0 END) as delinquent_principal_12,
    SUM(CASE WHEN f.acct_status_12 = 'Open' THEN f.principal_os_12 ELSE 0 END) as open_principal_12,

    -- Metric 6: Components for DQ / Open Rate (18 months)
    SUM(CASE WHEN f.dq_bucket_18 > '1' THEN 1 ELSE 0 END) as delinquent_18_mth_count,
    SUM(CASE WHEN f.acct_status_18 = 'Open' THEN 1 ELSE 0 END) as open_18_mth_count,

    -- Metric 7: Components for DQ$ / Open$ Rate (18 months)
    SUM(CASE WHEN f.dq_bucket_18 > '1' THEN f.principal_os_18 ELSE 0 END) as delinquent_principal_18,
    SUM(CASE WHEN f.acct_status_18 = 'Open' THEN f.principal_os_18 ELSE 0 END) as open_principal_18
FROM
    account_statements_base_temp a
JOIN
    future_performance f ON a.account_id = f.account_id AND a.statement_num = f.statement_num
WHERE
    a.statement_num >= 3 AND a.eligibility_flag = 1 -- Final filter for eligible accounts from statement 3 onwards
GROUP BY
    1, 2, 3, 4,5 -- Group by the dimensions needed for the charts
ORDER BY
    a.cohort, a.clip_g_risk_group, a.balkan_v2_util_group, a.credit_line_bucket;

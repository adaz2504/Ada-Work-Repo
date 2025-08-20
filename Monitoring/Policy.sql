-- Credit Policy Monitoring 
SELECT
    left(statement_end_dt, 7) as stmt_mth,
    CASE 
        WHEN (ASSUMPTIONS_DELINQUENT_LAST_6_MONTHS_PASSED = FALSE OR
              ASSUMPTIONS_ACTIVE_LAST_6_MONTHS_PASSED = FALSE OR
              ASSUMPTIONS_OVERLIMIT_PASSED = FALSE OR
              ASSUMPTIONS_BLOCK_CD_PASSED = FALSE OR
              TRANSUNION_FICO_08_SCORE_PASSED = FALSE OR
              TRANSUNION_VANTAGE_30_SCORE_PASSED = FALSE OR
              TRANSUNION_TOTAL_REVOLVING_DEBT_PASSED = FALSE OR
              TRANSUNION_TOTAL_REVOLVING_DEBT_TO_INCOME_PASSED = FALSE OR
              TRANSUNION_NON_MORTGAGE_DEBT_TO_INCOME_PASSED = FALSE OR
              TRANSUNION_BANKCARD_TRADES_PASSED = FALSE OR
              TRANSUNION_TOTAL_TRADES_PASSED = FALSE OR
              TRANSUNION_REVOLVING_DEBT_VELOCITY_PASSED = FALSE) 
        THEN TRUE
        ELSE FALSE
    END AS eligibility_violation, -- This is the eligibility and hardcut rule in G-series policy
    CASE 
        WHEN (evaluation_risk_group > 11 -- CLIP Model Score <= G-Series risk group 11 to pass
              OR (statement_number = 7 AND evaluation_risk_group <= 8 AND evaluation_clip_amount > 1500) -- Statement 7 risk group 1-8 max CLIP of $1,500
              OR (statement_number = 11 AND evaluation_risk_group <= 8 AND evaluation_clip_amount > 2500) -- Statement 11 risk group 1-8 max CLIP of $2,500
              OR (statement_number IN (7, 11) AND evaluation_risk_group >= 9 AND evaluation_clip_amount > 500) -- Statement 7 risk group 9+ max CLIP of $500 & statement 11 risk group 9+ max CLIP of $500
              OR (statement_number IN (7, 11) AND pre_clip_credit_limit < 4000 AND pre_clip_credit_limit + evaluation_clip_amount > 5000)
              OR (statement_number IN (7, 11) AND pre_clip_credit_limit >= 4000 AND evaluation_clip_amount > 1000) --Y1 Max post-CLIP line of $5000 or ICL + $1000 (whichever is higher)
              OR (statement_number > 12 AND evaluation_risk_group <= 7 AND evaluation_clip_amount > 4000) -- Year 2+ Risk group 1-7 max CLIP of $4,000
              OR (statement_number > 12 AND evaluation_risk_group >= 8 AND evaluation_clip_amount > 2000) -- Year 2+ Risk group 8+ max CLIP of $2,000
              OR (statement_number > 12 AND pre_clip_credit_limit + evaluation_clip_amount > 8000) -- Year 2+ Max post-CLIP line of $8,000
             ) 
        THEN TRUE
        ELSE FALSE
    END AS max_clip_violation,
    COUNT(*) AS record_count
FROM
    datamart_db.dm_consolidated.account_statements_clip
WHERE
    evaluation_outcome = 'APPROVED'
GROUP BY
    1, 2, 3 -- Group by statement_month, eligibility_violation, AND max_clip_violation
ORDER BY
    1, 2, 3; -- It's good practice to order by the same columns

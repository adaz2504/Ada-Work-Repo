-- Final Metrics Calculation SQL
-- This script takes the output from monitoring.sql and calculates the final metrics
-- using the proper formulas defined in metrics logic

with monitoring_data as (
    -- This would be the output from monitoring.sql
    -- For now, we'll read from the existing results
    select * from (
        -- Insert monitoring.sql query here or read from saved results
        -- For this example, we'll assume the data is available
        select 
            retro_risk_series,
            statement_segment,
            npv_statement_number_segment,
            cohort,
            retro_risk_group,
            combined_util_group,
            is_in_in,
            is_swap_in,
            is_4k_plus,
            real_outcome,
            credit_limit_bucket,
            clip_amount_group_assumptions,
            statement_segment_new,
            eligibility_category,
            rewards_type,
            apr_type,
            statements_since_clip,
            
            -- Raw components from monitoring.sql
            original_statements,
            open_statements,
            charged_off_statements,
            chargeoffs_pbad,
            purchase_balance_open_accounts,
            total_balance_open_accounts,
            credit_limit_open_accounts,
            bkt2_accounts,
            voluntary_closures,
            principal_balance_chargedoff_accounts,
            credit_limit_chargedoff_accounts,
            cash_advance_takers,
            cash_advance_amount,
            late_fees,
            average_outstanding_balance_open_accounts,
            average_purchase_balance_open_accounts,
            
            -- Assumption columns
            pbad_per_open,
            utilization_per_open,
            final_purchase_volume_percentage,
            util_open_asmpt,
            severity_asmpt,
            close_open_asmpt,
            credit_line_per_open,
            cash_incidence_asmpt,
            penalty_fee_rate_asmpt,
            outstandings_per_original_asmpt,
            revolve_rate_asmpt
            
        from dm_consolidated.int_clip_performance_secondary_statements base
        left join dm_consolidated.int_clip_current_assumptions assumptions
            on base.retro_risk_series = assumptions.series
            and base.npv_statement_number_segment = assumptions.statement_number_segment
            and cast(base.retro_risk_group as varchar) = assumptions.risk_group
            and cast(base.combined_util_group as varchar) = assumptions.utilization_group
            and cast(base.credit_limit_bucket as varchar) = assumptions.credit_line_bucket
            and base.statements_since_clip = assumptions.statement_number
            and cast(base.clip_amount_group_assumptions as varchar) = assumptions.clip_amount_group
        where base.is_included_in_performance_metrics = true
          and base.statements_since_clip >= 3  -- Filter out very early statements
          and base.statements_since_clip <= 36  -- Focus on first 3 years
    ) 
)

select 
    -- Dimensional columns
    retro_risk_series,
    statement_segment,
    npv_statement_number_segment,
    cohort,
    retro_risk_group,
    combined_util_group,
    is_in_in,
    is_swap_in,
    is_4k_plus,
    real_outcome,
    credit_limit_bucket,
    clip_amount_group_assumptions,
    statement_segment_new,
    eligibility_category,
    rewards_type,
    apr_type,
    statements_since_clip,
    
    -- Raw components (for reference)
    original_statements,
    open_statements,
    charged_off_statements,
    bkt2_accounts,
    voluntary_closures,
    cash_advance_takers,
    late_fees,
    purchase_balance_open_accounts,
    total_balance_open_accounts,
    credit_limit_open_accounts,
    principal_balance_chargedoff_accounts,
    average_outstanding_balance_open_accounts,
    
    -- CALCULATED FINAL METRICS using proper formulas from metrics logic
    
    -- Pbad: CHARGE_OFFS_PBAD / OPEN_STATEMENTS (but using charged_off_statements directly)
    case when open_statements > 0 then charged_off_statements / open_statements else 0 end as pbad_actual,
    pbad_per_open as pbad_assumption,
    
    -- DQ30: BKT2_ACCOUNTS / OPEN_STATEMENTS
    case when open_statements > 0 then bkt2_accounts / open_statements else 0 end as dq30_actual,
    null as dq30_assumption,  -- No assumption data available
    
    -- Severity: PRINCIPAL_BALANCE_CHARGEDOFF_ACCOUNTS / CHARGED_OFF_STATEMENTS
    case when charged_off_statements > 0 then principal_balance_chargedoff_accounts / charged_off_statements else 0 end as severity_actual,
    severity_asmpt as severity_assumption,
    
    -- Utilization: TOTAL_BALANCE_OPEN_ACCOUNTS / CREDIT_LIMIT_OPEN_ACCOUNTS
    case when credit_limit_open_accounts > 0 then total_balance_open_accounts / credit_limit_open_accounts else 0 end as utilization_actual,
    utilization_per_open as utilization_assumption,
    
    -- Credit_Line: CREDIT_LIMIT_OPEN_ACCOUNTS / OPEN_STATEMENTS
    case when open_statements > 0 then credit_limit_open_accounts / open_statements else 0 end as credit_line_actual,
    credit_line_per_open as credit_line_assumption,
    
    -- Cash_Advance: CASH_ADVANCE_TAKERS / OPEN_STATEMENTS
    case when open_statements > 0 then cash_advance_takers / open_statements else 0 end as cash_advance_actual,
    cash_incidence_asmpt as cash_advance_assumption,
    
    -- Penalty: LATE_FEES / OPEN_STATEMENTS
    case when open_statements > 0 then late_fees / open_statements else 0 end as penalty_actual,
    penalty_fee_rate_asmpt as penalty_assumption,
    
    -- Pvol: PURCHASE_BALANCE_OPEN_ACCOUNTS / CREDIT_LIMIT_OPEN_ACCOUNTS
    case when credit_limit_open_accounts > 0 then purchase_balance_open_accounts / credit_limit_open_accounts else 0 end as pvol_actual,
    final_purchase_volume_percentage as pvol_assumption,
    
    -- Attrition: VOLUNTARY_CLOSURES / OPEN_STATEMENTS
    case when open_statements > 0 then voluntary_closures / open_statements else 0 end as attrition_actual,
    close_open_asmpt as attrition_assumption,
    
    -- Outstanding: AVERAGE_OUTSTANDING_BALANCE_OPEN_ACCOUNTS (absolute value)
    average_outstanding_balance_open_accounts as outstanding_actual,
    outstandings_per_original_asmpt as outstanding_assumption,
    
    -- Revolve_Rate: PURCHASE_BALANCE_OPEN_ACCOUNTS / AVERAGE_OUTSTANDING_BALANCE_OPEN_ACCOUNTS
    case when average_outstanding_balance_open_accounts > 0 then purchase_balance_open_accounts / average_outstanding_balance_open_accounts else 0 end as revolve_rate_actual,
    revolve_rate_asmpt as revolve_rate_assumption

from monitoring_data
where open_statements > 0  -- Only include records with actual open statements
order by random()
limit 1000;

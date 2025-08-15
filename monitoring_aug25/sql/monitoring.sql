-- Monitoring Enhancement SQL Script - Phase 2 Implementation
-- Based on original script structure with new dimensional splits and metrics
-- Using correct column names from dm_consolidated tables

-- Define dimensional mappings as CTEs
with dimensional_mappings as (
    select
        *,
        -- Statement Number Segment Function (Y1/Y2/Y3+)
        case 
            when initial_statement_number <= 12 then 'Y1'
            when initial_statement_number between 13 and 24 then 'Y2'
            else 'Y3+'
        end as statement_segment_new,
        
        -- Additional dimensions from existing data
        case when is_clip_eligible then 'Eligible' else 'Not Eligible' end as eligibility_category,
        case when rewards_rate > 0 then 'Rewards' else 'Non-Rewards' end as rewards_type,
        case when secondary_statement_prime_rate > 0.15 then 'High' else 'Low' end as apr_type
        
    from dm_consolidated.int_clip_performance_secondary_statements
    where is_included_in_performance_metrics = true
),

-- Pre-aggregate metrics with new dimensional splits
pre_aggregate_metrics as (
    select
        -- Original index columns
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
        
        -- New dimensional columns
        statement_segment_new,
        eligibility_category,
        rewards_type,
        apr_type,
        
        statements_since_clip,
        
        -- Original metrics
        count(*) as original_statements,
        sum(account_open_in_secondary_statement) as open_statements,
        sum(account_charged_off_in_secondary_statement) as charged_off_statements,
        sum(account_charged_off_in_secondary_statement) * 12 as chargeoffs_pbad,
        sum(account_open_in_secondary_statement * secondary_statement_purchase_balance) as purchase_balance_open_accounts,
        sum(account_open_in_secondary_statement * secondary_statement_total_balance) as total_balance_open_accounts,
        sum(account_open_in_secondary_statement * secondary_statement_credit_limit) as credit_limit_open_accounts,
        sum(account_open_in_secondary_statement * secondary_statement_is_bkt2) as bkt2_accounts,
        count_if(secondary_statement_voluntary_closure) as voluntary_closures,
        sum(account_charged_off_in_secondary_statement * secondary_statement_charge_off_principal_balance) as principal_balance_chargedoff_accounts,
        sum(account_charged_off_in_secondary_statement * secondary_statement_credit_limit) as credit_limit_chargedoff_accounts,
        sum(secondary_statement_cash_advance_taker_indicator) as cash_advance_takers,
        sum(secondary_statement_cash_advance_amount) as cash_advance_amount,
        sum(secondary_statement_late_fee_indicator) as late_fees,
        sum(account_open_in_secondary_statement * secondary_statement_average_outstanding_balance) as average_outstanding_balance_open_accounts,
        sum(account_open_in_secondary_statement * secondary_statement_average_purchase_balance) as average_purchase_balance_open_accounts,
        sum(account_open_in_secondary_statement * secondary_statement_average_credit_limit) as average_credit_limit_open_accounts

    from dimensional_mappings
    group by 
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
        statements_since_clip
),

-- Final monitoring view with assumptions
monitoring_enhanced as (
    select 
        metrics.*,
        
        -- Extending to 60 statements for assumptions (original logic)
        coalesce(metrics.open_statements, 
                 lag(metrics.open_statements) ignore nulls over (
                    partition by retro_risk_series, statement_segment, npv_statement_number_segment, cohort, 
                                retro_risk_group, combined_util_group, is_in_in, is_swap_in, is_4k_plus, 
                                real_outcome, credit_limit_bucket, clip_amount_group_assumptions
                    order by metrics.statements_since_clip)) as open_statements_assumptions,
                    
        coalesce(metrics.credit_limit_open_accounts,
                lag(metrics.credit_limit_open_accounts) ignore nulls over (
                    partition by retro_risk_series, statement_segment, npv_statement_number_segment, cohort, 
                                retro_risk_group, combined_util_group, is_in_in, is_swap_in, is_4k_plus, 
                                real_outcome, credit_limit_bucket, clip_amount_group_assumptions
                    order by metrics.statements_since_clip)) as credit_limit_open_accounts_assumptions,
                    
        coalesce(metrics.original_statements,
                lag(metrics.original_statements) ignore nulls over (
                    partition by retro_risk_series, statement_segment, npv_statement_number_segment, cohort, 
                                retro_risk_group, combined_util_group, is_in_in, is_swap_in, is_4k_plus, 
                                real_outcome, credit_limit_bucket, clip_amount_group_assumptions
                    order by metrics.statements_since_clip)) as original_statements_assumptions,
                    
        coalesce(metrics.average_outstanding_balance_open_accounts,
                lag(metrics.average_outstanding_balance_open_accounts) ignore nulls over (
                    partition by retro_risk_series, statement_segment, npv_statement_number_segment, cohort, 
                                retro_risk_group, combined_util_group, is_in_in, is_swap_in, is_4k_plus, 
                                real_outcome, credit_limit_bucket, clip_amount_group_assumptions
                    order by metrics.statements_since_clip)) as average_outstanding_balance_open_accounts_assumptions,
        
        -- Granular metrics with dimensional splits
        -- Pbad (Probability of Bad)
        case when open_statements > 0 then chargeoffs_pbad / open_statements else 0 end as pbad,
        
        -- Severity 
        case when charged_off_statements > 0 then principal_balance_chargedoff_accounts / charged_off_statements else 0 end as severity,
        
        -- Utilization
        case when credit_limit_open_accounts > 0 then total_balance_open_accounts / credit_limit_open_accounts else 0 end as utilization,
        
        -- Credit Line (average)
        case when open_statements > 0 then credit_limit_open_accounts / open_statements else 0 end as credit_line,
        
        -- Aggregate-only metrics (will be used in separate aggregate views)
        cash_advance_amount as cash_advance_aggregate,
        late_fees as penalty_aggregate,
        purchase_balance_open_accounts as pvol_aggregate,
        voluntary_closures as attrition_aggregate,
        average_outstanding_balance_open_accounts as outstanding_aggregate,
        
        -- Revolve rate calculation
        case when average_outstanding_balance_open_accounts > 0 
             then purchase_balance_open_accounts / average_outstanding_balance_open_accounts 
             else 0 end as revolve_aggregate,
             
        -- Join with assumptions for additional calculations
        assumptions.pbad_per_open,
        assumptions.utilization_per_open,
        assumptions.final_purchase_volume_percentage,
        assumptions.util_open_asmpt,
        assumptions.severity_asmpt,
        assumptions.close_open_asmpt,
        assumptions.credit_line_per_open,
        assumptions.cash_incidence_asmpt,
        assumptions.penalty_fee_rate_asmpt,
        assumptions.outstandings_per_original_asmpt,
        assumptions.revolve_rate_asmpt
        
    from pre_aggregate_metrics metrics
    left join dm_consolidated.int_clip_current_assumptions assumptions
        on metrics.retro_risk_series = assumptions.series
        and metrics.npv_statement_number_segment = assumptions.statement_number_segment
        and cast(metrics.retro_risk_group as varchar) = assumptions.risk_group
        and cast(metrics.combined_util_group as varchar) = assumptions.utilization_group
        and cast(metrics.credit_limit_bucket as varchar) = assumptions.credit_line_bucket
        and metrics.statements_since_clip = assumptions.statement_number
        and cast(metrics.clip_amount_group_assumptions as varchar) = assumptions.clip_amount_group
)

-- Final select with all enhanced monitoring metrics
select * from monitoring_enhanced
order by 
    retro_risk_series,
    statement_segment_new,
    retro_risk_group,
    combined_util_group,
    statements_since_clip
limit 100;

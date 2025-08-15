{% set index_columns = '
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
    clip_amount_group_assumptions'
%}

with pre_aggregate_metrics as (
    select
        {{ index_columns }},
        statements_since_clip,
        count(*) as original_statements,
        sum(account_open_in_secondary_statement) as open_statements,
        sum(account_charged_off_in_secondary_statement) as charged_off_statements,
        charged_off_statements * 12 as chargeoffs_pbad,
        sum(account_open_in_secondary_statement * secondary_statement_purchase_balance) as purchase_balance_open_accounts,
        sum(account_open_in_secondary_statement * secondary_statement_total_balance)  as total_balance_open_accounts,
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
    from {{ ref('int_clip_performance_secondary_statements') }}
    where is_included_in_performance_metrics
    group by all
)

select 
    metrics.* rename (statement_segment as initial_statement_number),
    -- Extending to 60 statements for assumptions
    coalesce(metrics.open_statements, 
             lag(metrics.open_statements) ignore nulls over (
                partition by {{ index_columns }}
                order by metrics.statements_since_clip)) as open_statements_assumptions,
    coalesce(metrics.credit_limit_open_accounts,
            lag(metrics.credit_limit_open_accounts) ignore nulls over (
                partition by {{ index_columns }}
                order by metrics.statements_since_clip)) as credit_limit_open_accounts_assumptions,
    coalesce(metrics.credit_limit_chargedoff_accounts,
            lag(metrics.credit_limit_chargedoff_accounts) ignore nulls over (
                partition by {{ index_columns }}
                order by metrics.statements_since_clip)) as credit_limit_chargedoff_accounts_assumptions, 
    coalesce(metrics.original_statements,
            lag(metrics.original_statements) ignore nulls over (
                partition by {{ index_columns }}
                order by metrics.statements_since_clip)) as original_statements_assumptions, 
    coalesce(metrics.average_outstanding_balance_open_accounts,
            lag(metrics.average_outstanding_balance_open_accounts) ignore nulls over (
                partition by {{ index_columns }}
                order by metrics.statements_since_clip)) as average_outstanding_balance_open_accounts_assumptions, 
    -- Pbad
    assumptions.pbad_per_open * open_statements_assumptions * 12 as chargeoffs_pbad_assumption, 
    -- LED, 0.65 and 0.91 are the average BKT2 that eventually CO and the COs that are not due to DQs, respectively
    coalesce(metrics.chargeoffs_pbad,
            lag(metrics.bkt2_accounts, {{ var('led_statements') }})
                over (partition by {{ index_columns }}
                      order by metrics.statements_since_clip) * 0.65 / 0.91 * 12) as chargeoffs_pbad_with_forecast,
    coalesce(metrics.open_statements,
            lag(metrics.open_statements, {{ var('led_statements') }})
                 over (partition by {{ index_columns }}
                       order by metrics.statements_since_clip)) as open_statements_with_forecast,
    -- Final Util
    assumptions.utilization_per_open * credit_limit_open_accounts_assumptions as total_balance_open_accounts_assumption,
    -- Base Util
    assumptions.util_open_asmpt * credit_limit_open_accounts_assumptions as total_balance_open_accounts_base_assumption,
    -- Pvol 
    assumptions.final_purchase_volume_percentage * credit_limit_open_accounts_assumptions as purchase_balance_open_accounts_assumption,
    -- Attrition per open
    assumptions.close_open_asmpt * open_statements_assumptions as voluntary_closures_assumption,
    -- Credit limit per open
    assumptions.credit_line_per_open * open_statements_assumptions as credit_limit_assumption,
    -- Severity
    assumptions.severity_asmpt * credit_limit_chargedoff_accounts_assumptions as principal_balance_chargeoff_assumption,
    -- Cash advance incidence (count)
    assumptions.cash_incidence_asmpt * open_statements_assumptions as cash_advance_takers_assumption,
    -- Penalty fee incidence
    assumptions.penalty_fee_rate_asmpt * open_statements_assumptions as late_fees_assumption,
    -- Outstandings per original
    assumptions.outstandings_per_original_asmpt * original_statements_assumptions as outstandings_original_assumption,
    -- Revolve rate
    assumptions.revolve_rate_asmpt * average_outstanding_balance_open_accounts_assumptions as revolving_purchase_balance_assumption
from pre_aggregate_metrics metrics
left join {{ ref('int_clip_current_assumptions') }} assumptions
    on metrics.retro_risk_series = assumptions.series
    and metrics.npv_statement_number_segment = assumptions.statement_number_segment
    and metrics.retro_risk_group = assumptions.risk_group
    and metrics.combined_util_group = assumptions.utilization_group
    and metrics.credit_limit_bucket = assumptions.credit_line_bucket
    and metrics.statements_since_clip = assumptions.statement_number
    and metrics.clip_amount_group_assumptions = assumptions.clip_amount_group
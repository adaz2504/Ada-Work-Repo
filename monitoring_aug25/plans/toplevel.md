# Monitoring Enhancement Project - Top Level Plan

## Project Overview
This project aims to enhance the existing monitoring capabilities by creating new granular and aggregate monitoring views based on the current Tableau dashboard SQL script. The goal is to provide more detailed insights into credit card performance metrics with specific dimensional splits.

## Current State Analysis
- **Existing Dashboard**: Built on complex SQL aggregation tracking credit card performance metrics
- **Key Dimensions**: Risk series, statement segments, risk groups, util groups, credit limits, clip amounts
- **Core Metrics**: Charge-offs, utilization, purchase volumes, attrition, cash advances, late fees

## Requirements Summary

### 1. Granular Views (with dimensional splits)
These metrics will be segmented by multiple dimensions:

**Dimensional Splitters:**
- **Statement Number Segments**: Y1 (≤12), Y2 (13-24), Y3+ (>24)
- **Pre Clip Line (PCL)**: 300, 500, 1000, 2000, 4000, 6000, 8000
- **Risk Group**: All 15 risk groups (RG 1-15)
- **Util Group**: All 10 util groups (UG 1-10)
- **Eligibility**: Various eligibility categories
- **Rewards Type**: Rewards vs Non-rewards
- **APR Type**: High vs Low

**Granular Metrics:**
- Pbad (Probability of Bad)
- Severity
- Utilization
- DQ 30 Rate (30-day delinquency rate)
- Marginal Utility / Utility (incremental outstanding/clip amount)
- Credit Line
- NIAT / Principal Outstanding
- GACO / Principal Outstanding

### 2. Aggregate-Only Views
These metrics will only be shown in aggregate form:
- Cash Advance
- Penalty
- Pvol (Purchase Volume)
- Attrition
- Revolve
- Outstanding

## Implementation Plan

### Phase 1: Data Model Design
1. **Analyze Current Schema**
   - Map existing tables and relationships from the original SQL script
   - Identify data sources for new dimensional splits
   - Document current metric calculations in `int_clip_performance_secondary_statements`

2. **Design New Dimensional Tables**
   - Create statement number segment mapping function (Y1/Y2/Y3+)
   - Build PCL buckets (300, 500, 1000, 2000, 4000, 6000, 8000)
   - Map all 15 risk groups individually
   - Map all 10 util groups individually
   - Identify eligibility categories from existing data
   - Create rewards type classification logic
   - Define APR type buckets (high/low)

3. **Create Base Views Structure**
   - Design fact table structure for granular metrics
   - Design aggregate table structure for aggregate-only metrics
   - Plan indexing strategy for performance

### Phase 2: SQL Development
1. **Dimensional Mapping Functions**
   ```sql
   -- Statement Number Segment Function
   CASE 
     WHEN statement_number <= 12 THEN 'Y1'
     WHEN statement_number BETWEEN 13 AND 24 THEN 'Y2'
     ELSE 'Y3+'
   END as statement_segment
   
   -- PCL Bucket Function
   CASE 
     WHEN pre_clip_line <= 300 THEN '300'
     WHEN pre_clip_line <= 500 THEN '500'
     WHEN pre_clip_line <= 1000 THEN '1000'
     WHEN pre_clip_line <= 2000 THEN '2000'
     WHEN pre_clip_line <= 4000 THEN '4000'
     WHEN pre_clip_line <= 6000 THEN '6000'
     WHEN pre_clip_line <= 8000 THEN '8000'
     ELSE '8000+'
   END as pcl_bucket
   ```

2. **Granular Metrics Views**
   - `vw_granular_pbad`: Pbad calculations with all dimensional splits
   - `vw_granular_severity`: Severity metrics with dimensional splits
   - `vw_granular_utilization`: Utilization metrics with dimensional splits
   - `vw_granular_dq30`: 30-day delinquency rates with dimensional splits
   - `vw_granular_marginal_utility`: Marginal utility calculations (incremental outstanding/clip amount)
   - `vw_granular_credit_line`: Credit line metrics
   - `vw_granular_niat_ratio`: NIAT/Principal Outstanding ratios
   - `vw_granular_gaco_ratio`: GACO/Principal Outstanding ratios

3. **Aggregate Views**
   - `vw_aggregate_cash_advance`: Cash advance totals
   - `vw_aggregate_penalty`: Penalty fee totals
   - `vw_aggregate_pvol`: Purchase volume totals
   - `vw_aggregate_attrition`: Attrition rates
   - `vw_aggregate_revolve`: Revolve rates
   - `vw_aggregate_outstanding`: Outstanding balances

### Phase 3: View Implementation
1. **Create Staging Tables**
   - Build intermediate staging tables for complex calculations
   - Implement data quality checks
   - Create refresh procedures

2. **Implement Granular Views**
   - Build each granular view with proper dimensional joins
   - Implement marginal utility calculations (incremental outstanding/clip amount)
   - Add all dimensional splits (Statement Number, PCL, RG 1-15, UG 1-10, Eligibility, Rewards Type, APR Type)
   - Add performance optimizations

3. **Implement Aggregate Views**
   - Create aggregate-only views without dimensional splits
   - Ensure consistency with existing dashboard metrics
   - Add data validation

### Phase 4: Testing & Validation
1. **Data Validation**
   - Compare new views with existing dashboard metrics
   - Validate dimensional splits are working correctly
   - Test edge cases and boundary conditions
   - Verify all 15 risk groups and 10 util groups are properly represented

2. **Performance Testing**
   - Measure query performance for each view
   - Optimize slow-running queries
   - Implement caching strategies if needed

3. **User Acceptance Testing**
   - Create sample reports using new views
   - Validate business logic with stakeholders
   - Document any discrepancies

### Phase 5: Documentation & Deployment
1. **Technical Documentation**
   - Document all view definitions
   - Create data dictionary for new dimensions
   - Write maintenance procedures

2. **User Documentation**
   - Create user guide for new monitoring views
   - Document dimensional definitions and business rules
   - Provide example queries and use cases

3. **Deployment**
   - Deploy to production environment
   - Update Tableau dashboard connections
   - Train end users on new capabilities

## Technical Implementation Details

### Data Sources
- Primary source: `int_clip_performance_secondary_statements` table
- Assumptions source: `int_clip_current_assumptions` table
- Additional reference tables may be needed for new dimensions (eligibility, rewards type, APR type)

### Key Calculations
- **Marginal Utility**: incremental outstanding / clip amount
- **NIAT Ratio**: NIAT / principal outstanding
- **GACO Ratio**: GACO / principal outstanding
- **DQ 30 Rate**: 30-day delinquency calculations
- **Pbad**: Probability of bad calculations from existing logic

### Performance Optimization
- Implement materialized views for frequently accessed data
- Create appropriate indexes on dimensional columns
- Consider partitioning strategies for large datasets

### Data Quality
- Implement data validation rules for new dimensional splits
- Create monitoring for data freshness and completeness
- Establish alerting for data quality issues

## Success Criteria
1. All granular views provide accurate metrics split by required dimensions
2. All 15 risk groups and 10 util groups are properly represented
3. Aggregate views maintain consistency with existing dashboard
4. Query performance meets business requirements
5. New views integrate seamlessly with existing Tableau dashboard
6. Marginal utility calculations are accurate (incremental outstanding/clip amount)

## Risk Mitigation
- **Data Inconsistency**: Implement comprehensive validation against existing metrics
- **Performance Issues**: Plan for incremental loading and caching strategies
- **Complex Dimensional Logic**: Thoroughly test all dimensional splits
- **Maintenance Overhead**: Automate refresh procedures and monitoring

## Execution Notes for Implementation
This plan will be executed step-by-step using available tools to:
1. Analyze existing database structure and connections
2. Create SQL scripts for dimensional mappings
3. Build and test each view incrementally
4. Validate results against existing dashboard
5. Document and deploy final solution

The implementation will leverage the existing `dn_connector.py` for database connectivity and build upon the current SQL structure in `original_sql_script.txt`.

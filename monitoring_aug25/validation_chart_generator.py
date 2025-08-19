#!/usr/bin/env python3
"""
Validation Chart Generator
Creates actual vs assumption validation charts for the 10 key metrics
"""

import pandas as pd
import matplotlib.pyplot as plt
import matplotlib
import numpy as np
import os
import sys
from dn_connector import get_snowflake_connection

# Set matplotlib to non-interactive backend
matplotlib.use('Agg')

def read_sql_file(file_path):
    """Read SQL query from file"""
    try:
        with open(file_path, 'r') as file:
            return file.read()
    except FileNotFoundError:
        print(f"Error: SQL file '{file_path}' not found.")
        sys.exit(1)
    except Exception as e:
        print(f"Error reading SQL file: {e}")
        sys.exit(1)

def connect_to_database():
    """Connect to Snowflake database using existing dn_connector"""
    try:
        print("Connecting to Snowflake database...")
        connection = get_snowflake_connection()
        print("✅ Database connection successful!")
        return connection
    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        sys.exit(1)

def execute_query(connection, sql_query):
    """Execute SQL query and return DataFrame"""
    try:
        print("Executing SQL query...")
        cursor = connection.cursor()
        cursor.execute(sql_query)
        
        # Fetch results and column names
        results = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        
        # Create DataFrame
        df = pd.DataFrame(results, columns=columns)
        cursor.close()
        
        print(f"✅ Query executed successfully! Retrieved {len(df)} rows and {len(df.columns)} columns.")
        return df
        
    except Exception as e:
        print(f"❌ Query execution failed: {e}")
        connection.close()
        sys.exit(1)

def convert_decimals_to_float(df):
    """Convert Decimal columns to float to avoid calculation errors"""
    print("🔄 Converting Decimal columns to float...")
    
    import decimal
    for col in df.columns:
        if df[col].dtype == 'object':
            # Check if column contains Decimal objects
            sample_val = df[col].dropna().iloc[0] if not df[col].dropna().empty else None
            if isinstance(sample_val, decimal.Decimal):
                df[col] = pd.to_numeric(df[col], errors='coerce')
                print(f"  • Converted {col} from Decimal to float")
    
    return df

def calculate_metrics(df):
    """Calculate the 10 key metrics according to metrics logic"""
    print("\n🧮 Calculating key metrics...")
    
    # Create a copy to avoid modifying original DataFrame
    df_with_metrics = df.copy()
    
    # Convert Decimal columns to float first
    df_with_metrics = convert_decimals_to_float(df_with_metrics)
    
    try:
        # Helper function to safely replace zeros with NaN
        def safe_divide(numerator, denominator):
            denom_safe = denominator.replace(0, float('nan'))
            return numerator / denom_safe
        
        # 1. Pbad per open: charged_off_statements/open_statements
        df_with_metrics['ACTUAL_PBAD_PER_OPEN'] = safe_divide(
            df_with_metrics['CHARGED_OFF_STATEMENTS'], 
            df_with_metrics['OPEN_STATEMENTS']
        )
        
        # 2. Severity: principal_balance_chargedoff_accounts/credit_limit_chargedoff_accounts
        df_with_metrics['ACTUAL_SEVERITY'] = safe_divide(
            df_with_metrics['PRINCIPAL_BALANCE_CHARGEDOFF_ACCOUNTS'], 
            df_with_metrics['CREDIT_LIMIT_CHARGEDOFF_ACCOUNTS']
        )
        
        # 3. Util: principal_balance_open_accounts/credit_limit_open_accounts
        if 'PRINCIPAL_BALANCE_OPEN_ACCOUNTS' in df_with_metrics.columns:
            df_with_metrics['ACTUAL_UTIL'] = safe_divide(
                df_with_metrics['PRINCIPAL_BALANCE_OPEN_ACCOUNTS'], 
                df_with_metrics['CREDIT_LIMIT_OPEN_ACCOUNTS']
            )
        elif 'TOTAL_BALANCE_OPEN_ACCOUNTS' in df_with_metrics.columns:
            df_with_metrics['ACTUAL_UTIL'] = safe_divide(
                df_with_metrics['TOTAL_BALANCE_OPEN_ACCOUNTS'], 
                df_with_metrics['CREDIT_LIMIT_OPEN_ACCOUNTS']
            )
        
        # 4. DQ30: bkt2_accounts/open_statements
        df_with_metrics['ACTUAL_DQ30'] = safe_divide(
            df_with_metrics['BKT2_ACCOUNTS'], 
            df_with_metrics['OPEN_STATEMENTS']
        )
        
        # 5. Credit Line: credit_limit_open_accounts/open_statements
        df_with_metrics['ACTUAL_CREDIT_LINE'] = safe_divide(
            df_with_metrics['CREDIT_LIMIT_OPEN_ACCOUNTS'], 
            df_with_metrics['OPEN_STATEMENTS']
        )
        
        # 6. Cash Advance: cash_advance_takers/open_statements
        df_with_metrics['ACTUAL_CASH_ADVANCE'] = safe_divide(
            df_with_metrics['CASH_ADVANCE_TAKERS'], 
            df_with_metrics['OPEN_STATEMENTS']
        )
        
        # 7. Penalty: late_fees/open_statements
        df_with_metrics['ACTUAL_PENALTY'] = safe_divide(
            df_with_metrics['LATE_FEES'], 
            df_with_metrics['OPEN_STATEMENTS']
        )
        
        # 8. Pvol: purchase_balance_open_accounts/total_balance_open_accounts
        df_with_metrics['ACTUAL_PVOL'] = safe_divide(
            df_with_metrics['PURCHASE_BALANCE_OPEN_ACCOUNTS'], 
            df_with_metrics['TOTAL_BALANCE_OPEN_ACCOUNTS']
        )
        
        # 9. Attrition: voluntary_closures/open_statements
        df_with_metrics['ACTUAL_ATTRITION'] = safe_divide(
            df_with_metrics['VOLUNTARY_CLOSURES'], 
            df_with_metrics['OPEN_STATEMENTS']
        )
        
        # 10. Outstanding: average_outstanding_balance_open_accounts
        df_with_metrics['ACTUAL_OUTSTANDING'] = df_with_metrics['AVERAGE_OUTSTANDING_BALANCE_OPEN_ACCOUNTS']
        
        print("✅ Key metrics calculated successfully!")
        
        return df_with_metrics
        
    except Exception as e:
        print(f"❌ Error calculating metrics: {e}")
        print("Proceeding with original DataFrame...")
        return df

def create_validation_chart(df, metric_name, actual_col, assumption_col, output_dir):
    """Create validation chart comparing actual vs assumption for a metric"""
    try:
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6))
        
        # Chart 1: Actual values
        actual_data = df[actual_col].dropna()
        if len(actual_data) > 0:
            ax1.hist(actual_data, bins=30, alpha=0.7, color='skyblue', edgecolor='black')
            ax1.set_title(f'{metric_name} - Actual\nMean: {actual_data.mean():.4f}, Count: {len(actual_data)}')
            ax1.set_xlabel(f'Actual {metric_name}')
            ax1.set_ylabel('Frequency')
            ax1.grid(True, alpha=0.3)
        else:
            ax1.text(0.5, 0.5, 'No actual data available', ha='center', va='center', transform=ax1.transAxes)
            ax1.set_title(f'{metric_name} - Actual (No Data)')
        
        # Chart 2: Assumption values
        if assumption_col and assumption_col in df.columns:
            assumption_data = df[assumption_col].dropna()
            if len(assumption_data) > 0:
                ax2.hist(assumption_data, bins=30, alpha=0.7, color='lightcoral', edgecolor='black')
                ax2.set_title(f'{metric_name} - Assumption\nMean: {assumption_data.mean():.4f}, Count: {len(assumption_data)}')
                ax2.set_xlabel(f'Assumption {metric_name}')
                ax2.set_ylabel('Frequency')
                ax2.grid(True, alpha=0.3)
            else:
                ax2.text(0.5, 0.5, 'No assumption data available', ha='center', va='center', transform=ax2.transAxes)
                ax2.set_title(f'{metric_name} - Assumption (No Data)')
        else:
            ax2.text(0.5, 0.5, 'No assumption column found', ha='center', va='center', transform=ax2.transAxes)
            ax2.set_title(f'{metric_name} - Assumption (Not Available)')
        
        plt.tight_layout()
        
        # Save the plot
        filename = f"{metric_name}_validation.png"
        filepath = os.path.join(output_dir, filename)
        plt.savefig(filepath, dpi=300, bbox_inches='tight')
        plt.close()
        
        print(f"  📊 Created validation chart: {filename}")
        
    except Exception as e:
        print(f"  ❌ Failed to create validation chart for {metric_name}: {e}")
        plt.close()

def create_dq30_chart(df, output_dir):
    """Create DQ30 chart (actual only, no assumption)"""
    try:
        plt.figure(figsize=(8, 6))
        
        actual_data = df['ACTUAL_DQ30'].dropna()
        if len(actual_data) > 0:
            plt.hist(actual_data, bins=30, alpha=0.7, color='skyblue', edgecolor='black')
            plt.title(f'DQ30 - Actual Only\nMean: {actual_data.mean():.4f}, Count: {len(actual_data)}')
            plt.xlabel('Actual DQ30')
            plt.ylabel('Frequency')
            plt.grid(True, alpha=0.3)
        else:
            plt.text(0.5, 0.5, 'No DQ30 data available', ha='center', va='center', transform=plt.gca().transAxes)
            plt.title('DQ30 - Actual (No Data)')
        
        # Save the plot
        filename = "DQ30_validation.png"
        filepath = os.path.join(output_dir, filename)
        plt.savefig(filepath, dpi=300, bbox_inches='tight')
        plt.close()
        
        print(f"  📊 Created DQ30 chart: {filename}")
        
    except Exception as e:
        print(f"  ❌ Failed to create DQ30 chart: {e}")
        plt.close()

def generate_validation_charts(df, output_dir):
    """Generate validation charts for all 10 key metrics"""
    print(f"\n📈 Generating validation charts for 10 key metrics...")
    
    # Create output directory
    os.makedirs(output_dir, exist_ok=True)
    
    # Define metric mappings (actual column, assumption column if available)
    metrics = [
        ("PBAD_PER_OPEN", "ACTUAL_PBAD_PER_OPEN", "PBAD"),
        ("SEVERITY", "ACTUAL_SEVERITY", "SEVERITY"),
        ("UTIL", "ACTUAL_UTIL", "UTILIZATION"),
        ("CREDIT_LINE", "ACTUAL_CREDIT_LINE", "CREDIT_LINE"),
        ("CASH_ADVANCE", "ACTUAL_CASH_ADVANCE", "CASH_ADVANCE_AGGREGATE"),
        ("PENALTY", "ACTUAL_PENALTY", "PENALTY_AGGREGATE"),
        ("PVOL", "ACTUAL_PVOL", "PVOL_AGGREGATE"),
        ("ATTRITION", "ACTUAL_ATTRITION", "ATTRITION_AGGREGATE"),
        ("OUTSTANDING", "ACTUAL_OUTSTANDING", "OUTSTANDING_AGGREGATE")
    ]
    
    # Generate validation charts for 9 metrics (excluding DQ30)
    for metric_name, actual_col, assumption_col in metrics:
        print(f"\nProcessing {metric_name}...")
        create_validation_chart(df, metric_name, actual_col, assumption_col, output_dir)
    
    # Generate DQ30 chart (actual only)
    print(f"\nProcessing DQ30...")
    create_dq30_chart(df, output_dir)
    
    print(f"\n📊 Validation chart generation complete!")
    print(f"  • Total charts created: 10")

def main():
    """Main function"""
    print("🚀 Starting Validation Chart Generator")
    print("=" * 50)
    
    # Configuration
    sql_file_path = "sql/monitoring.sql"
    output_directory = "./dq_charts_final"
    
    # Step 1: Read SQL query from file
    print(f"📖 Reading SQL query from: {sql_file_path}")
    sql_query = read_sql_file(sql_file_path)
    
    # Step 2: Connect to database
    connection = connect_to_database()
    
    try:
        # Step 3: Execute query and load data
        df = execute_query(connection, sql_query)
        
        # Save raw SQL output for troubleshooting
        df.to_csv('raw_sql_output.csv', index=False)
        print("✅ Saved raw SQL output to raw_sql_output.csv")
        
        # Step 4: Calculate metrics
        df_with_metrics = calculate_metrics(df)
        
        # Save calculated metrics for troubleshooting
        df_with_metrics.to_csv('calculated_metrics.csv', index=False)
        print("✅ Saved calculated metrics to calculated_metrics.csv")
        
        # Step 5: Generate validation charts
        generate_validation_charts(df_with_metrics, output_directory)
        
    finally:
        # Step 6: Close database connection
        print("\n🔐 Closing database connection...")
        connection.close()
        print("✅ Database connection closed.")
    
    # Step 7: Print completion message
    print("\n" + "=" * 50)
    print("🎉 VALIDATION CHARTS CREATED SUCCESSFULLY!")
    print(f"📁 Charts saved to: {os.path.abspath(output_directory)}")
    print("=" * 50)

if __name__ == "__main__":
    main()

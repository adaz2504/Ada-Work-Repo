#!/usr/bin/env python3
"""
Data Quality Visualizer Script
Connects to Snowflake, executes SQL query, and generates data quality charts
"""

import pandas as pd
import matplotlib.pyplot as plt
import matplotlib
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

def create_output_directory(dir_path):
    """Create output directory for charts"""
    try:
        os.makedirs(dir_path, exist_ok=True)
        print(f"✅ Output directory '{dir_path}' ready.")
    except Exception as e:
        print(f"❌ Failed to create directory '{dir_path}': {e}")
        sys.exit(1)

def is_numeric_column(series):
    """Check if a pandas series contains numeric data"""
    return pd.api.types.is_numeric_dtype(series)

def is_categorical_column(series, max_unique=50):
    """Check if a pandas series should be treated as categorical"""
    return series.nunique() <= max_unique and series.nunique() > 1

def create_range_groups(series, num_groups=8):
    """Create range-based groups for high-cardinality numeric columns"""
    try:
        clean_data = series.dropna()
        if len(clean_data) == 0:
            return pd.Series(dtype='category')
        
        min_val = clean_data.min()
        max_val = clean_data.max()
        
        # Create range boundaries
        range_size = (max_val - min_val) / num_groups
        
        # Create labels for ranges
        range_labels = []
        for i in range(num_groups):
            start = min_val + i * range_size
            end = min_val + (i + 1) * range_size
            if i == num_groups - 1:  # Last group includes max value
                range_labels.append(f"{start:.0f}-{end:.0f}")
            else:
                range_labels.append(f"{start:.0f}-{end:.0f}")
        
        # Create bins and assign to groups
        bins = [min_val + i * range_size for i in range(num_groups + 1)]
        bins[-1] = max_val + 0.001  # Ensure max value is included
        
        # Create categorical groups
        groups = pd.cut(series, bins=bins, labels=range_labels, include_lowest=True)
        
        return groups
        
    except Exception as e:
        print(f"  ⚠️  Error creating range groups: {e}")
        return pd.Series(dtype='category')

def create_range_bar_chart(series, column_name, output_dir):
    """Create and save bar chart for high-cardinality numeric column using ranges"""
    try:
        plt.figure(figsize=(12, 6))
        
        # Create range groups
        range_groups = create_range_groups(series, num_groups=8)
        
        if len(range_groups.dropna()) == 0:
            plt.text(0.5, 0.5, 'No data to plot', ha='center', va='center', transform=plt.gca().transAxes)
        else:
            # Get value counts for ranges
            value_counts = range_groups.value_counts().sort_index()
            
            # Create bar chart
            bars = plt.bar(range(len(value_counts)), value_counts.values, color='lightgreen', alpha=0.7)
            
            # Customize the plot
            plt.title(f'{column_name} Distribution by Ranges\nTotal Records: {len(series.dropna())}')
            plt.xlabel(f'{column_name} Ranges')
            plt.ylabel('Count')
            
            # Set x-axis labels
            plt.xticks(range(len(value_counts)), value_counts.index, rotation=45, ha='right')
            
            # Add value labels on bars
            for bar, count in zip(bars, value_counts.values):
                plt.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.01*max(value_counts.values),
                        str(count), ha='center', va='bottom')
            
            plt.grid(True, alpha=0.3, axis='y')
        
        # Save the plot
        filename = f"{column_name}_ranges_barchart.png"
        filepath = os.path.join(output_dir, filename)
        plt.savefig(filepath, dpi=300, bbox_inches='tight')
        plt.close()
        
        print(f"  📊 Created range bar chart: {filename}")
        
    except Exception as e:
        print(f"  ❌ Failed to create range bar chart for {column_name}: {e}")
        plt.close()

def create_histogram(series, column_name, output_dir):
    """Create and save histogram for numeric column"""
    try:
        plt.figure(figsize=(10, 6))
        
        # Remove null values for plotting
        clean_data = series.dropna()
        
        if len(clean_data) == 0:
            plt.text(0.5, 0.5, 'No data to plot', ha='center', va='center', transform=plt.gca().transAxes)
        else:
            plt.hist(clean_data, bins=30, alpha=0.7, color='skyblue', edgecolor='black')
            
            # Add statistics to title
            mean_val = clean_data.mean()
            min_val = clean_data.min()
            max_val = clean_data.max()
            
            plt.title(f'{column_name} Distribution\nMean: {mean_val:.4f}, Min: {min_val:.4f}, Max: {max_val:.4f}')
            plt.xlabel(column_name)
            plt.ylabel('Frequency')
            plt.grid(True, alpha=0.3)
        
        # Save the plot
        filename = f"{column_name}_histogram.png"
        filepath = os.path.join(output_dir, filename)
        plt.savefig(filepath, dpi=300, bbox_inches='tight')
        plt.close()
        
        print(f"  📊 Created histogram: {filename}")
        
    except Exception as e:
        print(f"  ❌ Failed to create histogram for {column_name}: {e}")
        plt.close()

def create_bar_chart(series, column_name, output_dir):
    """Create and save bar chart for categorical column"""
    try:
        plt.figure(figsize=(12, 6))
        
        # Get value counts
        value_counts = series.value_counts()
        
        if len(value_counts) == 0:
            plt.text(0.5, 0.5, 'No data to plot', ha='center', va='center', transform=plt.gca().transAxes)
        else:
            # Create bar chart
            bars = plt.bar(range(len(value_counts)), value_counts.values, color='lightcoral', alpha=0.7)
            
            # Customize the plot
            plt.title(f'{column_name} Value Counts\nUnique Values: {len(value_counts)}')
            plt.xlabel(column_name)
            plt.ylabel('Count')
            
            # Set x-axis labels
            plt.xticks(range(len(value_counts)), value_counts.index, rotation=45, ha='right')
            
            # Add value labels on bars
            for bar, count in zip(bars, value_counts.values):
                plt.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.01*max(value_counts.values),
                        str(count), ha='center', va='bottom')
            
            plt.grid(True, alpha=0.3, axis='y')
        
        # Save the plot
        filename = f"{column_name}_barchart.png"
        filepath = os.path.join(output_dir, filename)
        plt.savefig(filepath, dpi=300, bbox_inches='tight')
        plt.close()
        
        print(f"  📊 Created bar chart: {filename}")
        
    except Exception as e:
        print(f"  ❌ Failed to create bar chart for {column_name}: {e}")
        plt.close()

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

def calculate_tableau_metrics(df):
    """Calculate final metrics according to Tableau logic"""
    print("\n🧮 Calculating Tableau metrics...")
    
    # Create a copy to avoid modifying original DataFrame
    df_with_metrics = df.copy()
    
    # Convert Decimal columns to float first
    df_with_metrics = convert_decimals_to_float(df_with_metrics)
    
    try:
        # Helper function to safely replace zeros with NaN
        def safe_divide(numerator, denominator):
            denom_safe = denominator.replace(0, float('nan'))
            return numerator / denom_safe
        
        # Pbad per open: charged_off_statements/open_statements
        df_with_metrics['TABLEAU_PBAD_PER_OPEN'] = safe_divide(
            df_with_metrics['CHARGED_OFF_STATEMENTS'], 
            df_with_metrics['OPEN_STATEMENTS']
        )
        
        # Severity: principal_balance_chargedoff_accounts/credit_limit_chargedoff_accounts
        df_with_metrics['TABLEAU_SEVERITY'] = safe_divide(
            df_with_metrics['PRINCIPAL_BALANCE_CHARGEDOFF_ACCOUNTS'], 
            df_with_metrics['CREDIT_LIMIT_CHARGEDOFF_ACCOUNTS']
        )
        
        # Util: principal_balance_open_accounts/credit_limit_open_accounts (note: fixing typo in logic file)
        if 'PRINCIPAL_BALANCE_OPEN_ACCOUNTS' in df_with_metrics.columns:
            df_with_metrics['TABLEAU_UTIL'] = safe_divide(
                df_with_metrics['PRINCIPAL_BALANCE_OPEN_ACCOUNTS'], 
                df_with_metrics['CREDIT_LIMIT_OPEN_ACCOUNTS']
            )
        elif 'TOTAL_BALANCE_OPEN_ACCOUNTS' in df_with_metrics.columns:
            # Use total balance if principal balance not available
            df_with_metrics['TABLEAU_UTIL'] = safe_divide(
                df_with_metrics['TOTAL_BALANCE_OPEN_ACCOUNTS'], 
                df_with_metrics['CREDIT_LIMIT_OPEN_ACCOUNTS']
            )
        
        # DQ30: bkt2_accounts/open_statements
        df_with_metrics['TABLEAU_DQ30'] = safe_divide(
            df_with_metrics['BKT2_ACCOUNTS'], 
            df_with_metrics['OPEN_STATEMENTS']
        )
        
        # Credit Line: credit_limit_open_accounts/open_statements
        df_with_metrics['TABLEAU_CREDIT_LINE'] = safe_divide(
            df_with_metrics['CREDIT_LIMIT_OPEN_ACCOUNTS'], 
            df_with_metrics['OPEN_STATEMENTS']
        )
        
        # Cash Advance: cash_advance_takers/open_statements
        df_with_metrics['TABLEAU_CASH_ADVANCE'] = safe_divide(
            df_with_metrics['CASH_ADVANCE_TAKERS'], 
            df_with_metrics['OPEN_STATEMENTS']
        )
        
        # Penalty: late_fees/open_statements
        df_with_metrics['TABLEAU_PENALTY'] = safe_divide(
            df_with_metrics['LATE_FEES'], 
            df_with_metrics['OPEN_STATEMENTS']
        )
        
        # Pvol: purchase_balance_open_accounts/total_balance_open_accounts
        df_with_metrics['TABLEAU_PVOL'] = safe_divide(
            df_with_metrics['PURCHASE_BALANCE_OPEN_ACCOUNTS'], 
            df_with_metrics['TOTAL_BALANCE_OPEN_ACCOUNTS']
        )
        
        # Attrition: voluntary_closures/open_statements
        df_with_metrics['TABLEAU_ATTRITION'] = safe_divide(
            df_with_metrics['VOLUNTARY_CLOSURES'], 
            df_with_metrics['OPEN_STATEMENTS']
        )
        
        # Outstanding: average_outstanding_balance_open_accounts (no calculation needed)
        df_with_metrics['TABLEAU_OUTSTANDING'] = df_with_metrics['AVERAGE_OUTSTANDING_BALANCE_OPEN_ACCOUNTS']
        
        # Note: Revolve Rate ignored as requested
        
        print("✅ Tableau metrics calculated successfully!")
        
        # Print summary of new metrics
        new_metrics = [col for col in df_with_metrics.columns if col.startswith('TABLEAU_')]
        print(f"  • Added {len(new_metrics)} new metric columns:")
        for metric in new_metrics:
            print(f"    - {metric}")
        
        return df_with_metrics
        
    except Exception as e:
        print(f"❌ Error calculating Tableau metrics: {e}")
        print("Proceeding with original DataFrame...")
        return df

def generate_charts(df, output_dir):
    """Generate charts for all columns in DataFrame"""
    print(f"\n📈 Generating charts for {len(df.columns)} columns...")
    
    histogram_count = 0
    bar_chart_count = 0
    range_chart_count = 0
    skipped_count = 0
    
    for column_name in df.columns:
        print(f"\nProcessing column: {column_name}")
        series = df[column_name]
        
        # Skip columns with all null values
        if series.isna().all():
            print(f"  ⚠️  Skipping {column_name}: All values are null")
            skipped_count += 1
            continue
        
        # Handle boolean columns by converting to int
        if series.dtype == 'bool':
            series = series.astype(int)
            print(f"  🔄 Converted boolean column {column_name} to integer")
        
        # Check if numeric
        if is_numeric_column(series):
            # For high-cardinality numeric columns, use range-based charts
            if series.nunique() > 50:
                create_range_bar_chart(series, column_name, output_dir)
                range_chart_count += 1
            else:
                create_histogram(series, column_name, output_dir)
                histogram_count += 1
        
        # Check if categorical (low cardinality)
        elif is_categorical_column(series):
            create_bar_chart(series, column_name, output_dir)
            bar_chart_count += 1
        
        else:
            # For very high cardinality non-numeric columns, still skip
            print(f"  ⚠️  Skipping {column_name}: Too many unique values ({series.nunique()}) for visualization")
            skipped_count += 1
    
    print(f"\n📊 Chart generation complete!")
    print(f"  • Histograms created: {histogram_count}")
    print(f"  • Bar charts created: {bar_chart_count}")
    print(f"  • Range charts created: {range_chart_count}")
    print(f"  • Columns skipped: {skipped_count}")
    print(f"  • Total charts: {histogram_count + bar_chart_count + range_chart_count}")

def main():
    """Main function"""
    print("🚀 Starting Data Quality Visualizer")
    print("=" * 50)
    
    # Configuration
    sql_file_path = "sql/monitoring.sql"
    output_directory = "./dq_charts"
    
    # Step 1: Read SQL query from file
    print(f"📖 Reading SQL query from: {sql_file_path}")
    sql_query = read_sql_file(sql_file_path)
    
    # Step 2: Connect to database
    connection = connect_to_database()
    
    try:
        # Step 3: Execute query and load data
        df = execute_query(connection, sql_query)
        
        # Step 4: Calculate Tableau metrics
        df_with_metrics = calculate_tableau_metrics(df)
        
        # Step 5: Create output directory
        create_output_directory(output_directory)
        
        # Step 6: Generate charts for all columns (original + calculated metrics)
        generate_charts(df_with_metrics, output_directory)
        
    finally:
        # Step 6: Close database connection
        print("\n🔐 Closing database connection...")
        connection.close()
        print("✅ Database connection closed.")
    
    # Step 7: Print completion message
    print("\n" + "=" * 50)
    print("🎉 DATA QUALITY CHARTS CREATED SUCCESSFULLY!")
    print(f"📁 Charts saved to: {os.path.abspath(output_directory)}")
    print("=" * 50)

if __name__ == "__main__":
    main()

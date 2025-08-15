import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dn_connector import get_snowflake_connection, pull_df
import pandas as pd
from typing import Optional

class SnowflakeConnector:
    def __init__(self):
        self.connection = None
        self.cursor = None
    
    def connect(self):
        """
        Connect to Snowflake using existing dn_connector configuration
        """
        try:
            self.connection = get_snowflake_connection()
            self.cursor = self.connection.cursor()
            print("Successfully connected to Snowflake using dn_connector configuration")
            return True
            
        except Exception as e:
            print(f"Error connecting to Snowflake: {str(e)}")
            return False
    
    def execute_query(self, query: str, fetch_results: bool = True) -> Optional[pd.DataFrame]:
        """
        Execute a SQL query and return results as DataFrame
        """
        if not self.cursor:
            print("No active connection. Please connect first.")
            return None
            
        try:
            self.cursor.execute(query)
            
            if fetch_results:
                # Fetch results and column names
                results = self.cursor.fetchall()
                columns = [desc[0] for desc in self.cursor.description]
                
                # Convert to DataFrame
                df = pd.DataFrame(results, columns=columns)
                return df
            else:
                return None
                
        except Exception as e:
            print(f"Error executing query: {str(e)}")
            return None
    
    def execute_sql_file(self, file_path: str) -> Optional[pd.DataFrame]:
        """
        Execute SQL from a file
        """
        try:
            with open(file_path, 'r') as file:
                query = file.read()
            
            print(f"Executing SQL from: {file_path}")
            return self.execute_query(query)
            
        except Exception as e:
            print(f"Error reading/executing SQL file {file_path}: {str(e)}")
            return None
    
    def close(self):
        """
        Close the connection
        """
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
        print("Connection closed.")

def main():
    # Initialize connector
    sf = SnowflakeConnector()
    
    # Connect to Snowflake using dn_connector configuration
    if sf.connect():
        
        # Test connection with simple query
        print("\n=== Testing Connection with SELECT 1 ===")
        result = sf.execute_query("SELECT 1 as test_column")
        if result is not None:
            print("Connection test successful!")
            print(result)
        
        # Execute test file
        test_file = 'sql/test_file1.sql'
        if os.path.exists(test_file):
            print(f"\n=== Executing {test_file} ===")
            result = sf.execute_sql_file(test_file)
            if result is not None:
                print("Query executed successfully!")
                print(f"Result shape: {result.shape}")
                print("\nColumn names:")
                print(result.columns.tolist())
                print("\nFirst few rows:")
                print(result.head())
        
        # Close connection
        sf.close()
    else:
        print("Failed to connect to Snowflake using dn_connector configuration")

if __name__ == "__main__":
    main()

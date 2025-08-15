# db_connector.py

# --- Core Imports ---
import os
import logging
from typing import Union  # <-- ADDED for type hinting

# --- Library Imports ---
import snowflake.connector
import pandas as pd
import polars as pl

# --- Basic Logging Configuration ---
# This ensures that logging messages will be displayed in your terminal.
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Polars Availability Check ---
try:
    import polars as pl
    POLARS_AVAILABLE = True
    logging.info("✅ Polars available for high-performance data operations")
except ImportError:
    POLARS_AVAILABLE = False
    logging.warning("⚠️ Polars not available - using pandas fallback")

# --- Connection Functions ---

def get_snowflake_connection():
    """
    Establishes and returns a new connection to Snowflake.
    This function encapsulates the connection logic.
    """
    logging.info("Establishing new Snowflake connection...")
    try:
        conn = snowflake.connector.connect(
            user=os.getlogin().upper().replace(".", "_"),
            account='missionlane.us-east-1',
            authenticator='externalbrowser',
            warehouse='ML_QRY_WH',
            database='datamart_db',
            schema='public'
        )
        logging.info("✅ Snowflake connection successful.")
        return conn
    except Exception as e:
        logging.error(f"❌ Failed to connect to Snowflake: {e}")
        raise # Re-raise the exception after logging

def is_connection_active(conn):
    """Check if the Snowflake connection is active."""
    if conn is None or conn.is_closed():
        return False
    try:
        # Test the connection with a simple query
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        cursor.fetchone()
        cursor.close()
        return True
    except Exception: # <-- IMPROVED from bare 'except:'
        return False

# --- Data Pulling Functions ---

def pull_df_pl(cursor, query_syntax: str) -> pl.DataFrame:
    """Pull data using cursor and return as a Polars DataFrame."""
    if not POLARS_AVAILABLE:
        raise ImportError("Polars not available. Please install with: pip install polars")
    
    try:
        cursor.execute(query_syntax)
        columns = [col[0] for col in cursor.description]
        # fetchall() can be slow for large results, but is simple
        results = cursor.fetchall()
        df = pl.DataFrame(results, schema=columns, orient="row", infer_schema_length=100000)
        return df
    except Exception as e:
        logging.error(f"Error executing Polars query: {e}")
        raise

def pull_df_pd(cursor, query_syntax: str) -> pd.DataFrame:
    """Pull data using cursor and return as a Pandas DataFrame."""
    try:
        cursor.execute(query_syntax)
        # The fetch_pandas_all() method is often more efficient
        df = cursor.fetch_pandas_all()
        return df
    except Exception as e:
        logging.error(f"Error executing Pandas query: {e}")
        raise

def pull_df(cursor, query_syntax: str, use_polars: bool = True) -> Union[pl.DataFrame, pd.DataFrame]:
    """Main data loading function with explicit control over pandas vs polars."""
    if use_polars and POLARS_AVAILABLE:
        logging.info("🚀 Loading data with Polars (high-performance mode)")
        return pull_df_pl(cursor, query_syntax)
    
    if use_polars and not POLARS_AVAILABLE:
        logging.warning("⚠️ Polars requested but not available, using Pandas fallback")

    logging.info("📊 Loading data with Pandas (compatibility mode)")
    return pull_df_pd(cursor, query_syntax)
"""
Snowflake Query Script - Database Inspection
PE Org-AI-R Platform

Displays ALL tables and ALL data from each table in the database.

Run: .venv\Scripts\python.exe app\Scripts\query_snowflake.py
"""

import sys
import time
from pathlib import Path
from datetime import datetime

# Add project root to path
project_root = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(project_root))

from app.services.snowflake import get_snowflake_connection


def timed_query(cur, query, description="Query"):
    """Execute a query and return results with timing info."""
    start = time.perf_counter()
    cur.execute(query)
    results = cur.fetchall()
    elapsed = time.perf_counter() - start
    return results, elapsed


def print_separator(char="-", length=80):
    print(char * length)


def print_table_data(columns, rows, max_col_width=40):
    """Print table data in a formatted way."""
    if not rows:
        print("    (empty table - no data)")
        return

    # Get column widths (capped at max_col_width)
    col_widths = []
    for i, col in enumerate(columns):
        max_width = len(str(col))
        for row in rows:
            val_len = len(str(row[i])[:max_col_width])
            max_width = max(max_width, val_len)
        col_widths.append(min(max_width, max_col_width))

    # Print header
    header = " | ".join(str(col)[:w].ljust(w) for col, w in zip(columns, col_widths))
    print(f"    {header}")
    print(f"    {'-' * len(header)}")

    # Print ALL rows
    for row in rows:
        row_str = " | ".join(str(val)[:w].ljust(w) for val, w in zip(row, col_widths))
        print(f"    {row_str}")


def get_table_columns(cur, table_name):
    """Get column names for a table."""
    try:
        cur.execute(f"DESCRIBE TABLE {table_name}")
        return [row[0] for row in cur.fetchall()]
    except Exception as e:
        print(f"    Error getting columns for {table_name}: {e}")
        return []


def show_table_schema(cur, table_name):
    """Display table schema information."""
    try:
        cur.execute(f"DESCRIBE TABLE {table_name}")
        schema_rows = cur.fetchall()
        
        print(f"\n  {'='*60}")
        print(f"  TABLE SCHEMA: {table_name}")
        print(f"  {'='*60}")
        
        if not schema_rows:
            print("    No schema information available")
            return
        
        # Print schema in a formatted way
        print(f"    {'Column Name':<30} {'Data Type':<20} {'Nullable':<10}")
        print(f"    {'-'*30} {'-'*20} {'-'*10}")
        
        for row in schema_rows:
            col_name = row[0]
            data_type = row[1]
            nullable = row[2]
            print(f"    {col_name:<30} {data_type:<20} {nullable:<10}")
            
    except Exception as e:
        print(f"    Error getting schema: {e}")


def show_table_data(cur, table_name):
    """Display all data from a table."""
    print(f"\n{'='*80}")
    print(f"  TABLE: {table_name}")
    print(f"{'='*80}")

    try:
        # Get row count
        cur.execute(f"SELECT COUNT(*) FROM {table_name}")
        count = cur.fetchone()[0]
        print(f"  Total Rows: {count}")

        # Always show schema, even for empty tables
        show_table_schema(cur, table_name)
        
        if count == 0:
            print(f"\n  Table is empty - no data to display")
            return

        # Get column names
        columns = get_table_columns(cur, table_name)
        if not columns:
            print("  Could not retrieve column information")
            return

        # Get all data
        query = f"SELECT * FROM {table_name}"
        rows, elapsed = timed_query(cur, query, f"SELECT * FROM {table_name}")
        print(f"\n  Query Time: {elapsed:.3f}s")
        print(f"  Data:")

        # Print data
        print_table_data(columns, rows)

    except Exception as e:
        print(f"  Error reading table: {e}")


def get_all_tables(cur):
    """Get all tables in the current schema."""
    try:
        cur.execute("SHOW TABLES")
        tables = [t[1] for t in cur.fetchall()]
        return sorted(tables)
    except Exception as e:
        print(f"Error getting tables: {e}")
        return []


def main():
    print("\n" + "=" * 80)
    print("  SNOWFLAKE DATABASE INSPECTION SCRIPT")
    print("  PE Org-AI-R Platform")
    print(f"  Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)

    total_start = time.perf_counter()

    # Connect
    print("\n[1] Connecting to Snowflake...")
    conn_start = time.perf_counter()
    conn = get_snowflake_connection()
    cur = conn.cursor()
    print(f"    Connected in {time.perf_counter() - conn_start:.3f}s")

    # Get current context
    print("\n[2] Current Context")
    cur.execute("SELECT CURRENT_DATABASE(), CURRENT_SCHEMA(), CURRENT_WAREHOUSE(), CURRENT_ROLE()")
    db, schema, warehouse, role = cur.fetchone()
    print(f"    Database:  {db}")
    print(f"    Schema:    {schema}")
    print(f"    Warehouse: {warehouse}")
    print(f"    Role:      {role}")

    # Get all tables
    print("\n[3] Discovering All Tables...")
    all_tables = get_all_tables(cur)
    
    if not all_tables:
        print("    No tables found in the database!")
        cur.close()
        conn.close()
        return
    
    print(f"    Found {len(all_tables)} tables:")
    for i, table in enumerate(all_tables, 1):
        print(f"    {i:3d}. {table}")

    # Show data from each table
    print(f"\n[4] Table Data (ALL {len(all_tables)} TABLES)")

    for i, table in enumerate(all_tables, 1):
        print(f"\n{'='*80}")
        print(f"  TABLE {i}/{len(all_tables)}: {table}")
        print(f"{'='*80}")
        show_table_data(cur, table)

    # Summary
    print("\n" + "=" * 80)
    print("  DATABASE SUMMARY")
    print("=" * 80)
    
    print(f"  Total Tables: {len(all_tables)}")
    
    # Count rows for each table
    table_counts = {}
    for table in all_tables:
        try:
            cur.execute(f"SELECT COUNT(*) FROM {table}")
            count = cur.fetchone()[0]
            table_counts[table] = count
            print(f"    {table}: {count} rows")
        except Exception as e:
            print(f"    {table}: Error counting rows - {e}")
            table_counts[table] = 0

    total_rows = sum(table_counts.values())
    print(f"\n  Total Rows Across All Tables: {total_rows}")

    total_elapsed = time.perf_counter() - total_start
    print(f"\n  Total execution time: {total_elapsed:.3f}s")
    print("=" * 80)

    cur.close()
    conn.close()
    print("\nConnection closed.\n")


if __name__ == "__main__":
    main()

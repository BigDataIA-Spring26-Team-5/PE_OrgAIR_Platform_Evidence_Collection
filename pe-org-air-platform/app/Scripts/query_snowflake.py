"""
Snowflake Query Script - CRUD Verification
PE Org-AI-R Platform

Displays ALL data from each table to verify FastAPI CRUD operations.

Tables checked:
- INDUSTRIES
- COMPANIES
- ASSESSMENTS
- DIMENSION_SCORES

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


# Tables used by the FastAPI application
APP_TABLES = ["INDUSTRIES", "COMPANIES", "ASSESSMENTS", "DIMENSION_SCORES"]


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
    cur.execute(f"DESCRIBE TABLE {table_name}")
    return [row[0] for row in cur.fetchall()]


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

        if count == 0:
            print("  (empty table - no data)")
            return

        # Get column names
        columns = get_table_columns(cur, table_name)

        # Get all data
        query = f"SELECT * FROM {table_name} ORDER BY CREATED_AT DESC"
        rows, elapsed = timed_query(cur, query, f"SELECT * FROM {table_name}")
        print(f"  Query Time: {elapsed:.3f}s\n")

        # Print data
        print_table_data(columns, rows)

    except Exception as e:
        print(f"  Error reading table: {e}")


def main():
    print("\n" + "=" * 80)
    print("  SNOWFLAKE CRUD VERIFICATION SCRIPT")
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

    # Check which tables exist
    print("\n[3] Checking Application Tables...")
    cur.execute("SHOW TABLES")
    existing_tables = [t[1] for t in cur.fetchall()]

    for table in APP_TABLES:
        if table in existing_tables:
            print(f"    ✓ {table} exists")
        else:
            print(f"    ✗ {table} NOT FOUND")

    # Show data from each application table
    print("\n[4] Table Data (ALL ROWS)")

    for table in APP_TABLES:
        if table in existing_tables:
            show_table_data(cur, table)
        else:
            print(f"\n{'='*80}")
            print(f"  TABLE: {table}")
            print(f"{'='*80}")
            print(f"  ✗ Table does not exist!")

    # Summary
    print("\n" + "=" * 80)
    print("  SUMMARY")
    print("=" * 80)

    for table in APP_TABLES:
        if table in existing_tables:
            cur.execute(f"SELECT COUNT(*) FROM {table}")
            count = cur.fetchone()[0]
            print(f"    {table}: {count} rows")
        else:
            print(f"    {table}: (table not found)")

    total_elapsed = time.perf_counter() - total_start
    print(f"\n  Total execution time: {total_elapsed:.3f}s")
    print("=" * 80)

    cur.close()
    conn.close()
    print("\nConnection closed.\n")


if __name__ == "__main__":
    main()

"""
PE Org-AI-R Platform Foundation - Streamlit Dashboard
"""

import streamlit as st
import requests
import subprocess
import sys
import json
import os
import pandas as pd
from pathlib import Path
from datetime import datetime

# =============================================================================
# CONFIGURATION
# =============================================================================

API_BASE_URL = "http://localhost:8000"

st.set_page_config(
    page_title="PE Org-AI-R Platform",
    page_icon="🏢",
    layout="wide",
    initial_sidebar_state="expanded"
)

# =============================================================================
# CUSTOM CSS
# =============================================================================

st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        color: #1E3A8A;
        text-align: center;
        padding: 1rem;
    }
    .section-header {
        font-size: 1.5rem;
        font-weight: bold;
        color: #1E3A8A;
        border-bottom: 2px solid #3B82F6;
        padding-bottom: 0.5rem;
        margin-top: 2rem;
    }
    .cache-hit {
        padding: 0.75rem;
        border-radius: 0.5rem;
        border: 1px solid #10B981;
        margin-top: 0.5rem;
    }
    .cache-miss {
        padding: 0.75rem;
        border-radius: 0.5rem;
        border: 1px solid #F59E0B;
        margin-top: 0.5rem;
    }
    .cache-info {
        font-family: monospace;
        font-size: 0.85rem;
    }
</style>
""", unsafe_allow_html=True)

# =============================================================================
# SIDEBAR NAVIGATION
# =============================================================================

st.sidebar.markdown("## 🏢 PE Org-AI-R Platform")
st.sidebar.markdown("---")

page = st.sidebar.radio(
    "Navigation",
    ["🏠 Home", "📊 ERD Diagram", "❄️ Snowflake Setup", "📋 Data Management", "🔌 API Explorer", "🧪 Test Runner"]
)

# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def get_project_root():
    """Get the project root directory."""
    current_file = Path(__file__).resolve()
    project_root = current_file.parent.parent
    return str(project_root)

def check_api_health():
    """Check if FastAPI is running."""
    try:
        response = requests.get(f"{API_BASE_URL}/health", timeout=5)
        return True, response.json()
    except requests.exceptions.ConnectionError:
        return False, None
    except Exception as e:
        return False, str(e)

def make_api_request(method, endpoint, data=None, params=None):
    """Make API request and return response."""
    url = f"{API_BASE_URL}{endpoint}"
    try:
        if method == "GET":
            response = requests.get(url, params=params, timeout=10)
        elif method == "POST":
            response = requests.post(url, json=data, timeout=10)
        elif method == "PUT":
            response = requests.put(url, json=data, timeout=10)
        elif method == "DELETE":
            response = requests.delete(url, timeout=10)
        elif method == "PATCH":
            response = requests.patch(url, json=data, timeout=10)
        else:
            return None, None
        
        try:
            return response.status_code, response.json()
        except:
            return response.status_code, None
    except requests.exceptions.ConnectionError:
        return None, "API not reachable. Make sure FastAPI is running on port 8000."
    except Exception as e:
        return None, str(e)

def display_cache_info(response):
    """Display cache information from API response."""
    if not response or not isinstance(response, dict):
        return
    
    cache = response.get("cache")
    if not cache:
        return
    
    hit = cache.get("hit", False)
    source = cache.get("source", "unknown")
    key = cache.get("key", "N/A")
    latency = cache.get("latency_ms", 0)
    ttl = cache.get("ttl_seconds", 0)
    message = cache.get("message", "")
    
    if hit:
        st.markdown(f"""
        <div class="cache-hit">
            <strong>✅ CACHE HIT</strong><br>
            <span class="cache-info">
                Source: <strong>{source}</strong> | 
                Latency: <strong>{latency:.3f}ms</strong> | 
                TTL: <strong>{ttl}s</strong><br>
                Key: <code>{key}</code>
            </span>
        </div>
        """, unsafe_allow_html=True)
    else:
        st.markdown(f"""
        <div class="cache-miss">
            <strong>❌ CACHE MISS</strong><br>
            <span class="cache-info">
                Source: <strong>{source}</strong> | 
                Latency: <strong>{latency:.3f}ms</strong> | 
                TTL: <strong>{ttl}s</strong><br>
                Key: <code>{key}</code><br>
                <em>Data now cached for future requests</em>
            </span>
        </div>
        """, unsafe_allow_html=True)

def get_streamlit_snowflake_connection():
    """Get Snowflake connection using settings from app.config."""
    import snowflake.connector
    from dotenv import load_dotenv

    # Load .env file from project root
    project_root = get_project_root()
    env_path = os.path.join(project_root, ".env")
    load_dotenv(env_path)

    return snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA"),
        role=os.getenv("SNOWFLAKE_ROLE", "ACCOUNTADMIN"),
    )

def execute_sql_file(file_path: str, conn) -> tuple:
    """Execute SQL file, returns (success, message, results)."""
    import re
    try:
        with open(file_path, 'r') as f:
            content = f.read()

        # Handle $$ delimiters for stored procedures
        # Split by $$ first to handle procedure definitions
        parts = content.split('$$')
        statements = []

        if len(parts) > 1:
            # Has procedure definitions
            for i, part in enumerate(parts):
                if i % 2 == 0:
                    # Outside $$ - split by semicolon
                    for stmt in part.split(';'):
                        stmt = stmt.strip()
                        if stmt and not stmt.startswith('--'):
                            statements.append(stmt)
                else:
                    # Inside $$ - this is the procedure body, combine with previous
                    if statements:
                        statements[-1] = statements[-1] + '$$' + part + '$$'
        else:
            # No procedure definitions - simple split
            for stmt in content.split(';'):
                stmt = stmt.strip()
                if stmt and not stmt.startswith('--'):
                    # Remove comment lines
                    lines = [l for l in stmt.split('\n') if not l.strip().startswith('--')]
                    stmt = '\n'.join(lines).strip()
                    if stmt:
                        statements.append(stmt)

        results = []
        cursor = conn.cursor()

        for stmt in statements:
            if stmt:
                try:
                    cursor.execute(stmt)
                    if cursor.description:
                        results.append(cursor.fetchall())
                    else:
                        results.append(f"Executed: {stmt[:50]}...")
                except Exception as e:
                    results.append(f"Error in statement: {str(e)}")

        cursor.close()
        return True, f"Executed {len(statements)} statements", results
    except Exception as e:
        return False, f"Error: {str(e)}", []

def parse_seed_for_api(file_path: str, entity_type: str) -> list:
    """Parse CALL statements from seed files into API payloads."""
    import re

    with open(file_path, 'r') as f:
        content = f.read()

    payloads = []

    if entity_type == "companies":
        # CALL insert_company('id', 'name', 'ticker', 'industry_id', position_factor)
        pattern = r"CALL insert_company\('([^']+)',\s*'([^']+)',\s*(?:'([^']*)'|NULL),\s*'([^']+)',\s*([-\d.]+)\)"
        matches = re.findall(pattern, content)
        for match in matches:
            payload = {
                "id": match[0],
                "name": match[1],
                "industry_id": match[3],
                "position_factor": float(match[4])
            }
            if match[2]:  # ticker
                payload["ticker"] = match[2]
            payloads.append(payload)

    elif entity_type == "assessments":
        # CALL insert_assessment('id', 'company_id', 'type', 'date', 'status', 'primary', 'secondary', score)
        pattern = r"CALL insert_assessment\('([^']+)',\s*'([^']+)',\s*'([^']+)',\s*'([^']+)',\s*'([^']+)',\s*(?:'([^']*)'|NULL),\s*(?:'([^']*)'|NULL),\s*(?:([\d.]+)|NULL)\)"
        matches = re.findall(pattern, content)
        for match in matches:
            payload = {
                "id": match[0],
                "company_id": match[1],
                "assessment_type": match[2],
                "assessment_date": match[3],
                "status": match[4]
            }
            if match[5]:
                payload["primary_assessor"] = match[5]
            if match[6]:
                payload["secondary_assessor"] = match[6]
            if match[7]:
                payload["v_r_score"] = float(match[7])
            payloads.append(payload)

    elif entity_type == "dimension_scores":
        # CALL insert_dimension_score('id', 'assessment_id', 'dimension', score, weight, confidence, evidence_count)
        pattern = r"CALL insert_dimension_score\('([^']+)',\s*'([^']+)',\s*'([^']+)',\s*([\d.]+),\s*([\d.]+),\s*([\d.]+),\s*(\d+)\)"
        matches = re.findall(pattern, content)
        for match in matches:
            payload = {
                "id": match[0],
                "assessment_id": match[1],
                "dimension": match[2],
                "score": float(match[3]),
                "weight": float(match[4]),
                "confidence": float(match[5]),
                "evidence_count": int(match[6])
            }
            payloads.append(payload)

    return payloads

def run_pytest(test_file=None):
    """Run pytest and capture output."""
    try:
        project_root = get_project_root()
        cmd = [sys.executable, "-m", "pytest", "-v", "--tb=short"]
        
        if test_file:
            test_path = os.path.join(project_root, test_file)
            if not os.path.exists(test_path):
                return False, "", f"Test file not found: {test_path}"
            cmd.append(test_path)
        else:
            tests_dir = os.path.join(project_root, "tests")
            if not os.path.exists(tests_dir):
                return False, "", f"Tests directory not found: {tests_dir}"
            cmd.append(tests_dir)
        
        env = os.environ.copy()
        env["PYTHONPATH"] = project_root
        
        result = subprocess.run(
            cmd, capture_output=True, text=True,
            cwd=project_root, timeout=300, env=env  # Changed from 120 to 300 seconds
        )
        
        output = result.stdout
        if result.stderr:
            output += "\n--- STDERR ---\n" + result.stderr
        
        return result.returncode == 0, output, result.stderr
    except subprocess.TimeoutExpired:
        return False, "", "Test execution timed out (300s limit). Try running Model Tests and API Tests separately."
    except Exception as e:
        return False, "", f"Error: {str(e)}"

# =============================================================================
# PAGE: HOME
# =============================================================================

if page == "🏠 Home":
    st.markdown('<p class="main-header">🏢 PE Org-AI-R Platform Foundation</p>', unsafe_allow_html=True)
    
    st.markdown("""
    Welcome to the **PE Org-AI-R Platform Foundation** dashboard. This platform provides 
    AI readiness assessment tools for portfolio companies.
    """)
    
    st.markdown('<p class="section-header">🔍 System Status</p>', unsafe_allow_html=True)
    
    if st.button("🔄 Refresh Status"):
        st.rerun()
    
    api_healthy, health_data = check_api_health()
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.markdown("##### FastAPI")
        if api_healthy:
            st.success("✅ Running")
        else:
            st.error("❌ Not Running")
            st.caption("Run: `uvicorn app.main:app --reload`")
    
    with col2:
        st.markdown("##### Snowflake")
        if health_data and isinstance(health_data, dict):
            deps = health_data.get("dependencies", {})
            snowflake_status = deps.get("snowflake", "unknown")
            if snowflake_status.startswith("healthy"):
                st.success("✅ Connected")
            else:
                st.error("❌ Not Connected")
        else:
            st.warning("⚠️ Check API first")
    
    with col3:
        st.markdown("##### Redis")
        if health_data and isinstance(health_data, dict):
            deps = health_data.get("dependencies", {})
            redis_status = deps.get("redis", "unknown")
            if redis_status.startswith("healthy"):
                st.success("✅ Connected")
            else:
                st.error("❌ Not Connected")
        else:
            st.warning("⚠️ Check API first")
    
    with col4:
        st.markdown("##### AWS S3")
        if health_data and isinstance(health_data, dict):
            deps = health_data.get("dependencies", {})
            s3_status = deps.get("s3", "unknown")
            if s3_status.startswith("healthy"):
                st.success("✅ Connected")
            else:
                st.error("❌ Not Connected")
        else:
            st.warning("⚠️ Check API first")
    
    if health_data:
        with st.expander("📋 View Full Health Response"):
            st.json(health_data)
    
    st.markdown('<p class="section-header">📈 Quick Overview</p>', unsafe_allow_html=True)
    
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Models", "5")
    with col2:
        st.metric("API Endpoints", "16")
    with col3:
        st.metric("Dimensions", "7")
    with col4:
        st.metric("Test Cases", "79")

# =============================================================================
# PAGE: ERD DIAGRAM
# =============================================================================

elif page == "📊 ERD Diagram":
    st.markdown('<p class="main-header">📊 Entity Relationship Diagram</p>', unsafe_allow_html=True)
    
    st.markdown('<p class="section-header">📋 Entity Details</p>', unsafe_allow_html=True)
    
    tab1, tab2, tab3, tab4 = st.tabs(["Industry", "Company", "Assessment", "Dimension Score"])
    
    with tab1:
        st.markdown("""
        | Field | Type | Constraints |
        |-------|------|-------------|
        | `id` | UUID | Primary Key |
        | `name` | VARCHAR(255) | Unique, Not Null |
        | `sector` | VARCHAR(100) | Not Null |
        | `h_r_base` | DECIMAL(5,2) | 0-100 |
        | `created_at` | TIMESTAMP | Auto-generated |
        """)
    
    with tab2:
        st.markdown("""
        | Field | Type | Constraints |
        |-------|------|-------------|
        | `id` | UUID | Primary Key |
        | `name` | VARCHAR(255) | Not Null |
        | `ticker` | VARCHAR(10) | Optional, Uppercase |
        | `industry_id` | UUID | Foreign Key |
        | `position_factor` | DECIMAL(4,3) | -1.0 to 1.0 |
        | `is_deleted` | BOOLEAN | Default: FALSE |
        """)
    
    with tab3:
        st.markdown("""
        | Field | Type | Constraints |
        |-------|------|-------------|
        | `id` | UUID | Primary Key |
        | `company_id` | UUID | Foreign Key |
        | `assessment_type` | ENUM | screening, due_diligence, quarterly, exit_prep |
        | `status` | ENUM | draft, in_progress, submitted, approved, superseded |
        | `v_r_score` | DECIMAL(5,2) | 0-100, Optional |
        """)
    
    with tab4:
        st.markdown("""
        | Field | Type | Constraints |
        |-------|------|-------------|
        | `id` | UUID | Primary Key |
        | `assessment_id` | UUID | Foreign Key |
        | `dimension` | ENUM | 7 dimension types |
        | `score` | DECIMAL(5,2) | 0-100 |
        | `weight` | DECIMAL(4,3) | 0-1, Auto-assigned |
        """)
    
    st.markdown('<p class="section-header">⚖️ Dimension Weights</p>', unsafe_allow_html=True)
    
    st.markdown("""
    The platform uses weighted dimensions to calculate the overall AI Readiness score.
    Weights are automatically assigned based on dimension importance.
    """)
    
    weights_data = {
        "Dimension": [
            "Data Infrastructure", 
            "AI Governance", 
            "Technology Stack",
            "Talent & Skills", 
            "Leadership Vision", 
            "Use Case Portfolio", 
            "Culture Change"
        ],
        "Code": [
            "data_infrastructure",
            "ai_governance", 
            "technology_stack",
            "talent_skills",
            "leadership_vision",
            "use_case_portfolio",
            "culture_change"
        ],
        "Weight": [0.25, 0.20, 0.15, 0.15, 0.10, 0.10, 0.05],
        "Percentage": ["25%", "20%", "15%", "15%", "10%", "10%", "5%"]
    }
    st.table(weights_data)
    
    st.info("💡 **Note**: Weights must sum to 1.0 (100%). Data Infrastructure has the highest weight as it forms the foundation for AI readiness.")

# =============================================================================
# PAGE: SNOWFLAKE SETUP
# =============================================================================

elif page == "❄️ Snowflake Setup":
    st.markdown('<p class="main-header">❄️ Snowflake Setup</p>', unsafe_allow_html=True)

    # Get project root for file paths
    project_root = get_project_root()

    # =========================================================================
    # SECTION 1: SCHEMA MANAGEMENT
    # =========================================================================
    st.markdown('<p class="section-header">1. Schema Management</p>', unsafe_allow_html=True)

    st.warning("⚠️ **Warning**: Creating the schema will DROP all existing tables and recreate them. All data will be lost!")

    if st.button("🏗️ CREATE SCHEMA", key="create_schema", type="primary"):
        with st.spinner("Creating schema..."):
            try:
                conn = get_streamlit_snowflake_connection()
                cursor = conn.cursor()
                all_results = []
                has_errors = False

                # Step 1: Drop tables in reverse FK order
                st.text("Step 1: Dropping existing tables...")
                drop_statements = [
                    "DROP TABLE IF EXISTS dimension_scores",
                    "DROP TABLE IF EXISTS assessments",
                    "DROP TABLE IF EXISTS companies",
                    "DROP TABLE IF EXISTS industries"
                ]

                for stmt in drop_statements:
                    try:
                        cursor.execute(stmt)
                        all_results.append(f"✅ {stmt}")
                    except Exception as e:
                        all_results.append(f"❌ {stmt}: {str(e)}")
                        has_errors = True

                # Step 2: Create tables directly (not from file to avoid parsing issues)
                st.text("Step 2: Creating tables...")

                create_statements = [
                    ("industries", """
                        CREATE TABLE IF NOT EXISTS industries (
                            id VARCHAR(36) PRIMARY KEY,
                            name VARCHAR(255) NOT NULL UNIQUE,
                            sector VARCHAR(100) NOT NULL,
                            h_r_base DECIMAL(5,2),
                            created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
                        )
                    """),
                    ("companies", """
                        CREATE TABLE IF NOT EXISTS companies (
                            id VARCHAR(36) PRIMARY KEY,
                            name VARCHAR(255) NOT NULL,
                            ticker VARCHAR(10),
                            industry_id VARCHAR(36),
                            position_factor DECIMAL(4,3) DEFAULT 0.0,
                            is_deleted BOOLEAN DEFAULT FALSE,
                            created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
                            updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
                            FOREIGN KEY (industry_id) REFERENCES industries(id)
                        )
                    """),
                    ("assessments", """
                        CREATE TABLE IF NOT EXISTS assessments (
                            id VARCHAR(36) PRIMARY KEY,
                            company_id VARCHAR(36) NOT NULL,
                            assessment_type VARCHAR(20) NOT NULL,
                            assessment_date DATE NOT NULL,
                            status VARCHAR(20) DEFAULT 'draft',
                            primary_assessor VARCHAR(255),
                            secondary_assessor VARCHAR(255),
                            v_r_score DECIMAL(5,2),
                            confidence_lower DECIMAL(5,2),
                            confidence_upper DECIMAL(5,2),
                            created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
                            FOREIGN KEY (company_id) REFERENCES companies(id)
                        )
                    """),
                    ("dimension_scores", """
                        CREATE TABLE IF NOT EXISTS dimension_scores (
                            id VARCHAR(36) PRIMARY KEY,
                            assessment_id VARCHAR(36) NOT NULL,
                            dimension VARCHAR(30) NOT NULL,
                            score DECIMAL(5,2) NOT NULL,
                            weight DECIMAL(4,3),
                            confidence DECIMAL(4,3) DEFAULT 0.8,
                            evidence_count INT DEFAULT 0,
                            created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
                            FOREIGN KEY (assessment_id) REFERENCES assessments(id),
                            UNIQUE (assessment_id, dimension)
                        )
                    """),
                ]

                for table_name, create_sql in create_statements:
                    try:
                        cursor.execute(create_sql)
                        all_results.append(f"✅ Created table: {table_name}")
                    except Exception as e:
                        all_results.append(f"❌ Failed to create {table_name}: {str(e)}")
                        has_errors = True

                # Step 3: Create stored procedures from schema.sql
                st.text("Step 3: Creating stored procedures...")
                schema_path = os.path.join(project_root, "app", "database", "schema.sql")
                try:
                    with open(schema_path, 'r') as f:
                        schema_content = f.read()

                    # Extract and execute stored procedures (between $$ delimiters)
                    import re
                    procedure_pattern = r'(CREATE OR REPLACE PROCEDURE[^;]*\$\$.*?\$\$)'
                    procedures = re.findall(procedure_pattern, schema_content, re.DOTALL)

                    for i, proc in enumerate(procedures):
                        try:
                            cursor.execute(proc)
                            # Extract procedure name for logging
                            proc_name_match = re.search(r'PROCEDURE\s+(\w+)', proc)
                            proc_name = proc_name_match.group(1) if proc_name_match else f"procedure_{i+1}"
                            all_results.append(f"✅ Created procedure: {proc_name}")
                        except Exception as e:
                            all_results.append(f"❌ Failed to create procedure: {str(e)[:100]}")
                            has_errors = True
                except Exception as e:
                    all_results.append(f"⚠️ Could not create stored procedures: {str(e)}")

                cursor.close()
                conn.close()

                if has_errors:
                    st.error("❌ Schema creation completed with errors. Check details below.")
                else:
                    st.success("✅ Schema created successfully!")

                with st.expander("📋 View Details", expanded=has_errors):
                    for r in all_results:
                        st.text(r)

            except Exception as e:
                st.error(f"❌ Connection error: {str(e)}")

    # =========================================================================
    # SECTION 2: DATA INGESTION
    # =========================================================================
    st.markdown('<p class="section-header">2. Data Ingestion</p>', unsafe_allow_html=True)

    st.info("💡 Executes seed SQL files directly against Snowflake using stored procedures.")

    if st.button("📥 INGEST DATA", key="ingest_sql", type="primary"):
        seed_files = [
            ("seed-industries.sql", "Industries"),
            ("seed-companies.sql", "Companies"),
            ("seed-assessments.sql", "Assessments"),
            ("seed-dimension-scores.sql", "Dimension Scores")
        ]

        progress_bar = st.progress(0)
        status_text = st.empty()
        results_log = []

        try:
            conn = get_streamlit_snowflake_connection()

            for i, (filename, label) in enumerate(seed_files):
                status_text.text(f"Ingesting {label}...")
                progress_bar.progress((i + 1) / len(seed_files))

                file_path = os.path.join(project_root, "app", "database", filename)
                success, message, results = execute_sql_file(file_path, conn)

                if success:
                    results_log.append(f"✅ {label}: {message}")
                else:
                    results_log.append(f"❌ {label}: {message}")

            conn.close()
            status_text.text("Ingestion complete!")
            st.success("✅ Data ingestion completed!")

            with st.expander("📋 View Results"):
                for log in results_log:
                    st.text(log)

        except Exception as e:
            st.error(f"❌ Error: {str(e)}")

    # -------------------------------------------------------------------------
    # FastAPI Ingestion (COMMENTED OUT)
    # -------------------------------------------------------------------------
    # Note: FastAPI ingestion is disabled because:
    # 1. API generates new UUIDs, breaking foreign key relationships
    # 2. Seed files have hardcoded IDs that reference each other
    # 3. Direct SQL preserves exact IDs from seed files
    #
    # To re-enable, uncomment the code below and add tabs back:
    # tab_sql, tab_api = st.tabs(["Direct SQL", "Via FastAPI"])
    # -------------------------------------------------------------------------
    #
    # with tab_api:
    #     st.info("💡 Ingests data via FastAPI endpoints. Note: Industries must be ingested via SQL first (read-only in API).")
    #
    #     if st.button("📥 INGEST (FastAPI)", key="ingest_api", type="primary"):
    #         progress_bar = st.progress(0)
    #         status_text = st.empty()
    #         results_log = []
    #
    #         # First, ingest industries via SQL (required)
    #         status_text.text("Ingesting Industries via SQL (required)...")
    #         progress_bar.progress(0.1)
    #
    #         try:
    #             conn = get_streamlit_snowflake_connection()
    #             file_path = os.path.join(project_root, "app", "database", "seed-industries.sql")
    #             success, message, _ = execute_sql_file(file_path, conn)
    #             conn.close()
    #
    #             if success:
    #                 results_log.append(f"✅ Industries (SQL): {message}")
    #             else:
    #                 results_log.append(f"❌ Industries (SQL): {message}")
    #         except Exception as e:
    #             results_log.append(f"❌ Industries (SQL): {str(e)}")
    #
    #         # Companies via API
    #         status_text.text("Ingesting Companies via API...")
    #         progress_bar.progress(0.3)
    #
    #         companies_file = os.path.join(project_root, "app", "database", "seed-companies.sql")
    #         companies = parse_seed_for_api(companies_file, "companies")
    #         companies_success = 0
    #         for company in companies:
    #             company_data = {
    #                 "name": company["name"],
    #                 "industry_id": company["industry_id"],
    #                 "position_factor": company["position_factor"]
    #             }
    #             if "ticker" in company:
    #                 company_data["ticker"] = company["ticker"]
    #
    #             status_code, _ = make_api_request("POST", "/api/v1/companies", data=company_data)
    #             if status_code == 201:
    #                 companies_success += 1
    #
    #         results_log.append(f"✅ Companies (API): {companies_success}/{len(companies)} created")
    #
    #         # Assessments via API
    #         status_text.text("Ingesting Assessments via API...")
    #         progress_bar.progress(0.6)
    #
    #         assessments_file = os.path.join(project_root, "app", "database", "seed-assessments.sql")
    #         assessments = parse_seed_for_api(assessments_file, "assessments")
    #         assessments_success = 0
    #         assessment_id_map = {}
    #
    #         for assessment in assessments:
    #             assess_data = {
    #                 "company_id": assessment["company_id"],
    #                 "assessment_type": assessment["assessment_type"],
    #                 "assessment_date": assessment["assessment_date"]
    #             }
    #             if "primary_assessor" in assessment:
    #                 assess_data["primary_assessor"] = assessment["primary_assessor"]
    #
    #             status_code, response = make_api_request("POST", "/api/v1/assessments", data=assess_data)
    #             if status_code == 201 and response:
    #                 assessments_success += 1
    #                 # Map old ID to new ID for dimension scores
    #                 if "id" in response:
    #                     assessment_id_map[assessment["id"]] = response["id"]
    #
    #         results_log.append(f"✅ Assessments (API): {assessments_success}/{len(assessments)} created")
    #
    #         # Dimension Scores via API
    #         status_text.text("Ingesting Dimension Scores via API...")
    #         progress_bar.progress(0.9)
    #
    #         scores_file = os.path.join(project_root, "app", "database", "seed-dimension-scores.sql")
    #         scores = parse_seed_for_api(scores_file, "dimension_scores")
    #         scores_success = 0
    #
    #         for score in scores:
    #             # Use mapped assessment ID if available, otherwise use original
    #             assess_id = assessment_id_map.get(score["assessment_id"], score["assessment_id"])
    #
    #             score_data = {
    #                 "assessment_id": assess_id,
    #                 "dimension": score["dimension"],
    #                 "score": score["score"],
    #                 "confidence": score["confidence"]
    #             }
    #
    #             status_code, _ = make_api_request("POST", f"/api/v1/assessments/{assess_id}/scores", data=score_data)
    #             if status_code == 201:
    #                 scores_success += 1
    #
    #         results_log.append(f"✅ Dimension Scores (API): {scores_success}/{len(scores)} created")
    #
    #         progress_bar.progress(1.0)
    #         status_text.text("Ingestion complete!")
    #         st.success("✅ FastAPI ingestion completed!")
    #
    #         with st.expander("📋 View Results"):
    #             for log in results_log:
    #                 st.text(log)

    # =========================================================================
    # SECTION 3: DELETE TABLE
    # =========================================================================
    st.markdown('<p class="section-header">3. Delete Table</p>', unsafe_allow_html=True)

    st.warning("⚠️ **Warning**: This will permanently DROP the selected table. Consider foreign key constraints when deleting.")

    delete_table = st.selectbox(
        "Select Table to Delete",
        ["dimension_scores", "assessments", "companies", "industries"],
        key="delete_table_select"
    )

    st.info("""
    **Foreign Key Dependencies:**
    - `dimension_scores` → depends on `assessments`
    - `assessments` → depends on `companies`
    - `companies` → depends on `industries`

    Delete in reverse order: dimension_scores → assessments → companies → industries
    """)

    confirm_delete = st.checkbox("I understand this action is irreversible", key="confirm_delete_checkbox")

    if st.button("🗑️ DELETE TABLE", key="delete_table_btn", type="secondary", disabled=not confirm_delete):
        with st.spinner(f"Deleting {delete_table}..."):
            try:
                conn = get_streamlit_snowflake_connection()
                cursor = conn.cursor()
                cursor.execute(f"DROP TABLE IF EXISTS {delete_table}")
                cursor.close()
                conn.close()
                st.success(f"✅ Table `{delete_table}` deleted successfully!")
            except Exception as e:
                st.error(f"❌ Error deleting table: {str(e)}")

# =============================================================================
# PAGE: DATA MANAGEMENT
# =============================================================================

elif page == "📋 Data Management":
    st.markdown('<p class="main-header">📋 Data Management</p>', unsafe_allow_html=True)

    # =========================================================================
    # SECTION 1: CURRENT DATA IN SNOWFLAKE
    # =========================================================================
    st.markdown('<p class="section-header">1. Current Data in Snowflake</p>', unsafe_allow_html=True)

    col1, col2 = st.columns([3, 1])
    with col1:
        data_table = st.selectbox(
            "Select Table",
            ["Industries", "Companies", "Assessments", "Dimension Scores"],
            key="data_view_table"
        )
    with col2:
        st.markdown("<br>", unsafe_allow_html=True)
        refresh_data = st.button("🔄 Refresh", key="refresh_data_table")

    def show_table_not_found(table_name: str):
        """Display a graceful message when table is not found."""
        st.info(f"""
        **{table_name} table not found**

        The table may not exist yet. Go to **Snowflake Setup** to:
        1. Click **CREATE SCHEMA** to create tables
        2. Click **INGEST** to populate with seed data
        """)

    def show_api_error(status_code, response, table_name: str):
        """Display graceful error message for API errors."""
        error_msg = str(response) if response else "Unknown error"
        # Check for common "table not found" patterns
        if "does not exist" in error_msg.lower() or "not found" in error_msg.lower() or status_code == 500:
            show_table_not_found(table_name)
        else:
            st.error(f"Error loading {table_name}: {error_msg}")

    if data_table == "Industries":
        status_code, response = make_api_request("GET", "/api/v1/industries")
        if status_code == 200 and response and "items" in response:
            industries = response["items"]
            if industries:
                df = pd.DataFrame(industries)
                display_cols = ["id", "name", "sector", "h_r_base"]
                available_cols = [c for c in display_cols if c in df.columns]
                if available_cols:
                    df = df[available_cols]
                    col_names = {"id": "ID", "name": "Name", "sector": "Sector", "h_r_base": "H/R Base"}
                    df.columns = [col_names.get(c, c) for c in available_cols]
                st.dataframe(df, use_container_width=True, hide_index=True)
                st.caption(f"Total: {len(industries)} industries")
                display_cache_info(response)
            else:
                st.info("No industries found. Run **INGEST** on the Snowflake Setup page to add data.")
        elif status_code:
            show_api_error(status_code, response, "Industries")
        else:
            st.warning("Cannot connect to API. Make sure FastAPI is running: `uvicorn app.main:app --reload`")

    elif data_table == "Companies":
        status_code, response = make_api_request("GET", "/api/v1/companies", params={"page_size": 100})
        if status_code == 200 and response and "items" in response:
            companies = response["items"]
            if companies:
                df = pd.DataFrame(companies)
                display_cols = ["id", "name", "ticker", "industry_id", "position_factor"]
                available_cols = [c for c in display_cols if c in df.columns]
                if available_cols:
                    df = df[available_cols]
                    col_names = {"id": "ID", "name": "Name", "ticker": "Ticker", "industry_id": "Industry ID", "position_factor": "Position Factor"}
                    df.columns = [col_names.get(c, c) for c in available_cols]
                st.dataframe(df, use_container_width=True, hide_index=True)
                st.caption(f"Total: {response.get('total', len(companies))} companies")
                display_cache_info(response)
            else:
                st.info("No companies found. Run **INGEST** on the Snowflake Setup page to add data.")
        elif status_code:
            show_api_error(status_code, response, "Companies")
        else:
            st.warning("Cannot connect to API. Make sure FastAPI is running: `uvicorn app.main:app --reload`")

    elif data_table == "Assessments":
        status_code, response = make_api_request("GET", "/api/v1/assessments", params={"page_size": 100})
        if status_code == 200 and response and "items" in response:
            assessments = response["items"]
            if assessments:
                df = pd.DataFrame(assessments)
                display_cols = ["id", "company_id", "assessment_type", "assessment_date", "status"]
                available_cols = [c for c in display_cols if c in df.columns]
                if available_cols:
                    df = df[available_cols]
                    col_names = {"id": "ID", "company_id": "Company ID", "assessment_type": "Type", "assessment_date": "Date", "status": "Status"}
                    df.columns = [col_names.get(c, c) for c in available_cols]
                st.dataframe(df, use_container_width=True, hide_index=True)
                st.caption(f"Total: {response.get('total', len(assessments))} assessments")
            else:
                st.info("No assessments found. Run **INGEST** on the Snowflake Setup page to add data.")
        elif status_code:
            show_api_error(status_code, response, "Assessments")
        else:
            st.warning("Cannot connect to API. Make sure FastAPI is running: `uvicorn app.main:app --reload`")

    elif data_table == "Dimension Scores":
        # Need to select an assessment first
        status_code, assess_response = make_api_request("GET", "/api/v1/assessments", params={"page_size": 100})

        if status_code != 200 or not assess_response:
            show_api_error(status_code, assess_response, "Assessments")
        elif "items" not in assess_response or not assess_response["items"]:
            st.info("No assessments available. Create assessments first via **INGEST** on the Snowflake Setup page.")
        else:
            assessment_options = {}
            for a in assess_response["items"]:
                label = f"{a['id'][:8]}... ({a['assessment_type']} - {a['status']})"
                assessment_options[label] = a["id"]

            selected_label = st.selectbox("Select Assessment", list(assessment_options.keys()), key="score_assess_select")
            selected_id = assessment_options[selected_label]

            score_status, score_response = make_api_request("GET", f"/api/v1/assessments/{selected_id}/scores")
            if score_status == 200:
                if isinstance(score_response, list) and len(score_response) > 0:
                    df = pd.DataFrame(score_response)
                    display_cols = ["dimension", "score", "weight", "confidence", "evidence_count"]
                    available_cols = [c for c in display_cols if c in df.columns]
                    if available_cols:
                        df = df[available_cols]
                        col_names = {"dimension": "Dimension", "score": "Score", "weight": "Weight", "confidence": "Confidence", "evidence_count": "Evidence Count"}
                        df.columns = [col_names.get(c, c) for c in available_cols]
                    st.dataframe(df, use_container_width=True, hide_index=True)
                else:
                    st.info("No dimension scores recorded for this assessment yet. This assessment may be in draft status.")
            elif score_status == 404:
                st.info("No dimension scores found for this assessment.")
            else:
                st.warning(f"Could not load dimension scores. API returned status {score_status}.")

    # =========================================================================
    # SECTION 2: UPDATE RECORDS
    # =========================================================================
    st.markdown('<p class="section-header">2. Update Records</p>', unsafe_allow_html=True)

    update_table = st.selectbox(
        "Select Table to Update",
        ["Companies", "Assessments", "Dimension Scores"],
        key="update_table_select"
    )

    if update_table == "Companies":
        status_code, comp_response = make_api_request("GET", "/api/v1/companies", params={"page_size": 100})
        if status_code != 200 or not comp_response:
            show_table_not_found("Companies")
        elif "items" not in comp_response or not comp_response["items"]:
            st.info("No companies available to update. Run **INGEST** on the Snowflake Setup page first.")
        else:
            company_options = {}
            for c in comp_response["items"]:
                label = f"{c['name']} ({c.get('ticker', 'N/A')})"
                company_options[label] = c

            selected_company_label = st.selectbox("Select Company", list(company_options.keys()), key="update_company_select")
            selected_company = company_options[selected_company_label]

            st.markdown("**Edit Fields:**")
            col1, col2 = st.columns(2)
            with col1:
                new_name = st.text_input("Name", value=selected_company["name"], key="update_comp_name")
                new_ticker = st.text_input("Ticker", value=selected_company.get("ticker", ""), key="update_comp_ticker")
            with col2:
                new_position = st.slider("Position Factor", -1.0, 1.0, float(selected_company.get("position_factor", 0)), 0.01, key="update_comp_pos")

            if st.button("💾 UPDATE", key="update_company_btn"):
                update_data = {
                    "name": new_name,
                    "position_factor": new_position
                }
                if new_ticker:
                    update_data["ticker"] = new_ticker

                status_code, response = make_api_request("PUT", f"/api/v1/companies/{selected_company['id']}", data=update_data)
                if status_code == 200:
                    st.success("✅ Company updated successfully!")
                    st.json(response)
                else:
                    st.error(f"❌ Error: {response}")

    elif update_table == "Assessments":
        status_code, assess_response = make_api_request("GET", "/api/v1/assessments", params={"page_size": 100})
        if status_code != 200 or not assess_response:
            show_table_not_found("Assessments")
        elif "items" not in assess_response or not assess_response["items"]:
            st.info("No assessments available to update. Run **INGEST** on the Snowflake Setup page first.")
        else:
            assessment_options = {}
            for a in assess_response["items"]:
                label = f"{a['id'][:8]}... ({a['assessment_type']} - {a['status']})"
                assessment_options[label] = a

            selected_assess_label = st.selectbox("Select Assessment", list(assessment_options.keys()), key="update_assess_select")
            selected_assess = assessment_options[selected_assess_label]

            st.markdown("**Edit Status:**")
            new_status = st.selectbox(
                "Status",
                ["draft", "in_progress", "submitted", "approved", "superseded"],
                index=["draft", "in_progress", "submitted", "approved", "superseded"].index(selected_assess.get("status", "draft")),
                key="update_assess_status"
            )

            if st.button("💾 UPDATE", key="update_assess_btn"):
                status_code, response = make_api_request("PATCH", f"/api/v1/assessments/{selected_assess['id']}/status", data={"status": new_status})
                if status_code == 200:
                    st.success("✅ Assessment status updated successfully!")
                    st.json(response)
                else:
                    st.error(f"❌ Error: {response}")

    elif update_table == "Dimension Scores":
        # First select assessment, then score
        status_code, assess_response = make_api_request("GET", "/api/v1/assessments", params={"page_size": 100})
        if status_code != 200 or not assess_response:
            show_table_not_found("Assessments")
        elif "items" not in assess_response or not assess_response["items"]:
            st.info("No assessments available. Run **INGEST** on the Snowflake Setup page first.")
        else:
            assessment_options = {}
            for a in assess_response["items"]:
                label = f"{a['id'][:8]}... ({a['assessment_type']})"
                assessment_options[label] = a["id"]
            selected_assess_label = st.selectbox("Select Assessment", list(assessment_options.keys()), key="update_score_assess_select")
            selected_assess_id = assessment_options[selected_assess_label]

            status_code, scores_response = make_api_request("GET", f"/api/v1/assessments/{selected_assess_id}/scores")

            if status_code == 200 and isinstance(scores_response, list) and len(scores_response) > 0:
                score_options = {}
                for s in scores_response:
                    label = f"{s['dimension']} (Score: {s['score']})"
                    score_options[label] = s

                selected_score_label = st.selectbox("Select Dimension Score", list(score_options.keys()), key="update_score_select")
                selected_score = score_options[selected_score_label]

                st.markdown("**Edit Fields:**")
                col1, col2, col3 = st.columns(3)
                with col1:
                    new_score = st.slider("Score", 0.0, 100.0, float(selected_score.get("score", 0)), 0.5, key="update_score_value")
                with col2:
                    new_confidence = st.slider("Confidence", 0.0, 1.0, float(selected_score.get("confidence", 0.8)), 0.01, key="update_score_confidence")
                with col3:
                    new_evidence = st.number_input("Evidence Count", min_value=0, value=int(selected_score.get("evidence_count", 0)), key="update_score_evidence")

                if st.button("💾 UPDATE", key="update_score_btn"):
                    update_data = {
                        "score": new_score,
                        "confidence": new_confidence,
                        "evidence_count": new_evidence
                    }

                    update_status, response = make_api_request("PUT", f"/api/v1/scores/{selected_score['id']}", data=update_data)
                    if update_status == 200:
                        st.success("✅ Dimension score updated successfully!")
                        st.json(response)
                    else:
                        st.error(f"❌ Error: {response}")
            elif status_code == 200:
                st.info("No dimension scores recorded for this assessment yet. Select an assessment that has been scored.")
            else:
                st.warning(f"Could not load dimension scores. Try selecting a different assessment.")

# =============================================================================
# PAGE: API EXPLORER
# =============================================================================

elif page == "🔌 API Explorer":
    st.markdown('<p class="main-header">🔌 API Explorer</p>', unsafe_allow_html=True)
    
    api_healthy, _ = check_api_health()
    if not api_healthy:
        st.error("⚠️ FastAPI is not running. Start it with: `uvicorn app.main:app --reload`")
    
    api_category = st.selectbox(
        "Select API Category",
        ["Health", "Industries", "Companies", "Assessments", "Dimension Scores"]
    )
    
    st.markdown("---")
    
    # =========================================================================
    # HEALTH API
    # =========================================================================
    if api_category == "Health":
        st.markdown("### 🏥 Health Check API")
        
        col1, col2 = st.columns(2)
        
        with col1:
            if st.button("🔍 GET /health", key="health_get"):
                with st.spinner("Checking health..."):
                    status_code, response = make_api_request("GET", "/health")
                    if status_code:
                        # st.success(f"✅ Status: {status_code}") if status_code == 200 else st.warning(f"⚠️ Status: {status_code}")
                        st.json(response)
                    else:
                        st.error(f"❌ Error: {response}")
        
        with col2:
            if st.button("📊 GET /health/cache/stats", key="cache_stats"):
                with st.spinner("Fetching cache stats..."):
                    status_code, response = make_api_request("GET", "/health/cache/stats")
                    if status_code:
                        st.info(f"Status: {status_code}")
                        st.json(response)
                    else:
                        st.error(f"❌ Error: {response}")
        
        st.markdown("---")
        
        st.markdown("#### 🗑️ Flush Cache")
        st.warning("⚠️ This will clear ALL cached data from Redis!")
        
        if st.button("🗑️ DELETE /health/cache/flush", key="cache_flush", type="secondary"):
            with st.spinner("Flushing cache..."):
                status_code, response = make_api_request("DELETE", "/health/cache/flush")
                if status_code and response:
                    if response.get("success"):
                        st.success("✅ Cache flushed successfully!")
                    else:
                        st.error(f"❌ Failed: {response.get('error', 'Unknown error')}")
                    st.json(response)
                else:
                    st.error(f"❌ Error: {response}")
    
    # =========================================================================
    # INDUSTRIES API
    # =========================================================================
    elif api_category == "Industries":
        st.markdown("### 🏭 Industries API")
        st.info("💾 **Redis Caching**: Industries are cached for 1 hour (3600 seconds)")

        # Data Table View
        st.markdown("#### 📊 Current Data in Snowflake")
        if st.button("🔄 Refresh Industries Table", key="refresh_industries"):
            st.rerun()

        status_code, response = make_api_request("GET", "/api/v1/industries")
        if status_code == 200 and response and "items" in response:
            industries = response["items"]
            if industries:
                df = pd.DataFrame(industries)
                display_cols = ["id", "name", "sector", "h_r_base"]
                available_cols = [c for c in display_cols if c in df.columns]
                if available_cols:
                    df = df[available_cols]
                    col_names = {"id": "ID", "name": "Name", "sector": "Sector", "h_r_base": "H/R Base"}
                    df.columns = [col_names.get(c, c) for c in available_cols]
                st.dataframe(df, use_container_width=True, hide_index=True)
                display_cache_info(response)
            else:
                st.warning("No industries found in database")
        elif status_code:
            st.error(f"Error fetching industries: {response}")
        else:
            st.warning("⚠️ Cannot connect to API")

        st.markdown("---")

        st.markdown("#### 🔍 Get Industry by ID")
        industry_id = st.text_input("Industry UUID", value="550e8400-e29b-41d4-a716-446655440001", key="ind_id")

        if st.button("GET /api/v1/industries/{id}", key="industry_get"):
            if industry_id:
                with st.spinner("Fetching industry..."):
                    status_code, response = make_api_request("GET", f"/api/v1/industries/{industry_id}")
                    if status_code:
                        st.info(f"Status: {status_code}")
                        display_cache_info(response)
                        st.json(response)
                    else:
                        st.error(f"Error: {response}")
            else:
                st.warning("Please enter an Industry UUID")
    
    # =========================================================================
    # COMPANIES API
    # =========================================================================
    elif api_category == "Companies":
        st.markdown("### 🏢 Companies API")
        st.info("💾 **Redis Caching**: Companies are cached for 5 minutes (300 seconds)")

        # Data Table View
        st.markdown("#### 📊 Current Data in Snowflake")
        if st.button("🔄 Refresh Companies Table", key="refresh_companies"):
            st.rerun()

        status_code, response = make_api_request("GET", "/api/v1/companies", params={"page_size": 100})
        if status_code == 200 and response and "items" in response:
            companies = response["items"]
            if companies:
                df = pd.DataFrame(companies)
                df = df[["id", "name", "ticker", "industry_id", "position_factor"]]
                df.columns = ["ID", "Name", "Ticker", "Industry ID", "Position Factor"]
                st.dataframe(df, use_container_width=True, hide_index=True)
                st.caption(f"Total: {response.get('total', len(companies))} companies")
                display_cache_info(response)
            else:
                st.warning("No companies found in database")
        elif status_code:
            st.error(f"Error fetching companies: {response}")
        else:
            st.warning("⚠️ Cannot connect to API")

        st.markdown("---")

        # CREATE COMPANY
        st.markdown("#### ➕ Create Company")
        
        with st.form("create_company_form"):
            new_comp_name = st.text_input("Company Name *")
            new_comp_ticker = st.text_input("Ticker Symbol")
            new_comp_industry = st.text_input("Industry ID (UUID) *", value="550e8400-e29b-41d4-a716-446655440001")
            new_comp_position = st.slider("Position Factor", -1.0, 1.0, 0.0, 0.1)
            
            if st.form_submit_button("POST /api/v1/companies"):
                if new_comp_name and new_comp_industry:
                    data = {
                        "name": new_comp_name,
                        "industry_id": new_comp_industry,
                        "position_factor": new_comp_position
                    }
                    if new_comp_ticker:
                        data["ticker"] = new_comp_ticker
                    
                    with st.spinner("Creating company..."):
                        status_code, response = make_api_request("POST", "/api/v1/companies", data=data)
                        if status_code == 201:
                            st.success(f"✅ Company created! Status: {status_code}")
                            st.caption("🔄 Cache invalidated - list cache cleared")
                        else:
                            st.error(f"❌ Status: {status_code}")
                        st.json(response)
                else:
                    st.warning("Please fill required fields (*)")
        
        st.markdown("---")
        
        # GET COMPANIES
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("#### 📋 List Companies")
            page_num = st.number_input("Page", min_value=1, value=1, key="comp_page")
            page_size = st.number_input("Page Size", min_value=1, max_value=100, value=20, key="comp_size")
            
            if st.button("GET /api/v1/companies", key="companies_list"):
                with st.spinner("Fetching companies..."):
                    status_code, response = make_api_request(
                        "GET", "/api/v1/companies",
                        params={"page": page_num, "page_size": page_size}
                    )
                    if status_code:
                        st.info(f"Status: {status_code}")
                        display_cache_info(response)
                        st.json(response)
                    else:
                        st.error(f"Error: {response}")
        
        with col2:
            st.markdown("#### 🔍 Get Company by ID")
            company_id = st.text_input("Company UUID", key="comp_id")
            
            if st.button("GET /api/v1/companies/{id}", key="company_get"):
                if company_id:
                    with st.spinner("Fetching company..."):
                        status_code, response = make_api_request("GET", f"/api/v1/companies/{company_id}")
                        if status_code:
                            st.info(f"Status: {status_code}")
                            display_cache_info(response)
                            st.json(response)
                        else:
                            st.error(f"Error: {response}")
                else:
                    st.warning("Please enter a Company UUID")
        
        st.markdown("---")
        
        # UPDATE COMPANY
        st.markdown("#### 🔄 Update Company")
        col1, col2 = st.columns(2)
        with col1:
            update_comp_id = st.text_input("Company ID to Update", key="update_comp_id")
        with col2:
            update_comp_name = st.text_input("New Company Name", key="update_comp_name")
            update_comp_position = st.slider("New Position Factor", -1.0, 1.0, 0.0, 0.1, key="update_comp_pos")
        
        if st.button("PUT /api/v1/companies/{id}", key="company_update"):
            if update_comp_id:
                data = {}
                if update_comp_name:
                    data["name"] = update_comp_name
                data["position_factor"] = update_comp_position
                
                with st.spinner("Updating company..."):
                    status_code, response = make_api_request("PUT", f"/api/v1/companies/{update_comp_id}", data=data)
                    if status_code == 200:
                        st.success(f"✅ Company updated! Status: {status_code}")
                        st.caption("🔄 Cache invalidated for this company")
                    else:
                        st.error(f"❌ Status: {status_code}")
                    st.json(response)
            else:
                st.warning("Please enter a Company ID")
        
        st.markdown("---")
        
        # DELETE COMPANY
        st.markdown("#### 🗑️ Delete Company")
        delete_comp_id = st.text_input("Company UUID to Delete", key="delete_comp_id")
        if st.button("DELETE /api/v1/companies/{id}", key="company_delete"):
            if delete_comp_id:
                with st.spinner("Deleting company..."):
                    status_code, response = make_api_request("DELETE", f"/api/v1/companies/{delete_comp_id}")
                    if status_code == 204:
                        st.success(f"✅ Company deleted! Status: {status_code}")
                        st.caption("🔄 Cache invalidated")
                    else:
                        st.error(f"❌ Status: {status_code}")
                        if response:
                            st.json(response)
            else:
                st.warning("Please enter a Company UUID")
    
    # =========================================================================
    # ASSESSMENTS API
    # =========================================================================
    elif api_category == "Assessments":
        st.markdown("### 📋 Assessments API")

        # Data Table View
        st.markdown("#### 📊 Current Data in Snowflake")
        if st.button("🔄 Refresh Assessments Table", key="refresh_assessments"):
            st.rerun()

        status_code, response = make_api_request("GET", "/api/v1/assessments", params={"page_size": 100})
        if status_code == 200 and response and "items" in response:
            assessments = response["items"]
            if assessments:
                df = pd.DataFrame(assessments)
                display_cols = ["id", "company_id", "assessment_type", "assessment_date", "status"]
                df = df[[c for c in display_cols if c in df.columns]]
                df.columns = ["ID", "Company ID", "Type", "Date", "Status"]
                st.dataframe(df, use_container_width=True, hide_index=True)
                st.caption(f"Total: {response.get('total', len(assessments))} assessments")
            else:
                st.warning("No assessments found in database")
        elif status_code:
            st.error(f"Error fetching assessments: {response}")
        else:
            st.warning("⚠️ Cannot connect to API")

        st.markdown("---")

        # CREATE ASSESSMENT
        st.markdown("#### ➕ Create Assessment")
        
        with st.form("create_assessment_form"):
            assess_company = st.text_input("Company ID (UUID) *")
            assess_type = st.selectbox("Assessment Type *", ["screening", "due_diligence", "quarterly", "exit_prep"])
            assess_date = st.date_input("Assessment Date *")
            assess_primary = st.text_input("Primary Assessor")
            
            if st.form_submit_button("POST /api/v1/assessments"):
                data = {
                    "company_id": assess_company,
                    "assessment_type": assess_type,
                    "assessment_date": str(assess_date)
                }
                if assess_primary:
                    data["primary_assessor"] = assess_primary
                
                with st.spinner("Creating assessment..."):
                    status_code, response = make_api_request("POST", "/api/v1/assessments", data=data)
                    if status_code == 201:
                        st.success(f"✅ Assessment created! Status: {status_code}")
                    else:
                        st.error(f"❌ Status: {status_code}")
                    st.json(response)
        
        st.markdown("---")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("#### 📋 List Assessments")
            if st.button("GET /api/v1/assessments", key="assess_list"):
                with st.spinner("Fetching assessments..."):
                    status_code, response = make_api_request("GET", "/api/v1/assessments")
                    if status_code:
                        st.info(f"Status: {status_code}")
                        st.json(response)
                    else:
                        st.error(f"Error: {response}")
        
        with col2:
            st.markdown("#### 🔍 Get Assessment by ID")
            assess_id = st.text_input("Assessment UUID", key="assess_id")
            
            if st.button("GET /api/v1/assessments/{id}", key="assess_get"):
                if assess_id:
                    with st.spinner("Fetching assessment..."):
                        status_code, response = make_api_request("GET", f"/api/v1/assessments/{assess_id}")
                        if status_code:
                            st.info(f"Status: {status_code}")
                            st.json(response)
                        else:
                            st.error(f"Error: {response}")
    
    # =========================================================================
    # DIMENSION SCORES API
    # =========================================================================
    elif api_category == "Dimension Scores":
        st.markdown("### 📊 Dimension Scores API")

        # Data Table View - need assessment ID to fetch scores
        st.markdown("#### 📊 View Scores by Assessment")

        # First get assessments for dropdown
        _, assess_response = make_api_request("GET", "/api/v1/assessments", params={"page_size": 100})
        assessment_options = {}
        if assess_response and "items" in assess_response:
            for a in assess_response["items"]:
                label = f"{a['id'][:8]}... ({a['assessment_type']} - {a['status']})"
                assessment_options[label] = a["id"]

        if assessment_options:
            selected_assess_label = st.selectbox("Select Assessment", list(assessment_options.keys()), key="score_view_assess")
            selected_assess_id = assessment_options[selected_assess_label]

            if st.button("🔄 Load Scores", key="refresh_scores"):
                pass  # Just triggers rerun with selection

            status_code, response = make_api_request("GET", f"/api/v1/assessments/{selected_assess_id}/scores")
            if status_code == 200 and response:
                if isinstance(response, list) and len(response) > 0:
                    df = pd.DataFrame(response)
                    display_cols = ["dimension", "score", "weight", "confidence", "evidence_count"]
                    df = df[[c for c in display_cols if c in df.columns]]
                    df.columns = ["Dimension", "Score", "Weight", "Confidence", "Evidence Count"]
                    st.dataframe(df, use_container_width=True, hide_index=True)
                else:
                    st.info("No scores found for this assessment")
            elif status_code == 404:
                st.info("No scores found for this assessment")
            elif status_code:
                st.error(f"Error: {response}")
        else:
            st.warning("No assessments available. Create an assessment first.")

        st.markdown("---")

        st.markdown("#### ➕ Add Dimension Score")
        
        with st.form("add_score_form"):
            score_assess = st.text_input("Assessment ID (UUID) *")
            score_dimension = st.selectbox("Dimension *", [
                "data_infrastructure", "ai_governance", "technology_stack",
                "talent_skills", "leadership_vision", "use_case_portfolio", "culture_change"
            ])
            score_value = st.slider("Score *", 0.0, 100.0, 75.0, 0.5)
            score_confidence = st.slider("Confidence", 0.0, 1.0, 0.8, 0.05)
            
            if st.form_submit_button("POST /api/v1/assessments/{id}/scores"):
                if score_assess:
                    data = {
                        "assessment_id": score_assess,
                        "dimension": score_dimension,
                        "score": score_value,
                        "confidence": score_confidence
                    }
                    with st.spinner("Adding score..."):
                        status_code, response = make_api_request(
                            "POST", f"/api/v1/assessments/{score_assess}/scores", data=data
                        )
                        if status_code == 201:
                            st.success(f"✅ Score added! Status: {status_code}")
                        else:
                            st.error(f"❌ Status: {status_code}")
                        st.json(response)
        
        st.markdown("---")
        
        st.markdown("#### 📋 Get Scores for Assessment")
        score_assess_id = st.text_input("Assessment UUID", key="score_assess_id")
        
        if st.button("GET /api/v1/assessments/{id}/scores", key="scores_get"):
            if score_assess_id:
                with st.spinner("Fetching scores..."):
                    status_code, response = make_api_request("GET", f"/api/v1/assessments/{score_assess_id}/scores")
                    if status_code:
                        st.info(f"Status: {status_code}")
                        st.json(response)
                    else:
                        st.error(f"Error: {response}")

# =============================================================================
# PAGE: TEST RUNNER
# =============================================================================

# =============================================================================
# PAGE: TEST RUNNER (Beautified with All Features)
# =============================================================================
# Replace the entire "elif page == "🧪 Test Runner":" section in your app.py

elif page == "🧪 Test Runner":
    st.markdown('<p class="main-header">🧪 Test Runner</p>', unsafe_allow_html=True)
    
    # Additional CSS for test results
    st.markdown("""
    <style>
        .metric-card {
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            padding: 1rem;
            border-radius: 0.75rem;
            color: white;
            text-align: center;
            box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
            margin-bottom: 0.5rem;
        }
        .metric-card-success {
            background: linear-gradient(135deg, #11998e 0%, #38ef7d 100%);
        }
        .metric-card-danger {
            background: linear-gradient(135deg, #eb3349 0%, #f45c43 100%);
        }
        .metric-card-warning {
            background: linear-gradient(135deg, #f2994a 0%, #f2c94c 100%);
        }
        .metric-card-info {
            background: linear-gradient(135deg, #4facfe 0%, #00f2fe 100%);
        }
        .metric-value {
            font-size: 2rem;
            font-weight: bold;
            margin: 0;
        }
        .metric-label {
            font-size: 0.85rem;
            opacity: 0.9;
            margin-top: 0.25rem;
        }
        .test-item-pass {
            background-color: #d1fae5;
            border-left: 4px solid #10b981;
            padding: 0.4rem 0.75rem;
            margin: 0.2rem 0;
            border-radius: 0 0.25rem 0.25rem 0;
            font-family: monospace;
            font-size: 0.8rem;
        }
        .test-item-fail {
            background-color: #fee2e2;
            border-left: 4px solid #ef4444;
            padding: 0.4rem 0.75rem;
            margin: 0.2rem 0;
            border-radius: 0 0.25rem 0.25rem 0;
            font-family: monospace;
            font-size: 0.8rem;
        }
        .test-item-skip {
            background-color: #fef3c7;
            border-left: 4px solid #f59e0b;
            padding: 0.4rem 0.75rem;
            margin: 0.2rem 0;
            border-radius: 0 0.25rem 0.25rem 0;
            font-family: monospace;
            font-size: 0.8rem;
        }
        .warning-item {
            background-color: #fef9c3;
            border-left: 4px solid #eab308;
            padding: 0.5rem 0.75rem;
            margin: 0.3rem 0;
            border-radius: 0 0.25rem 0.25rem 0;
            font-size: 0.8rem;
        }
        .skip-reason {
            color: #92400e;
            font-style: italic;
            margin-left: 0.5rem;
        }
        .warning-reason {
            color: #854d0e;
            font-size: 0.75rem;
            margin-top: 0.25rem;
        }
    </style>
    """, unsafe_allow_html=True)
    
    def parse_pytest_output(output: str) -> dict:
        """Parse pytest output to extract test results."""
        import re
        
        results = {
            "passed": 0,
            "failed": 0,
            "skipped": 0,
            "warnings": 0,
            "errors": 0,
            "total": 0,
            "duration": "0.00s",
            "test_cases": [],
            "failed_tests": [],
            "skipped_tests": [],
            "warning_messages": [],
            "collection_count": 0
        }
        
        if not output:
            return results
        
        lines = output.split('\n')
        
        # Track if we're in warnings section
        in_warnings_section = False
        current_warning = []
        
        for i, line in enumerate(lines):
            line_stripped = line.strip()
            
            # Detect warnings section
            if 'warnings summary' in line.lower():
                in_warnings_section = True
                continue
            
            if in_warnings_section:
                if line_stripped.startswith('--') or 'passed' in line.lower() or 'failed' in line.lower():
                    in_warnings_section = False
                    if current_warning:
                        results["warning_messages"].append('\n'.join(current_warning))
                        current_warning = []
                elif line_stripped and not line_stripped.startswith('='):
                    # Check if this is a new warning (starts with a file path)
                    if re.match(r'^[\w\\/:._-]+:\d+', line_stripped) or line_stripped.startswith('C:\\') or line_stripped.startswith('/'):
                        if current_warning:
                            results["warning_messages"].append('\n'.join(current_warning))
                        current_warning = [line_stripped]
                    elif current_warning:
                        current_warning.append(line_stripped)
            
            # Detect test results
            if '::' in line and ('PASSED' in line or 'FAILED' in line or 'SKIPPED' in line):
                try:
                    parts = line.split('::')
                    if len(parts) >= 2:
                        test_file = parts[0].split('/')[-1] if '/' in parts[0] else parts[0].split('\\')[-1]
                        test_class = parts[1] if len(parts) > 2 else ""
                        test_name = parts[-1].split()[0] if parts[-1] else ""
                        
                        status = "passed" if "PASSED" in line else "failed" if "FAILED" in line else "skipped"
                        
                        percentage_match = re.search(r'\[\s*(\d+)%\]', line)
                        percentage = percentage_match.group(1) if percentage_match else ""
                        
                        test_info = {
                            "file": test_file,
                            "class": test_class,
                            "name": test_name,
                            "status": status,
                            "percentage": percentage,
                            "full_line": line_stripped,
                            "reason": ""
                        }
                        
                        # Look for skip reason
                        if status == "skipped":
                            skip_match = re.search(r'SKIPPED\s*(?:\[.*?\])?\s*[-:]?\s*(.*)', line)
                            if skip_match:
                                test_info["reason"] = skip_match.group(1).strip()
                            results["skipped_tests"].append(test_info)
                        
                        results["test_cases"].append(test_info)
                        
                        if status == "failed":
                            results["failed_tests"].append(f"{test_class}::{test_name}")
                except:
                    pass
            
            # Detect collection count
            if 'collected' in line.lower():
                match = re.search(r'collected\s+(\d+)\s+item', line)
                if match:
                    results["collection_count"] = int(match.group(1))
        
        # Parse summary line
        for line in lines:
            if re.search(r'\d+\s+(passed|failed)', line) and ('in ' in line or 'second' in line):
                passed_match = re.search(r'(\d+)\s+passed', line)
                failed_match = re.search(r'(\d+)\s+failed', line)
                skipped_match = re.search(r'(\d+)\s+skipped', line)
                warning_match = re.search(r'(\d+)\s+warning', line)
                error_match = re.search(r'(\d+)\s+error', line)
                duration_match = re.search(r'in\s+([\d.]+\s*s)', line)
                
                if passed_match:
                    results["passed"] = int(passed_match.group(1))
                if failed_match:
                    results["failed"] = int(failed_match.group(1))
                if skipped_match:
                    results["skipped"] = int(skipped_match.group(1))
                if warning_match:
                    results["warnings"] = int(warning_match.group(1))
                if error_match:
                    results["errors"] = int(error_match.group(1))
                if duration_match:
                    results["duration"] = duration_match.group(1)
                break
        
        results["total"] = results["passed"] + results["failed"] + results["skipped"]
        
        return results
    
    def display_test_results(results: dict, test_type: str):
        """Display beautiful test results."""
        total = results["total"]
        passed = results["passed"]
        failed = results["failed"]
        skipped = results["skipped"]
        warnings = results["warnings"]
        duration = results["duration"]
        
        if total == 0:
            st.warning(f"No tests were executed for {test_type}")
            return
        
        pass_rate = (passed / total * 100) if total > 0 else 0
        
        # Overall status banner
        if failed == 0:
            st.markdown(f"""
            <div style="background: linear-gradient(135deg, #10b981 0%, #059669 100%); 
                        color: white; padding: 1.25rem; border-radius: 0.75rem; text-align: center; margin-bottom: 1rem;">
                <h2 style="margin: 0; font-size: 1.5rem;">✅ All {total} Tests Passed!</h2>
                <p style="margin: 0.5rem 0 0 0; opacity: 0.9;">Completed in {duration}</p>
            </div>
            """, unsafe_allow_html=True)
        else:
            st.markdown(f"""
            <div style="background: linear-gradient(135deg, #ef4444 0%, #dc2626 100%); 
                        color: white; padding: 1.25rem; border-radius: 0.75rem; text-align: center; margin-bottom: 1rem;">
                <h2 style="margin: 0; font-size: 1.5rem;">❌ {failed} Test{'s' if failed > 1 else ''} Failed</h2>
                <p style="margin: 0.5rem 0 0 0; opacity: 0.9;">{passed} passed, {skipped} skipped in {duration}</p>
            </div>
            """, unsafe_allow_html=True)
        
        # Metrics row
        col1, col2, col3, col4, col5 = st.columns(5)
        
        with col1:
            st.markdown(f"""
            <div class="metric-card metric-card-info">
                <p class="metric-value">{total}</p>
                <p class="metric-label">Total</p>
            </div>
            """, unsafe_allow_html=True)
        
        with col2:
            st.markdown(f"""
            <div class="metric-card metric-card-success">
                <p class="metric-value">{passed}</p>
                <p class="metric-label">Passed ✓</p>
            </div>
            """, unsafe_allow_html=True)
        
        with col3:
            st.markdown(f"""
            <div class="metric-card metric-card-danger">
                <p class="metric-value">{failed}</p>
                <p class="metric-label">Failed ✗</p>
            </div>
            """, unsafe_allow_html=True)
        
        with col4:
            st.markdown(f"""
            <div class="metric-card metric-card-warning">
                <p class="metric-value">{skipped}</p>
                <p class="metric-label">Skipped ⊘</p>
            </div>
            """, unsafe_allow_html=True)
        
        with col5:
            st.markdown(f"""
            <div class="metric-card">
                <p class="metric-value">{warnings}</p>
                <p class="metric-label">Warnings ⚠</p>
            </div>
            """, unsafe_allow_html=True)
        
        # Progress bar
        st.markdown(f"""
        <div style="margin: 1rem 0;">
            <div style="display: flex; justify-content: space-between; margin-bottom: 0.5rem;">
                <span><strong>Pass Rate: {pass_rate:.1f}%</strong></span>
                <span style="color: #6b7280;">⏱️ {duration}</span>
            </div>
            <div style="background: #e5e7eb; border-radius: 0.5rem; height: 1.25rem; overflow: hidden; display: flex;">
                <div style="width: {passed/total*100 if total else 0}%; background: #10b981; height: 100%;"></div>
                <div style="width: {failed/total*100 if total else 0}%; background: #ef4444; height: 100%;"></div>
                <div style="width: {skipped/total*100 if total else 0}%; background: #f59e0b; height: 100%;"></div>
            </div>
        </div>
        """, unsafe_allow_html=True)
        
        # Failed tests with details
        if results["failed_tests"]:
            st.markdown("#### ❌ Failed Tests")
            for test in results["failed_tests"]:
                st.markdown(f'<div class="test-item-fail"><strong>FAILED:</strong> {test}</div>', unsafe_allow_html=True)
        
        # Skipped tests with reasons
        if results["skipped_tests"]:
            st.markdown("#### ⊘ Skipped Tests")
            for test in results["skipped_tests"]:
                reason = test.get("reason", "No reason provided")
                st.markdown(f'''
                <div class="test-item-skip">
                    <strong>SKIPPED:</strong> {test["class"]}::{test["name"]}
                    <span class="skip-reason">→ {reason if reason else "No reason provided"}</span>
                </div>
                ''', unsafe_allow_html=True)
        
        # Warnings with details
        if results["warning_messages"]:
            st.markdown("#### ⚠️ Warnings")
            # Group similar warnings
            warning_counts = {}
            for warn in results["warning_messages"]:
                # Extract the main warning type
                if 'DeprecationWarning' in warn:
                    key = "DeprecationWarning"
                elif 'PydanticDeprecatedSince20' in warn:
                    key = "Pydantic V2 Migration Warning"
                else:
                    key = warn[:80] + "..." if len(warn) > 80 else warn
                
                if key not in warning_counts:
                    warning_counts[key] = {"count": 0, "details": warn}
                warning_counts[key]["count"] += 1
            
            for warn_type, info in warning_counts.items():
                with st.expander(f"⚠️ {warn_type} ({info['count']}x)"):
                    st.code(info["details"], language="text")
        
        # Test results grouped by class
        if results["test_cases"]:
            st.markdown("#### 📋 Test Results by Class")
            
            test_groups = {}
            for test in results["test_cases"]:
                class_name = test["class"] or "Other"
                if class_name not in test_groups:
                    test_groups[class_name] = []
                test_groups[class_name].append(test)
            
            for class_name, tests in test_groups.items():
                passed_in_group = sum(1 for t in tests if t["status"] == "passed")
                failed_in_group = sum(1 for t in tests if t["status"] == "failed")
                skipped_in_group = sum(1 for t in tests if t["status"] == "skipped")
                total_in_group = len(tests)
                
                status_emoji = "✅" if failed_in_group == 0 else "❌"
                
                with st.expander(f"{status_emoji} {class_name} ({passed_in_group}/{total_in_group} passed)"):
                    for test in tests:
                        if test["status"] == "passed":
                            st.markdown(f'<div class="test-item-pass">✓ {test["name"]}</div>', unsafe_allow_html=True)
                        elif test["status"] == "failed":
                            st.markdown(f'<div class="test-item-fail">✗ {test["name"]}</div>', unsafe_allow_html=True)
                        else:
                            reason = test.get("reason", "")
                            st.markdown(f'<div class="test-item-skip">⊘ {test["name"]} <span class="skip-reason">{reason}</span></div>', unsafe_allow_html=True)
    
    # Store test results in session state
    if "test_results" not in st.session_state:
        st.session_state.test_results = {}
    
    st.markdown("""
    Run automated tests for the PE Org-AI-R Platform. Tests validate Pydantic models and FastAPI endpoints.
    """)
    
    st.markdown('<p class="section-header">🎯 Run Tests</p>', unsafe_allow_html=True)
    
    # Three column layout for test buttons
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown("### 📦 Model Tests")
        st.markdown("""
        Tests Pydantic model validations:
        - Dimension Score validation
        - Industry validation
        - Company validation
        - Assessment validation
        - Enum & Weight tests
        """)
        
        if st.button("▶️ Run Model Tests", key="run_models", use_container_width=True):
            with st.spinner("Running model tests..."):
                success, stdout, stderr = run_pytest("tests/test_models.py")
                st.session_state.test_results["models"] = {
                    "success": success,
                    "stdout": stdout,
                    "stderr": stderr,
                    "parsed": parse_pytest_output(stdout)
                }
    
    with col2:
        st.markdown("### 🔌 API Tests")
        st.markdown("""
        Tests FastAPI endpoints:
        - Health endpoint
        - Company endpoints
        - Assessment endpoints
        - Dimension Score endpoints
        - Error responses
        """)
        
        if st.button("▶️ Run API Tests", key="run_api", use_container_width=True):
            with st.spinner("Running API tests..."):
                success, stdout, stderr = run_pytest("tests/test_api.py")
                st.session_state.test_results["api"] = {
                    "success": success,
                    "stdout": stdout,
                    "stderr": stderr,
                    "parsed": parse_pytest_output(stdout)
                }
    
    with col3:
        st.markdown("### 🚀 All Tests")
        st.markdown("""
        Run complete test suite:
        - All model tests
        - All API tests
        - Full coverage
        """)
        
        if st.button("▶️ Run All Tests", key="run_all", type="primary", use_container_width=True):
            with st.spinner("Running all tests..."):
                success, stdout, stderr = run_pytest()
                st.session_state.test_results["all"] = {
                    "success": success,
                    "stdout": stdout,
                    "stderr": stderr,
                    "parsed": parse_pytest_output(stdout)
                }
                if success:
                    st.balloons()
    
    # Display results if available
    st.markdown('<p class="section-header">📊 Test Results</p>', unsafe_allow_html=True)
    
    if "models" in st.session_state.test_results:
        with st.expander("📦 Model Test Results", expanded=True):
            results = st.session_state.test_results["models"]
            display_test_results(results["parsed"], "Model Tests")
            with st.expander("📜 Raw Output"):
                st.code(results["stdout"] or results["stderr"] or "No output", language="text")
    
    if "api" in st.session_state.test_results:
        with st.expander("🔌 API Test Results", expanded=True):
            results = st.session_state.test_results["api"]
            display_test_results(results["parsed"], "API Tests")
            with st.expander("📜 Raw Output"):
                st.code(results["stdout"] or results["stderr"] or "No output", language="text")
    
    if "all" in st.session_state.test_results:
        with st.expander("🚀 All Test Results", expanded=True):
            results = st.session_state.test_results["all"]
            display_test_results(results["parsed"], "All Tests")
            with st.expander("📜 Raw Output"):
                st.code(results["stdout"] or results["stderr"] or "No output", language="text")
    
    if not st.session_state.test_results:
        st.info("👆 Click one of the buttons above to run tests and see results here.")
    
    # Test coverage summary table
    st.markdown('<p class="section-header">📋 Test Coverage Summary</p>', unsafe_allow_html=True)
    
    test_summary = pd.DataFrame({
        "Category": [
            "🔢 Enumerations",
            "⚖️ Dimension Weights",
            "📊 Dimension Score Model",
            "🏭 Industry Model",
            "🏢 Company Model",
            "📋 Assessment Model",
            "🏥 Health API",
            "🏢 Company API",
            "📋 Assessment API",
            "📊 Dimension Score API"
        ],
        "Tests": [6, 4, 10, 7, 8, 10, 9, 8, 9, 6],
        "Type": ["Model", "Model", "Model", "Model", "Model", "Model", "API", "API", "API", "API"],
        "Status": ["✅", "✅", "✅", "✅", "✅", "✅", "✅", "✅", "✅", "✅"]
    })
    
    col1, col2 = st.columns([3, 1])
    
    with col1:
        st.dataframe(
            test_summary,
            column_config={
                "Category": st.column_config.TextColumn("Category", width="medium"),
                "Tests": st.column_config.NumberColumn("# Tests", width="small"),
                "Type": st.column_config.TextColumn("Type", width="small"),
                "Status": st.column_config.TextColumn("Status", width="small")
            },
            hide_index=True,
            use_container_width=True
        )
    
    with col2:
        total_tests = test_summary["Tests"].sum()
        model_tests = test_summary[test_summary["Type"] == "Model"]["Tests"].sum()
        api_tests = test_summary[test_summary["Type"] == "API"]["Tests"].sum()
        
        st.metric("Total Tests", total_tests)
        st.metric("Model Tests", model_tests)
        st.metric("API Tests", api_tests)

# =============================================================================
# FOOTER
# =============================================================================

st.sidebar.markdown("---")
st.sidebar.markdown("### 📚 Quick Links")
st.sidebar.markdown(f"- [Swagger UI]({API_BASE_URL}/docs)")
st.sidebar.markdown(f"- [ReDoc]({API_BASE_URL}/redoc)")
st.sidebar.markdown("---")
st.sidebar.markdown("*PE Org-AI-R Platform v1.0.0*")
# streamlit/app.py
# SEC Filings & Signals Pipeline UI

from __future__ import annotations
import json
import re
import os
from typing import Any, Dict, List, Optional, Tuple
import pandas as pd
import requests
import streamlit as st
import boto3
from botocore.exceptions import ClientError

# Load environment variables (if using python-dotenv)
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# AWS S3 Configuration
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", "")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "")
AWS_REGION = os.getenv("AWS_REGION", "us-east-2")
S3_BUCKET = os.getenv("S3_BUCKET", "pe-orgair-platform-group5")

def get_s3_client():
    """Create and return an S3 client."""
    return boto3.client(
        's3',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION
    )

def fetch_from_s3(s3_key: str) -> str:
    """Fetch file content from S3."""
    try:
        s3_client = get_s3_client()
        response = s3_client.get_object(Bucket=S3_BUCKET, Key=s3_key)
        content = response['Body'].read().decode('utf-8')
        return content
    except ClientError as e:
        st.warning(f"S3 Error: {e}")
        return ""
    except Exception as e:
        st.warning(f"Error fetching from S3: {e}")
        return ""

# Configuration
st.set_page_config(page_title="SEC Filings & Signals", layout="wide", page_icon="📊")

if "base_url" not in st.session_state:
    st.session_state["base_url"] = "http://localhost:8000"
if "last_ticker" not in st.session_state:
    st.session_state["last_ticker"] = "CAT"
if "analysis_markdown" not in st.session_state:
    st.session_state["analysis_markdown"] = None

# Helper Functions
def api_url(base: str, path: str) -> str:
    return f"{base.rstrip('/')}/{path.lstrip('/')}"

def safe_json(resp: requests.Response) -> Dict[str, Any]:
    try:
        return resp.json()
    except:
        return {"_error": resp.text, "_status": resp.status_code}

def post_json(url: str, payload: Dict[str, Any], timeout_s: int = 240) -> Dict[str, Any]:
    resp = requests.post(url, json=payload, timeout=timeout_s)
    if resp.status_code >= 400:
        raise RuntimeError(f"POST failed ({resp.status_code}): {safe_json(resp).get('detail', safe_json(resp))}")
    return safe_json(resp)

def post(url: str, params: Optional[Dict[str, Any]] = None, timeout_s: int = 300) -> Dict[str, Any]:
    resp = requests.post(url, params=params, timeout=timeout_s)
    if resp.status_code >= 400:
        raise RuntimeError(f"POST failed ({resp.status_code}): {safe_json(resp).get('detail', safe_json(resp))}")
    return safe_json(resp)

def get(url: str, params: Optional[Dict[str, Any]] = None, timeout_s: int = 120) -> Dict[str, Any]:
    resp = requests.get(url, params=params, timeout=timeout_s)
    if resp.status_code >= 400:
        raise RuntimeError(f"GET failed ({resp.status_code}): {safe_json(resp).get('detail', safe_json(resp))}")
    return safe_json(resp)

def render_kpis(items: List[Tuple[str, Any]]) -> None:
    cols = st.columns(len(items))
    for i, (label, value) in enumerate(items):
        cols[i].metric(label, value)

def show_json(title: str, data: Any) -> None:
    with st.expander(title, expanded=False):
        st.code(json.dumps(data, indent=2, default=str), language="json")

def df_from_table(headers: List[str], rows: List[List[Any]]) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame()
    max_cols = max(len(r) for r in rows)
    cols = headers[:max_cols] if headers else [f"col_{i}" for i in range(max_cols)]
    while len(cols) < max_cols:
        cols.append(f"col_{len(cols)}")
    return pd.DataFrame([r + [None]*(max_cols - len(r)) for r in rows], columns=cols)

def parse_markdown_content(markdown_content: str) -> List[Dict]:
    """
    Parse markdown content and extract headings, text, and tables.
    Returns a list of dicts with 'type', 'level', 'title', 'content', 'headers', 'rows'.
    """
    elements = []
    lines = markdown_content.split('\n')
    
    i = 0
    while i < len(lines):
        line = lines[i]
        
        # Check for main title (# )
        if line.startswith('# ') and not line.startswith('## '):
            elements.append({
                'type': 'heading',
                'level': 1,
                'title': line.lstrip('#').strip(),
                'content': None
            })
            i += 1
            continue
        
        # Check for h2 headings (## )
        if line.startswith('## '):
            elements.append({
                'type': 'heading',
                'level': 2,
                'title': line.lstrip('#').strip(),
                'content': None
            })
            i += 1
            continue
        
        # Check for h3 headings (### )
        if line.startswith('### '):
            elements.append({
                'type': 'heading',
                'level': 3,
                'title': line.lstrip('#').strip(),
                'content': None
            })
            i += 1
            continue
        
        # Check for h4 headings (#### )
        if line.startswith('#### '):
            elements.append({
                'type': 'heading',
                'level': 4,
                'title': line.lstrip('#').strip(),
                'content': None
            })
            i += 1
            continue
        
        # Check if this is a table (starts with |)
        if line.strip().startswith('|'):
            table_lines = []
            while i < len(lines) and lines[i].strip().startswith('|'):
                table_lines.append(lines[i].strip())
                i += 1
            
            # Parse the table
            if len(table_lines) >= 2:
                # First line is headers
                header_line = table_lines[0]
                headers = [cell.strip() for cell in header_line.split('|') if cell.strip()]
                
                # Skip separator line (second line with dashes)
                rows = []
                for tl in table_lines[2:]:
                    cells = [cell.strip() for cell in tl.split('|') if cell.strip()]
                    if cells:
                        rows.append(cells)
                
                elements.append({
                    'type': 'table',
                    'level': 0,
                    'title': None,
                    'headers': headers,
                    'rows': rows
                })
            continue
        
        # Check for regular text (non-empty, non-heading, non-table)
        if line.strip() and not line.strip().startswith('|') and not line.startswith('#'):
            # Collect consecutive text lines
            text_content = []
            while i < len(lines) and lines[i].strip() and not lines[i].startswith('#') and not lines[i].strip().startswith('|'):
                text_content.append(lines[i].strip())
                i += 1
            
            if text_content:
                elements.append({
                    'type': 'text',
                    'level': 0,
                    'title': None,
                    'content': ' '.join(text_content)
                })
            continue
        
        i += 1
    
    return elements

def parse_markdown_tables(markdown_content: str) -> List[Dict]:
    """
    Parse markdown content and extract tables with their headers.
    Returns a list of dicts with 'title', 'headers', and 'rows'.
    """
    tables = []
    lines = markdown_content.split('\n')
    
    current_title = ""
    current_table_lines = []
    in_table = False
    
    for i, line in enumerate(lines):
        # Check for headers (## or ###)
        if line.startswith('## ') or line.startswith('### '):
            current_title = line.lstrip('#').strip()
            continue
        
        # Check if this is a table line (starts with |)
        if line.strip().startswith('|'):
            if not in_table:
                in_table = True
                current_table_lines = []
            current_table_lines.append(line.strip())
        else:
            # End of table
            if in_table and current_table_lines:
                # Parse the table
                table_data = parse_single_table(current_table_lines)
                if table_data:
                    table_data['title'] = current_title
                    tables.append(table_data)
                current_table_lines = []
                in_table = False
    
    # Handle last table if exists
    if in_table and current_table_lines:
        table_data = parse_single_table(current_table_lines)
        if table_data:
            table_data['title'] = current_title
            tables.append(table_data)
    
    return tables

def parse_single_table(table_lines: List[str]) -> Optional[Dict]:
    """Parse a single markdown table into headers and rows."""
    if len(table_lines) < 2:
        return None
    
    # First line is headers
    header_line = table_lines[0]
    headers = [cell.strip() for cell in header_line.split('|') if cell.strip()]
    
    # Skip separator line (second line with dashes)
    rows = []
    for line in table_lines[2:]:
        cells = [cell.strip() for cell in line.split('|') if cell.strip()]
        if cells:
            rows.append(cells)
    
    return {'headers': headers, 'rows': rows}

def extract_section_content(text: str, section_name: str, max_chars: int = 1500) -> str:
    if not text:
        return ""
    
    patterns = {
        'business': [
            r'Item\s*1\.\s*Business\.?\s*(.*?)(?=Item\s*1A\.|Item\s*1B\.|Item\s*2\.|Part\s*II|$)',
            r'Item\s*1[\.\s]+Business(.*?)(?=Item\s*1A|Item\s*2|Part\s*II|$)',
        ],
        'risk_factors': [
            r'Item\s*1A\.\s*Risk\s*Factors\.?\s*(.*?)(?=Item\s*1B\.|Item\s*2\.|Part\s*II|$)',
            r'Item\s*1A[\.\s]+Risk\s*Factors(.*?)(?=Item\s*1B|Item\s*2|Part\s*II|$)',
        ],
        'mda': [
            r'Item\s*7\.\s*Management.?s?\s*Discussion\s*and\s*Analysis.*?\.?\s*(.*?)(?=Item\s*7A\.|Item\s*8\.|Part\s*III|$)',
            r'Item\s*7[\.\s]+Management.?s?\s*Discussion(.*?)(?=Item\s*7A|Item\s*8|Part\s*III|$)',
        ],
    }
    
    section_patterns = patterns.get(section_name, [])
    
    for pattern in section_patterns:
        match = re.search(pattern, text, re.IGNORECASE | re.DOTALL)
        if match:
            content = match.group(1).strip()
            content = re.sub(r'\s+', ' ', content)
            if len(content) > 50:
                return content[:max_chars] + ("..." if len(content) > max_chars else "")
    
    return ""

def extract_lines_containing(text: str, search_term: str, num_lines: int = 5, chars_per_line: int = 300) -> str:
    if not text:
        return ""
    
    results = []
    search_lower = search_term.lower()
    text_lower = text.lower()
    pos = 0
    
    while len(results) < num_lines:
        idx = text_lower.find(search_lower, pos)
        if idx == -1:
            break
        
        start = max(0, idx - 50)
        end = min(len(text), idx + chars_per_line)
        
        while start > 0 and text[start] not in ' \n\t':
            start -= 1
        
        snippet = text[start:end].strip()
        snippet = re.sub(r'\s+', ' ', snippet)
        
        if snippet and len(snippet) > 20:
            results.append(snippet)
        
        pos = idx + len(search_term) + 100
    
    return "\n\n---\n\n".join(results) if results else ""

def load_json_file_content(s3_key: str, ticker: str, filing_type: str, filing_date: str) -> Tuple[str, str]:
    keys_to_try = [
        s3_key,
        f"sec/parsed/{ticker}/{filing_type}/{filing_date}_full.json",
        f"parsed/{ticker}/{filing_type}/{filing_date}_full.json",
    ]
    
    for key in keys_to_try:
        if not key:
            continue
        try:
            content = fetch_from_s3(key)
            if content:
                if content.strip().startswith('{'):
                    data = json.loads(content)
                    text = (data.get('text', '') or 
                            data.get('content', '') or 
                            data.get('full_text', '') or 
                            data.get('raw_text', '') or
                            content)
                    return text, f"s3://{S3_BUCKET}/{key}"
                else:
                    return content, f"s3://{S3_BUCKET}/{key}"
        except Exception as e:
            continue
    
    return "", ""

def fetch_documents_table(base_url: str, ticker: Optional[str] = None) -> None:
    try:
        params = {"ticker": ticker, "limit": 100} if ticker else {"limit": 100}
        data = get(api_url(base_url, "/api/v1/documents"), params=params)
        docs = data.get("documents", [])
        if docs:
            st.subheader("📋 Documents Table (Snowflake)")
            df = pd.DataFrame([{
                "ID": d.get("id", "")[:12] + "...",
                "Ticker": d.get("ticker"),
                "Filing Type": d.get("filing_type"),
                "Filing Date": str(d.get("filing_date", ""))[:10],
                "Status": d.get("status"),
                "S3 Key": (d.get("s3_key") or "")[:30] + "...",
                "Words": d.get("word_count", 0),
                "Chunks": d.get("chunk_count", 0)
            } for d in docs])
            st.dataframe(df, use_container_width=True, hide_index=True)
            st.caption(f"Total: {data.get('count', len(docs))} documents")
        else:
            st.info("No documents found in Snowflake")
    except Exception as e:
        st.error(f"❌ Error fetching documents: {e}")

def fetch_chunks_table(base_url: str, ticker: Optional[str] = None) -> None:
    try:
        params = {"ticker": ticker, "limit": 50} if ticker else {"limit": 50}
        doc_data = get(api_url(base_url, "/api/v1/documents"), params=params)
        docs = doc_data.get("documents", [])
        
        all_chunks = []
        for doc in docs[:10]:
            doc_id = doc.get("id")
            if doc_id:
                try:
                    chunk_data = get(api_url(base_url, f"/api/v1/documents/chunks/{doc_id}"))
                    chunks = chunk_data.get("chunks", [])
                    for c in chunks[:5]:
                        all_chunks.append({
                            "Chunk ID": c.get("id", "")[:12] + "...",
                            "Document ID": doc_id[:12] + "...",
                            "Ticker": doc.get("ticker"),
                            "Index": c.get("chunk_index"),
                            "Section": c.get("section"),
                            "Words": c.get("word_count"),
                            "Content Preview": (c.get("content") or "")[:80] + "..."
                        })
                except:
                    pass
        
        if all_chunks:
            st.subheader("📦 Document Chunks Table (Snowflake)")
            st.dataframe(pd.DataFrame(all_chunks), use_container_width=True, hide_index=True)
            st.caption(f"Showing sample of chunks (limited for display)")
        else:
            st.info("No chunks found")
    except Exception as e:
        st.error(f"❌ Error fetching chunks: {e}")

# Sidebar Navigation
st.sidebar.title("📊 SEC Pipeline")
st.sidebar.divider()

main_section = st.sidebar.selectbox("Select Section", ["SEC Filings", "Signals"], index=0)

if main_section == "SEC Filings":
    sub_page = st.sidebar.radio(
        "Pipeline Step",
        ["1. Download Filings", "2. Parsing", "3. PDF Parsing", "4. De-duplication", "5. Chunking", "📊 Reports", "📈 SEC Analysis Summary"],
        index=0
    )
else:
    sub_page = st.sidebar.radio("Signal Type", ["Leadership Score"], index=0)

st.sidebar.divider()
st.sidebar.subheader("⚙️ API Settings")
base_url = st.sidebar.text_input("FastAPI URL", value=st.session_state["base_url"])
st.session_state["base_url"] = base_url

if st.sidebar.button("🔍 Health Check", use_container_width=True):
    try:
        get(api_url(base_url, "/health"))
        st.sidebar.success("✅ API Connected")
    except Exception as e:
        st.sidebar.error(f"❌ {str(e)[:50]}")

# SEC Filings Section
if main_section == "SEC Filings":
    st.title("📄 SEC Filings Pipeline")
    
    # Page 1: Download Filings
    if sub_page == "1. Download Filings":
        st.header("Step 1: Download SEC Filings")
        
        st.warning("⚠️ We already have data present in the backend, so please delete a company first and then proceed with the downloading of the filings.")
        
        with st.expander("🗑️ Delete Existing Data", expanded=False):
            del_ticker = st.text_input("Ticker to Delete", value=st.session_state["last_ticker"], key="del_ticker").upper().strip()
            
            del_col1, del_col2 = st.columns(2)
            with del_col1:
                if st.button("🗑️ Delete ALL Data", type="secondary", use_container_width=True, key="del_all"):
                    if del_ticker:
                        try:
                            with st.spinner(f"Deleting all data for {del_ticker}..."):
                                resp = requests.delete(api_url(base_url, f"/api/v1/documents/reset/{del_ticker}"), timeout=120)
                            if resp.status_code < 400:
                                st.success(f"✅ All data deleted for {del_ticker}")
                                st.json(safe_json(resp))
                            else:
                                st.error(f"❌ Error: {safe_json(resp).get('detail', safe_json(resp))}")
                        except Exception as e:
                            st.error(f"❌ Error: {e}")
                
                if st.button("🗑️ Delete RAW Files Only", type="secondary", use_container_width=True, key="del_raw"):
                    if del_ticker:
                        try:
                            with st.spinner(f"Deleting raw files for {del_ticker}..."):
                                resp = requests.delete(api_url(base_url, f"/api/v1/documents/reset/{del_ticker}/raw"), timeout=120)
                            if resp.status_code < 400:
                                st.success(f"✅ Raw files deleted for {del_ticker}")
                                st.json(safe_json(resp))
                            else:
                                st.error(f"❌ Error: {safe_json(resp).get('detail', safe_json(resp))}")
                        except Exception as e:
                            st.error(f"❌ Error: {e}")
            
            with del_col2:
                if st.button("🗑️ Delete PARSED Files Only", type="secondary", use_container_width=True, key="del_parsed"):
                    if del_ticker:
                        try:
                            with st.spinner(f"Deleting parsed files for {del_ticker}..."):
                                resp = requests.delete(api_url(base_url, f"/api/v1/documents/reset/{del_ticker}/parsed"), timeout=120)
                            if resp.status_code < 400:
                                st.success(f"✅ Parsed files deleted for {del_ticker}")
                                st.json(safe_json(resp))
                            else:
                                st.error(f"❌ Error: {safe_json(resp).get('detail', safe_json(resp))}")
                        except Exception as e:
                            st.error(f"❌ Error: {e}")
                
                if st.button("🗑️ Delete CHUNKS Only", type="secondary", use_container_width=True, key="del_chunks"):
                    if del_ticker:
                        try:
                            with st.spinner(f"Deleting chunks for {del_ticker}..."):
                                resp = requests.delete(api_url(base_url, f"/api/v1/documents/reset/{del_ticker}/chunks"), timeout=120)
                            if resp.status_code < 400:
                                st.success(f"✅ Chunks deleted for {del_ticker}")
                                st.json(safe_json(resp))
                            else:
                                st.error(f"❌ Error: {safe_json(resp).get('detail', safe_json(resp))}")
                        except Exception as e:
                            st.error(f"❌ Error: {e}")
        
        st.divider()
        st.info("📥 Downloads filings from SEC EDGAR → Uploads to S3 → Saves metadata to Snowflake")
        
        st.subheader("Option 1: Download by Ticker")
        with st.form("collect_form"):
            c1, c2 = st.columns(2)
            with c1:
                ticker = st.text_input("Ticker Symbol", value=st.session_state["last_ticker"]).upper().strip()
                filing_types = st.multiselect("Filing Types", ["10-K", "10-Q", "8-K", "DEF 14A"], default=["10-K", "10-Q", "8-K", "DEF 14A"])
            with c2:
                years_back = st.slider("Years Back", 1, 10, 3)
            submitted = st.form_submit_button("📥 Download Filings", use_container_width=True, type="primary")
        
        if submitted and ticker and filing_types:
            st.session_state["last_ticker"] = ticker
            try:
                with st.spinner(f"Collecting filings for {ticker}..."):
                    data = post_json(api_url(base_url, "/api/v1/documents/collect"),
                        {"ticker": ticker, "filing_types": filing_types, "years_back": years_back})
                st.success("✅ Collection Complete!")
                render_kpis([("Found", data.get("documents_found", 0)), ("Uploaded", data.get("documents_uploaded", 0)),
                             ("Skipped", data.get("documents_skipped", 0)), ("Failed", data.get("documents_failed", 0))])
                
                summary = data.get("summary", {})
                if summary:
                    st.markdown("#### Summary by Filing Type")
                    st.dataframe(pd.DataFrame([{"Filing Type": k, "Count": v} for k, v in summary.items()]), use_container_width=True, hide_index=True)
                
                show_json("Raw JSON Response", data)
                st.divider()
                fetch_documents_table(base_url, ticker)
            except Exception as e:
                st.error(f"❌ Error: {e}")
        
        st.divider()
        
        st.subheader("Option 2: Download ALL Companies")
        st.warning("⚠️ **NOT RECOMMENDED**: Downloading all 10 companies at once may exceed the SEC EDGAR rate limit (0.1 - 10 requests/second). Use Option 1 for individual tickers instead.")
        
        with st.form("collect_all_form"):
            c1, c2 = st.columns(2)
            with c1:
                all_filing_types = st.multiselect("Filing Types", ["10-K", "10-Q", "8-K", "DEF 14A"], 
                    default=["10-K", "10-Q", "8-K", "DEF 14A"], key="all_filing_types")
            with c2:
                all_years_back = st.slider("Years Back", 1, 10, 3, key="all_years_back")
            submitted_all = st.form_submit_button("📥 Download ALL Companies (Not Recommended)", use_container_width=True)
        
        if submitted_all:
            try:
                with st.spinner("Collecting filings for ALL companies... This may take several minutes..."):
                    query_str = "&".join([f"filing_types={ft}" for ft in all_filing_types])
                    full_url = f"{api_url(base_url, '/api/v1/documents/collect/all')}?{query_str}&years_back={all_years_back}"
                    resp = requests.post(full_url, timeout=600)
                    data = safe_json(resp)
                
                if resp.status_code < 400:
                    st.success("✅ Collection Complete for ALL Companies!")
                    if isinstance(data, list):
                        for company_data in data:
                            st.markdown(f"**{company_data.get('ticker', 'Unknown')}**: {company_data.get('documents_uploaded', 0)} uploaded")
                    show_json("Raw JSON Response", data)
                    st.divider()
                    fetch_documents_table(base_url)
                else:
                    st.error(f"❌ Error: {data.get('detail', data)}")
            except Exception as e:
                st.error(f"❌ Error: {e}")
    
    # Page 2: Parsing
    elif sub_page == "2. Parsing":
        st.header("Step 2: Parse Documents")
        st.info("📄 Downloads from S3 → Extracts text/tables → Identifies sections (Items 1, 1A, 7) → Updates Snowflake")
        
        st.subheader("Option 1: Parse by Ticker")
        c1, c2 = st.columns([2, 1])
        with c1:
            ticker = st.text_input("Ticker", value=st.session_state["last_ticker"]).upper().strip()
        
        if st.button("📄 Parse Documents", type="primary", use_container_width=True):
            if ticker:
                st.session_state["last_ticker"] = ticker
                try:
                    with st.spinner(f"Parsing documents for {ticker}..."):
                        data = post(api_url(base_url, f"/api/v1/documents/parse/{ticker}"))
                    st.success("✅ Parsing Complete!")
                    
                    render_kpis([("Total", data.get("total_documents", 0)), ("Parsed", data.get("parsed", 0)),
                                 ("Skipped", data.get("skipped", 0)), ("Failed", data.get("failed", 0))])
                    
                    results = data.get("results", [])
                    if results:
                        st.subheader("Parsed Documents Summary")
                        df = pd.DataFrame([{
                            "Document ID": r.get("document_id", "")[:20] + "...",
                            "Filing Type": r.get("filing_type"),
                            "Filing Date": r.get("filing_date"),
                            "Format": r.get("source_format"),
                            "Words": r.get("word_count", 0),
                            "Tables": r.get("table_count", 0),
                            "Sections": ", ".join(r.get("sections_found", [])[:3])
                        } for r in results])
                        st.dataframe(df, use_container_width=True, hide_index=True)
                        
                        st.divider()
                        st.subheader("📊 RAW vs PARSED Content Comparison (10-K Document)")
                        st.caption("Showing 10-K document which contains Items 1, 1A, and 7")
                        
                        doc_10k = None
                        for r in results:
                            if r.get("filing_type") == "10-K":
                                doc_10k = r
                                break
                        
                        if not doc_10k:
                            doc_10k = results[0]
                            st.warning(f"⚠️ No 10-K document found. Showing {doc_10k.get('filing_type')} instead.")
                        
                        doc_id = doc_10k.get("document_id")
                        if doc_id:
                            try:
                                parsed_content = get(api_url(base_url, f"/api/v1/documents/parsed/{doc_id}"))
                                
                                st.markdown(f"**Document: {doc_10k.get('filing_type')} - {doc_10k.get('filing_date')}** | Words: {parsed_content.get('word_count', 0):,} | Tables: {parsed_content.get('table_count', 0)}")
                                
                                col1, col2 = st.columns(2)
                                
                                with col1:
                                    st.markdown("##### 📁 RAW Content")
                                    st.markdown(f"**S3 Key:** `{parsed_content.get('s3_key', 'N/A')}`")
                                    
                                    raw_text = parsed_content.get('text_preview', '')
                                    
                                    if raw_text:
                                        st.markdown("**Raw Text (showing key sections if found):**")
                                        
                                        section_previews = []
                                        
                                        item1_match = re.search(r'(item\s*1[.\s]+business.{0,500})', raw_text, re.IGNORECASE | re.DOTALL)
                                        if item1_match:
                                            section_previews.append(("Item 1 - Business", item1_match.group(1)[:400]))
                                        
                                        item1a_match = re.search(r'(item\s*1a[.\s]+risk\s*factors.{0,500})', raw_text, re.IGNORECASE | re.DOTALL)
                                        if item1a_match:
                                            section_previews.append(("Item 1A - Risk Factors", item1a_match.group(1)[:400]))
                                        
                                        item7_match = re.search(r'(item\s*7[.\s]+management.{0,500})', raw_text, re.IGNORECASE | re.DOTALL)
                                        if item7_match:
                                            section_previews.append(("Item 7 - MD&A", item7_match.group(1)[:400]))
                                        
                                        if section_previews:
                                            for section_name, preview in section_previews:
                                                with st.expander(f"📄 {section_name}", expanded=True):
                                                    st.text(preview + "...")
                                        else:
                                            st.text_area("Raw Text Content", value=raw_text[:2500], height=300, disabled=True, key="raw_text_parse")
                                    
                                    tables = parsed_content.get('tables', [])
                                    if tables:
                                        with st.expander(f"📋 Raw HTML Table Structure ({len(tables)} tables)"):
                                            html_preview = ""
                                            for idx, table in enumerate(tables[:2]):
                                                if isinstance(table, dict):
                                                    headers = table.get('headers', [])
                                                    rows = table.get('rows', [])
                                                    html_preview += f"<table id='table_{idx+1}'>\n  <thead><tr>\n"
                                                    for h in headers[:5]:
                                                        html_preview += f"    <th>{str(h)[:25]}</th>\n"
                                                    html_preview += "  </tr></thead>\n  <tbody>\n"
                                                    for row in rows[:3]:
                                                        html_preview += "    <tr>"
                                                        for cell in row[:5]:
                                                            html_preview += f"<td>{str(cell)[:20]}</td>"
                                                        html_preview += "</tr>\n"
                                                    html_preview += "  </tbody>\n</table>\n\n"
                                            st.code(html_preview, language="html")
                                
                                with col2:
                                    st.markdown("##### 📄 PARSED Content")
                                    
                                    sections_list = parsed_content.get('sections', [])
                                    st.markdown(f"**Sections Found:** {', '.join(sections_list) if sections_list else 'None'}")
                                    
                                    st.markdown("---")
                                    st.markdown("**📑 Key SEC Filing Sections (10-K only):**")
                                    
                                    s3_key = parsed_content.get('s3_key', '')
                                    full_text, loaded_path = load_json_file_content(
                                        s3_key, 
                                        ticker, 
                                        doc_10k.get('filing_type', '10-K'),
                                        doc_10k.get('filing_date', '')
                                    )
                                    
                                    if loaded_path:
                                        st.success(f"📂 Loaded from: `{loaded_path}`")
                                        st.caption(f"📄 Total text length: {len(full_text):,} characters")
                                    else:
                                        st.warning(f"⚠️ Could not load JSON file from S3. Tried key: `{s3_key}`")
                                        full_text = parsed_content.get('text_preview', '') or parsed_content.get('text', '') or ''
                                        if full_text:
                                            st.info(f"Using text_preview from API ({len(full_text):,} chars)")
                                    
                                    st.markdown("**✅ Item 1 - Business (lines containing 'Item 1. Business'):**")
                                    item1_lines = extract_lines_containing(full_text, "Item 1. Business", num_lines=3, chars_per_line=400)
                                    if not item1_lines:
                                        item1_lines = extract_lines_containing(full_text, "Item 1 Business", num_lines=3, chars_per_line=400)
                                    if item1_lines:
                                        st.text_area("Item 1 Content", value=item1_lines, height=180, disabled=True, key="item1_content")
                                    else:
                                        st.info("No lines found containing 'Item 1. Business'")
                                    
                                    st.markdown("**✅ Item 1A - Risk Factors (lines containing 'Item 1A'):**")
                                    item1a_lines = extract_lines_containing(full_text, "Item 1A", num_lines=3, chars_per_line=400)
                                    if item1a_lines:
                                        st.text_area("Item 1A Content", value=item1a_lines, height=180, disabled=True, key="item1a_content")
                                    else:
                                        st.info("No lines found containing 'Item 1A'")
                                    
                                    st.markdown("**✅ Item 7 - MD&A (lines containing 'Item 7'):**")
                                    item7_lines = extract_lines_containing(full_text, "Item 7", num_lines=3, chars_per_line=400)
                                    if item7_lines:
                                        st.text_area("Item 7 Content", value=item7_lines, height=180, disabled=True, key="item7_content")
                                    else:
                                        st.info("No lines found containing 'Item 7'")
                                    
                                    tables = parsed_content.get('tables', [])
                                    st.markdown("---")
                                    st.markdown(f"**📊 Parsed Tables ({len(tables)} total):**")
                                    
                                    if tables:
                                        shown = 0
                                        for idx, table in enumerate(tables):
                                            if shown >= 3:
                                                break
                                            if isinstance(table, dict):
                                                headers = table.get('headers', [])
                                                rows = table.get('rows', [])
                                                if rows and len(rows) >= 2 and headers:
                                                    st.markdown(f"**Table {idx + 1}:**")
                                                    num_cols = len(headers)
                                                    norm_rows = []
                                                    for row in rows[:8]:
                                                        if isinstance(row, list):
                                                            norm_row = row[:num_cols] + [''] * (num_cols - len(row))
                                                            norm_rows.append(norm_row[:num_cols])
                                                    if norm_rows:
                                                        st.dataframe(pd.DataFrame(norm_rows, columns=headers[:num_cols]), use_container_width=True, hide_index=True, height=150)
                                                        shown += 1
                                        
                                        if len(tables) > 3:
                                            st.caption(f"... and {len(tables) - 3} more tables")
                                    else:
                                        st.info("No tables extracted")
                                
                            except Exception as e:
                                st.warning(f"Could not fetch parsed content: {e}")
                    
                    show_json("Raw JSON Response", data)
                except Exception as e:
                    st.error(f"❌ Error: {e}")
        
        st.divider()
        
        st.subheader("Option 2: Parse ALL Companies")
        st.warning("⚠️ **NOT RECOMMENDED**: Parsing all companies at once may exceed the rate limit (0.1 - 10 requests/second). Use Option 1 instead.")
        
        if st.button("📄 Parse ALL Companies (Not Recommended)", use_container_width=True):
            try:
                with st.spinner("Parsing documents for ALL companies..."):
                    data = post(api_url(base_url, "/api/v1/documents/parse"), timeout_s=600)
                st.success("✅ Parsing Complete for ALL Companies!")
                render_kpis([("Total Parsed", data.get("total_parsed", 0)), ("Skipped", data.get("total_skipped", 0)), ("Failed", data.get("total_failed", 0))])
                by_company = data.get("by_company", [])
                if by_company:
                    st.dataframe(pd.DataFrame(by_company), use_container_width=True, hide_index=True)
                show_json("Raw JSON Response", data)
            except Exception as e:
                st.error(f"❌ Error: {e}")
    
    # Page 3: PDF Parsing
    elif sub_page == "3. PDF Parsing":
        st.header("Step 3: PDF Parsing (Sample)")
        st.info("📄 Parse a sample 10-K PDF file from `data/sample_10k/` folder")
        
        ticker = st.text_input("Ticker Symbol", value="AAPL", help="Company ticker symbol for the PDF")
        st.caption("**Note**: Place PDF in `data/sample_10k/` folder")
        
        if st.button("📄 Parse PDF", type="primary", use_container_width=True):
            try:
                with st.spinner("Parsing PDF..."):
                    resp = requests.get(api_url(base_url, "/api/v1/sec/parse-pdf"), params={"ticker": ticker, "upload_to_s3": False}, timeout=300)
                    data = safe_json(resp)
                
                if resp.status_code < 400:
                    st.success(f"✅ {data.get('message', 'PDF Parsing Complete!')}")
                    render_kpis([("Pages", data.get('page_count', 0)), ("Words", f"{data.get('word_count', 0):,}"), ("Tables", data.get('table_count', 0))])
                    st.markdown(f"**File**: `{data.get('pdf_file', 'N/A')}` | **Hash**: `{data.get('content_hash', 'N/A')}`")
                    show_json("Raw JSON Response", data)
                else:
                    st.error(f"❌ Error: {data.get('detail', data)}")
            except Exception as e:
                st.error(f"❌ Error: {e}")
    
    # Page 4: De-duplication
    elif sub_page == "4. De-duplication":
        st.header("Step 4: De-duplication")
        st.info("De-duplication happens automatically during collection via content hash checking")
        
        st.markdown("""
        ### How De-duplication Works
        1. **Content Hash**: Each document's content is hashed using SHA-256
        2. **Duplicate Check**: Before uploading, the hash is compared against existing records
        3. **Skip Duplicates**: Documents with matching hashes are skipped
        """)
        
        ticker = st.text_input("Ticker to Check", value=st.session_state["last_ticker"]).upper().strip()
        
        if st.button("🔍 Check Documents", use_container_width=True):
            if ticker:
                fetch_documents_table(base_url, ticker)
    
    # Page 5: Chunking
    elif sub_page == "5. Chunking":
        st.header("Step 5: Chunk Documents")
        st.info("📦 Splits parsed documents into overlapping chunks for LLM processing")
        
        st.subheader("Option 1: Chunk by Ticker")
        with st.form("chunk_form"):
            c1, c2, c3 = st.columns(3)
            with c1:
                ticker = st.text_input("Ticker", value=st.session_state["last_ticker"]).upper().strip()
            with c2:
                chunk_size = st.number_input("Chunk Size (words)", 100, 2000, 750, 50)
            with c3:
                chunk_overlap = st.number_input("Overlap (words)", 0, 200, 50, 10)
            submitted = st.form_submit_button("📦 Chunk Documents", type="primary", use_container_width=True)
        
        if submitted and ticker:
            st.session_state["last_ticker"] = ticker
            try:
                with st.spinner(f"Chunking documents for {ticker}..."):
                    data = post(api_url(base_url, f"/api/v1/documents/chunk/{ticker}"), params={"chunk_size": chunk_size, "chunk_overlap": chunk_overlap})
                st.success("✅ Chunking Complete!")
                render_kpis([("Documents", data.get("total_documents", 0)), ("Chunked", data.get("chunked", 0)), ("Total Chunks", data.get("total_chunks", 0)), ("Failed", data.get("failed", 0))])
                show_json("Raw JSON Response", data)
                st.divider()
                fetch_chunks_table(base_url, ticker)
            except Exception as e:
                st.error(f"❌ Error: {e}")
        
        st.divider()
        
        st.subheader("Option 2: Chunk ALL Companies")
        st.warning("⚠️ **NOT RECOMMENDED**: Chunking all companies at once may exceed the rate limit (0.1 - 10 requests/second).")
        
        with st.form("chunk_all_form"):
            c1, c2 = st.columns(2)
            with c1:
                all_chunk_size = st.number_input("Chunk Size", 100, 2000, 750, 50, key="all_cs")
            with c2:
                all_chunk_overlap = st.number_input("Overlap", 0, 200, 50, 10, key="all_co")
            submitted_all = st.form_submit_button("📦 Chunk ALL (Not Recommended)", use_container_width=True)
        
        if submitted_all:
            try:
                with st.spinner("Chunking ALL companies..."):
                    data = post(api_url(base_url, "/api/v1/documents/chunk"), params={"chunk_size": all_chunk_size, "chunk_overlap": all_chunk_overlap}, timeout_s=600)
                st.success("✅ Chunking Complete!")
                render_kpis([("Documents", data.get("total_documents_chunked", 0)), ("Chunks", data.get("total_chunks_created", 0))])
                show_json("Raw JSON Response", data)
                fetch_chunks_table(base_url)
            except Exception as e:
                st.error(f"❌ Error: {e}")
    
    # Reports Page
    elif sub_page == "📊 Reports":
        st.header("📊 Evidence Collection Report")
        
        if st.button("🔄 Load Report", type="primary", use_container_width=True):
            try:
                with st.spinner("Loading report..."):
                    data = get(api_url(base_url, "/api/v1/documents/report/table"))
                st.success("✅ Report Loaded!")
                
                summary = data.get("summary_table", {})
                if summary.get("rows"):
                    kpis = [(str(r[0]), r[1]) for r in summary["rows"][:5] if len(r) >= 2]
                    if kpis:
                        render_kpis(kpis)
                    st.subheader("Summary Statistics")
                    st.table(df_from_table(summary.get("headers", []), summary.get("rows", [])))
                
                company = data.get("company_table", {})
                if company.get("rows"):
                    st.subheader("Documents by Company")
                    st.dataframe(df_from_table(company.get("headers", []), company.get("rows", [])), use_container_width=True, hide_index=True)
                
                show_json("Raw JSON Response", data)
            except Exception as e:
                st.error(f"❌ Error: {e}")
    
    # NEW PAGE: SEC Analysis Summary
    elif sub_page == "📈 SEC Analysis Summary":
        st.header("📈 SEC Analysis Summary")
        st.info("📊 Export and view section analysis for all companies with word counts and keyword mentions")
        
        # Define possible local file paths for sec_analysis_summary.md
        LOCAL_ANALYSIS_PATHS = [
            "sec_analysis_summary.md",
            "./sec_analysis_summary.md",
            "../sec_analysis_summary.md",
            "../../sec_analysis_summary.md",
            "../../../sec_analysis_summary.md",
            "sec_analysis.md",
            "./sec_analysis.md",
            "../sec_analysis.md",
            "../../sec_analysis.md",
            "data/sec_analysis_summary.md",
            "output/sec_analysis_summary.md",
        ]
        
        def load_local_analysis_file() -> Optional[str]:
            """Try to load sec_analysis.md from local filesystem."""
            for path in LOCAL_ANALYSIS_PATHS:
                if os.path.exists(path):
                    try:
                        with open(path, 'r', encoding='utf-8') as f:
                            content = f.read()
                            if content.strip():
                                return content, path
                    except Exception as e:
                        continue
            return None, None
        
        # Try to load from local file first (on page load)
        if not st.session_state.get("analysis_markdown"):
            local_content, local_path = load_local_analysis_file()
            if local_content:
                st.session_state["analysis_markdown"] = local_content
                st.session_state["analysis_source"] = f"Local file: {local_path}"
        
        col1, col2, col3 = st.columns([1, 1, 1])
        
        with col1:
            if st.button("📂 Load from Local File", type="primary", use_container_width=True):
                local_content, local_path = load_local_analysis_file()
                if local_content:
                    st.session_state["analysis_markdown"] = local_content
                    st.session_state["analysis_source"] = f"Local file: {local_path}"
                    st.success(f"✅ Loaded from: `{local_path}`")
                else:
                    st.warning(f"⚠️ No local file found. Searched in: {', '.join(LOCAL_ANALYSIS_PATHS)}")
        
        with col2:
            if st.button("🔄 Refresh from API", type="secondary", use_container_width=True):
                try:
                    with st.spinner("Fetching SEC Analysis from API (timeout: 10 minutes)..."):
                        # Call the export endpoint with 10 minute timeout
                        resp = requests.get(
                            api_url(base_url, "/api/v1/documents/analysis/export"),
                            timeout=600  # 10 minutes timeout
                        )
                        
                        if resp.status_code < 400:
                            markdown_content = resp.text
                            st.session_state["analysis_markdown"] = markdown_content
                            st.session_state["analysis_source"] = "API: /api/v1/documents/analysis/export"
                            
                            # Save to local file for future use
                            try:
                                with open("sec_analysis_summary.md", 'w', encoding='utf-8') as f:
                                    f.write(markdown_content)
                                st.success("✅ Analysis loaded from API and saved to `sec_analysis_summary.md`")
                            except:
                                st.success("✅ Analysis loaded from API!")
                        else:
                            st.error(f"❌ Error: {resp.status_code} - {resp.text[:200]}")
                except requests.exceptions.Timeout:
                    st.error("❌ Request timed out after 10 minutes. The analysis may take too long to generate.")
                except Exception as e:
                    st.error(f"❌ Error fetching analysis: {e}")
        
        with col3:
            if st.session_state.get("analysis_markdown"):
                st.download_button(
                    label="💾 Save as Markdown",
                    data=st.session_state["analysis_markdown"],
                    file_name="sec_analysis.md",
                    mime="text/markdown",
                    use_container_width=True
                )
        
        # Show source info
        if st.session_state.get("analysis_source"):
            st.caption(f"📍 Source: {st.session_state['analysis_source']}")
        
        st.divider()
        
        # Display the analysis if loaded
        if st.session_state.get("analysis_markdown"):
            markdown_content = st.session_state["analysis_markdown"]
            
            # Parse all elements (headings, text, tables)
            elements = parse_markdown_content(markdown_content)
            
            if elements:
                for element in elements:
                    elem_type = element.get('type')
                    level = element.get('level', 0)
                    title = element.get('title', '')
                    
                    # Display headings
                    if elem_type == 'heading':
                        if level == 1:
                            st.title(f"📊 {title}")
                        elif level == 2:
                            st.header(f"📈 {title}")
                        elif level == 3:
                            st.subheader(f"📋 {title}")
                        elif level == 4:
                            st.markdown(f"**{title}**")
                    
                    # Display text content
                    elif elem_type == 'text':
                        content = element.get('content', '')
                        if content:
                            st.markdown(content)
                    
                    # Display tables
                    elif elem_type == 'table':
                        headers = element.get('headers', [])
                        rows = element.get('rows', [])
                        
                        if headers and rows:
                            # Normalize rows
                            normalized_rows = []
                            for row in rows:
                                if len(row) < len(headers):
                                    row = row + [''] * (len(headers) - len(row))
                                elif len(row) > len(headers):
                                    row = row[:len(headers)]
                                normalized_rows.append(row)
                            
                            df = pd.DataFrame(normalized_rows, columns=headers)
                            st.dataframe(df, use_container_width=True, hide_index=True)
                        
                        st.markdown("")  # Add some spacing after tables
            else:
                # Fallback: Just render the markdown directly
                st.markdown("### Raw Markdown Content")
                st.markdown(markdown_content)
            
            # Also show raw markdown in expander
            with st.expander("📄 View Raw Markdown", expanded=False):
                st.code(markdown_content, language="markdown")
        else:
            st.warning("⚠️ No analysis data loaded. Click 'Load from Local File' or 'Refresh from API' above.")
            
            # Show what the analysis includes
            st.markdown("""
            ### What's included in the analysis:
            
            - **Section Word Counts** - Word counts for each section (Business, Risk Factors, MD&A, etc.) by company
            - **Keyword Mentions** - Frequency of key terms like "risk", "growth", "competition", etc.
            - **Filing Coverage** - Overview of 10-K, 10-Q, 8-K, and DEF 14A filings per company
            - **Comparative Analysis** - Side-by-side comparison across all 10 companies
            
            ---
            
            ### Local File Locations Searched:
            """)
            for path in LOCAL_ANALYSIS_PATHS:
                exists = "✅" if os.path.exists(path) else "❌"
                st.caption(f"{exists} `{path}`")

# Signals Section
else:
    st.title("📈 Signals")
    
    if sub_page == "Leadership Score":
        st.header("Leadership Score Signal")
        
        ticker = st.text_input("Ticker Symbol", value=st.session_state["last_ticker"]).upper().strip()
        
        if st.button("🎯 Get Leadership Score", type="primary", use_container_width=True):
            if ticker:
                st.session_state["last_ticker"] = ticker
                try:
                    with st.spinner(f"Fetching leadership score for {ticker}..."):
                        data = get(api_url(base_url, f"/api/v1/signals/leadership/{ticker}"))
                    st.success("✅ Signal Retrieved!")
                    
                    if isinstance(data, dict):
                        score = data.get("score") or data.get("leadership_score")
                        if score is not None:
                            st.metric("Leadership Score", f"{score:.2f}" if isinstance(score, (int, float)) else score)
                        st.dataframe(pd.DataFrame([{"Field": k, "Value": str(v)[:100]} for k, v in data.items()]), use_container_width=True, hide_index=True)
                    
                    show_json("Raw JSON Response", data)
                except Exception as e:
                    st.error(f"❌ Error: {e}")

st.sidebar.divider()
st.sidebar.caption("SEC Filings & Signals Pipeline v1.0")
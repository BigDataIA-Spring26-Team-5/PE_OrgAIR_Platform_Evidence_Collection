from __future__ import annotations

import os
import uuid
from datetime import datetime, timezone
from typing import List, Optional

import snowflake.connector
from dotenv import load_dotenv

from app.pipelines.chunking import DocumentChunk



# MODULE-LEVEL CONNECTION (USED BY REPOSITORIES)


def get_snowflake_connection():
    """
    Backward-compatible Snowflake connection factory.
    Used by repositories via dependency injection.
    """
    # Load environment variables from .env file
    load_dotenv()
    
    return snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA"),
        role=os.getenv("SNOWFLAKE_ROLE"),
    )



# SERVICE CLASS (USED BY PIPELINES)


class SnowflakeService:
    """
    Snowflake service for SEC pipeline operations.
    
    Tables used:
    - companies: Read company_id validation
    - documents: Store document metadata
    - document_chunks: Store document chunks
    """
    
    def __init__(self):
        self.conn = get_snowflake_connection()

    def close(self):
        """Close the Snowflake connection."""
        try:
            self.conn.close()
        except Exception:
            pass

    # -------------------------
    # Company operations
    # -------------------------
    def list_companies(self) -> list[dict]:
        """
        List all active companies with tickers.
        Used to validate company_id in download step.
        """
        sql = """
            SELECT id, ticker
            FROM companies
            WHERE is_deleted = FALSE
            AND ticker IS NOT NULL
        """
        cur = self.conn.cursor(snowflake.connector.DictCursor)
        try:
            cur.execute(sql)
            return cur.fetchall()
        finally:
            cur.close()

    # -------------------------
    # Document operations
    # -------------------------
    def document_exists_by_hash(self, content_hash: str) -> bool:
        """
        Check if document with given content_hash already exists.
        Used for deduplication in step 3.
        """
        sql = "SELECT 1 FROM documents WHERE content_hash = %s LIMIT 1"
        cur = self.conn.cursor()
        try:
            cur.execute(sql, (content_hash,))
            return cur.fetchone() is not None
        finally:
            cur.close()

    def insert_document(
        self,
        company_id: str,
        ticker: str,
        filing_type: str,
        filing_date: datetime,
        local_path: str,
        content_hash: str,
        word_count: int,
        status: str,
        s3_key: Optional[str] = None,
        error_message: Optional[str] = None,
        chunk_count: Optional[int] = None,
        source_url: Optional[str] = None,
        processed_at: Optional[datetime] = None,
        doc_id: Optional[str] = None,
    ) -> str:
        """
        Insert or update a document in the documents table.
        
        Table: documents
        Columns:
        - id: VARCHAR(36) PRIMARY KEY
        - company_id: VARCHAR(36) NOT NULL REFERENCES companies(id)
        - ticker: VARCHAR(10) NOT NULL
        - filing_type: VARCHAR(20) NOT NULL
        - filing_date: DATE NOT NULL
        - source_url: VARCHAR(500)
        - local_path: VARCHAR(500)
        - s3_key: VARCHAR(500)
        - content_hash: VARCHAR(64)
        - word_count: INT
        - chunk_count: INT
        - status: VARCHAR(20) DEFAULT 'pending'
        - error_message: VARCHAR(1000)
        - created_at: TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
        - processed_at: TIMESTAMP_NTZ
        
        Valid status values: pending, downloaded, parsed, chunked, indexed, failed
        """
        doc_id = doc_id or str(uuid.uuid4())
        processed_at = processed_at or datetime.now(timezone.utc)

        sql = """
        MERGE INTO documents t
        USING (SELECT %s AS id) s
        ON t.id = s.id
        WHEN NOT MATCHED THEN INSERT (
            id, company_id, ticker, filing_type, filing_date,
            source_url, local_path, s3_key, content_hash,
            word_count, chunk_count, status, error_message,
            created_at, processed_at
        ) VALUES (
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s,
            %s, %s, %s, %s,
            CURRENT_TIMESTAMP(), %s
        )
        WHEN MATCHED THEN UPDATE SET
            status = %s,
            error_message = %s,
            word_count = COALESCE(%s, t.word_count),
            chunk_count = COALESCE(%s, t.chunk_count),
            local_path = COALESCE(%s, t.local_path),
            s3_key = COALESCE(%s, t.s3_key),
            processed_at = %s;
        """

        params = (
            doc_id,
            # INSERT values
            doc_id, company_id, ticker, filing_type, filing_date.date(),
            source_url, local_path, s3_key, content_hash,
            word_count, chunk_count, status, error_message,
            processed_at,
            # UPDATE values
            status, error_message, word_count, chunk_count,
            local_path, s3_key, processed_at
        )

        cur = self.conn.cursor()
        try:
            cur.execute(sql, params)
            self.conn.commit()
            return doc_id
        finally:
            cur.close()

    # -------------------------
    # Chunk operations
    # -------------------------
    def insert_chunks(self, chunks: List[DocumentChunk]) -> int:
        """
        Insert chunks into document_chunks table.
        
        Table: document_chunks
        Columns:
        - id: VARCHAR(36) PRIMARY KEY
        - document_id: VARCHAR(36) NOT NULL REFERENCES documents(id)
        - chunk_index: INT NOT NULL
        - content: TEXT NOT NULL
        - section: VARCHAR(50)
        - start_char: INT
        - end_char: INT
        - word_count: INT
        - created_at: TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
        - UNIQUE (document_id, chunk_index)
        """
        if not chunks:
            return 0

        sql = """
        INSERT INTO document_chunks (
            id, document_id, chunk_index, content, section,
            start_char, end_char, word_count, created_at
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP())
        """

        rows = [
            (
                str(uuid.uuid4()),  # Generate UUID for chunk id
                c.document_id,
                c.chunk_index,
                c.content,
                c.section,
                c.start_char,
                c.end_char,
                c.word_count,
            )
            for c in chunks
        ]

        cur = self.conn.cursor()
        try:
            cur.executemany(sql, rows)
            self.conn.commit()
            return len(rows)
        finally:
            cur.close()

    # -------------------------
    # Read APIs
    # -------------------------
    def list_documents(self, ticker: Optional[str] = None) -> list[dict]:
        """List all documents, optionally filtered by ticker."""
        sql = """
        SELECT id, company_id, ticker, filing_type, filing_date,
               source_url, local_path, s3_key, content_hash, 
               word_count, chunk_count, status, error_message,
               created_at, processed_at
        FROM documents
        """
        params = []

        if ticker:
            sql += " WHERE ticker = %s"
            params.append(ticker)
        
        sql += " ORDER BY filing_date DESC"

        cur = self.conn.cursor(snowflake.connector.DictCursor)
        try:
            cur.execute(sql, params if params else None)
            return cur.fetchall()
        finally:
            cur.close()

    def get_document(self, doc_id: str) -> Optional[dict]:
        """Get a single document by ID."""
        sql = "SELECT * FROM documents WHERE id = %s"
        cur = self.conn.cursor(snowflake.connector.DictCursor)
        try:
            cur.execute(sql, (doc_id,))
            return cur.fetchone()
        finally:
            cur.close()

    def get_document_chunks(self, doc_id: str) -> list[dict]:
        """Get all chunks for a document, ordered by chunk_index."""
        sql = """
        SELECT id, document_id, chunk_index, section, 
               start_char, end_char, word_count, content,
               created_at
        FROM document_chunks
        WHERE document_id = %s
        ORDER BY chunk_index
        """
        cur = self.conn.cursor(snowflake.connector.DictCursor)
        try:
            cur.execute(sql, (doc_id,))
            return cur.fetchall()
        finally:
            cur.close()

    def get_pipeline_summary(self, ticker: Optional[str] = None) -> dict:
        """
        Get summary statistics for the pipeline.
        Useful for the /stats endpoint.
        """
        where_clause = "WHERE ticker = %s" if ticker else ""
        params = [ticker] if ticker else []
        
        sql = f"""
        SELECT 
            COUNT(DISTINCT d.id) as total_documents,
            SUM(d.chunk_count) as total_chunks,
            COUNT(DISTINCT d.ticker) as unique_tickers,
            COUNT(CASE WHEN d.status = 'chunked' THEN 1 END) as chunked_documents,
            COUNT(CASE WHEN d.status = 'failed' THEN 1 END) as failed_documents
        FROM documents d
        {where_clause}
        """
        
        cur = self.conn.cursor(snowflake.connector.DictCursor)
        try:
            cur.execute(sql, params if params else None)
            return cur.fetchone()
        finally:
            cur.close()
    # -------------------------
    # External Signal operations (Pipeline 2: Jobs, Patents)
    # -------------------------

    def insert_external_signal(
        self,
        signal_id: str,
        company_id: str,
        category: str,
        source: str,
        score: float,
        evidence_count: int,
        summary: str,
        raw_payload: dict,
    ) -> str:
        """
        Insert or update an external signal (job market, patents, etc.).

        Args:
            signal_id: Unique identifier for the signal
            company_id: Reference to companies table
            category: Signal category (job_market, innovation_activity, etc.)
            source: Data source (linkedin, indeed, uspto, etc.)
            score: Computed score (0-100)
            evidence_count: Number of evidence items (e.g., AI jobs found)
            summary: Human-readable summary
            raw_payload: Full data as JSON (stored as VARIANT)

        Returns:
            signal_id
        """
        import json

        sql = """
        MERGE INTO external_signals t
        USING (SELECT %s AS id) s
        ON t.id = s.id
        WHEN NOT MATCHED THEN INSERT (
            id, company_id, category, source, score,
            evidence_count, summary, raw_payload, created_at
        ) VALUES (
            %s, %s, %s, %s, %s,
            %s, %s, PARSE_JSON(%s), CURRENT_TIMESTAMP()
        )
        WHEN MATCHED THEN UPDATE SET
            score = %s,
            evidence_count = %s,
            summary = %s,
            raw_payload = PARSE_JSON(%s),
            created_at = CURRENT_TIMESTAMP()
        """

        payload_json = json.dumps(raw_payload, default=str)
        params = (
            signal_id,
            signal_id, company_id, category, source, score,
            evidence_count, summary, payload_json,
            score, evidence_count, summary, payload_json,
        )

        cur = self.conn.cursor()
        try:
            cur.execute(sql, params)
            self.conn.commit()
            return signal_id
        finally:
            cur.close()

    def get_external_signals(
        self,
        company_id: Optional[str] = None,
        category: Optional[str] = None,
    ) -> list[dict]:
        """
        Retrieve external signals, optionally filtered by company or category.

        Args:
            company_id: Filter by company (optional)
            category: Filter by category like 'job_market', 'innovation_activity' (optional)

        Returns:
            List of signal records
        """
        sql = "SELECT * FROM external_signals WHERE 1=1"
        params = []

        if company_id:
            sql += " AND company_id = %s"
            params.append(company_id)
        if category:
            sql += " AND category = %s"
            params.append(category)

        sql += " ORDER BY created_at DESC"

        cur = self.conn.cursor(snowflake.connector.DictCursor)
        try:
            cur.execute(sql, params if params else None)
            return cur.fetchall()
        finally:
            cur.close()

    def get_pipeline_summary(self, ticker: Optional[str] = None) -> dict:
        """
        Get summary statistics for the pipeline.
        Useful for the /stats endpoint.
        """
        where_clause = "WHERE ticker = %s" if ticker else ""
        params = [ticker] if ticker else []
        
        sql = f"""
        SELECT 
            COUNT(DISTINCT d.id) as total_documents,
            SUM(d.chunk_count) as total_chunks,
            COUNT(DISTINCT d.ticker) as unique_tickers,
            COUNT(CASE WHEN d.status = 'chunked' THEN 1 END) as chunked_documents,
            COUNT(CASE WHEN d.status = 'failed' THEN 1 END) as failed_documents
        FROM documents d
        {where_clause}
        """
        
        cur = self.conn.cursor(snowflake.connector.DictCursor)
        try:
            cur.execute(sql, params if params else None)
            return cur.fetchone()
        finally:
            cur.close()
# import snowflake.connector
# from app.config import settings


# def get_snowflake_connection():
#     """
#     Returns a Snowflake connection.
#     Used by services or repositories when database access is required.
#     """
#     return snowflake.connector.connect(
#     user=settings.SNOWFLAKE_USER,
#     password=settings.SNOWFLAKE_PASSWORD.get_secret_value(),
#     account=settings.SNOWFLAKE_ACCOUNT,
#     warehouse=settings.SNOWFLAKE_WAREHOUSE,
#     database=settings.SNOWFLAKE_DATABASE,
#     schema=settings.SNOWFLAKE_SCHEMA,
#     role=settings.SNOWFLAKE_ROLE,

#     )


from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import List, Optional

import snowflake.connector
from dotenv import load_dotenv

from app.pipelines.chunking import DocumentChunk


# ============================================================
# ✅ MODULE-LEVEL CONNECTION (USED BY REPOSITORIES)
# ============================================================

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


# ============================================================
# ✅ SERVICE CLASS (USED BY PIPELINES)
# ============================================================

class SnowflakeService:
    def __init__(self):
        self.conn = get_snowflake_connection()

    def close(self):
        try:
            self.conn.close()
        except Exception:
            pass

    # -------------------------
    # Document operations
    # -------------------------
    # def list_companies(self) -> list[dict]:
    #     sql = "SELECT id, ticker FROM companies WHERE is_deleted = FALSE"
    #     cur = self.conn.cursor(snowflake.connector.DictCursor)
    #     try:
    #         cur.execute(sql)
    #         return cur.fetchall()
    #     finally:
    #          cur.close()
    def list_companies(self) -> list[dict]:
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
            
    def document_exists_by_hash(self, content_hash: str) -> bool:
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
        s3_key: str,
        content_hash: str,
        word_count: int,
        status: str,
        error_message: Optional[str] = None,
        chunk_count: Optional[int] = None,
        source_url: Optional[str] = None,
        processed_at: Optional[datetime] = None,
        doc_id: Optional[str] = None,
    ) -> str:
        doc_id = doc_id or content_hash
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
            doc_id, company_id, ticker, filing_type, filing_date.date(),
            source_url, local_path, s3_key, content_hash,
            word_count, chunk_count, status, error_message,
            processed_at,
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
                f"{c.document_id}:{c.chunk_index}",
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
        sql = """
        SELECT id, company_id, ticker, filing_type, filing_date,
               s3_key, content_hash, word_count, chunk_count,
               status, created_at, processed_at
        FROM documents
        """
        params = []

        if ticker:
            sql += " WHERE ticker = %s"
            params.append(ticker)

        cur = self.conn.cursor(snowflake.connector.DictCursor)
        try:
            cur.execute(sql, params if params else None)
            return cur.fetchall()
        finally:
            cur.close()

    def get_document(self, doc_id: str) -> Optional[dict]:
        sql = "SELECT * FROM documents WHERE id = %s"
        cur = self.conn.cursor(snowflake.connector.DictCursor)
        try:
            cur.execute(sql, (doc_id,))
            return cur.fetchone()
        finally:
            cur.close()

    def get_document_chunks(self, doc_id: str) -> list[dict]:
        sql = """
        SELECT chunk_index, section, start_char, end_char, word_count, content
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

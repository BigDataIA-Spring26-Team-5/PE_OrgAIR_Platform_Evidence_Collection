"""
Signal Scores Repository - PE Org-AI-R Platform
app/repositories/signal_scores_repository.py

Handles Snowflake operations for company signal scores.
Upsert strategy: Replace row if ticker exists, insert if new.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Dict, List, Optional
from uuid import UUID

import snowflake.connector

from app.services.snowflake import get_snowflake_connection


class SignalScoresRepository:
    """
    Repository for storing and retrieving company signal scores from Snowflake.

    Table: SIGNAL_SCORES
    Upsert strategy: MERGE by ticker (replace if exists, insert if new)
    """

    TABLE_NAME = "SIGNAL_SCORES"

    def __init__(self):
        self.conn = get_snowflake_connection()

    def upsert_signal_scores(
        self,
        company_id: str,
        company_name: str,
        ticker: str,
        hiring_score: Optional[float] = None,
        innovation_score: Optional[float] = None,
        tech_stack_score: Optional[float] = None,
        leadership_score: Optional[float] = None,
        composite_score: Optional[float] = None,
        total_jobs: int = 0,
        ai_jobs: int = 0,
        total_patents: int = 0,
        ai_patents: int = 0,
        techstack_keywords: Optional[List[str]] = None,
        s3_jobs_key: Optional[str] = None,
        s3_patents_key: Optional[str] = None,
    ) -> str:
        """
        Insert or update signal scores for a company by ticker.

        If ticker exists, replaces the existing row.
        If ticker doesn't exist, inserts a new row.

        Args:
            company_id: UUID of the company
            company_name: Company name
            ticker: Company ticker symbol (used as upsert key)
            hiring_score: Job market/hiring signal score (0-100)
            innovation_score: Patent/innovation signal score (0-100)
            tech_stack_score: Tech stack signal score (0-100)
            leadership_score: Leadership signal score (0-100) - blank for now
            composite_score: Calculated composite score
            total_jobs: Total job postings found
            ai_jobs: AI-related job postings
            total_patents: Total patents found
            ai_patents: AI-related patents
            techstack_keywords: List of tech keywords found
            s3_jobs_key: S3 key for jobs data
            s3_patents_key: S3 key for patents data

        Returns:
            ticker of the upserted record
        """
        import json

        ticker_upper = ticker.upper()
        now = datetime.now(timezone.utc)
        keywords_json = json.dumps(techstack_keywords or [])

        sql = """
        MERGE INTO SIGNAL_SCORES t
        USING (SELECT %s AS ticker) s
        ON t.ticker = s.ticker
        WHEN NOT MATCHED THEN INSERT (
            company_id, company_name, ticker,
            hiring_score, innovation_score, tech_stack_score,
            leadership_score, composite_score,
            total_jobs, ai_jobs, total_patents, ai_patents,
            techstack_keywords,
            s3_jobs_key, s3_patents_key,
            created_at, updated_at
        ) VALUES (
            %s, %s, %s,
            %s, %s, %s,
            %s, %s,
            %s, %s, %s, %s,
            PARSE_JSON(%s),
            %s, %s,
            %s, %s
        )
        WHEN MATCHED THEN UPDATE SET
            company_id = %s,
            company_name = %s,
            hiring_score = %s,
            innovation_score = %s,
            tech_stack_score = %s,
            leadership_score = %s,
            composite_score = %s,
            total_jobs = %s,
            ai_jobs = %s,
            total_patents = %s,
            ai_patents = %s,
            techstack_keywords = PARSE_JSON(%s),
            s3_jobs_key = %s,
            s3_patents_key = %s,
            updated_at = %s
        """

        params = (
            # USING clause
            ticker_upper,
            # INSERT values
            company_id, company_name, ticker_upper,
            hiring_score, innovation_score, tech_stack_score,
            leadership_score, composite_score,
            total_jobs, ai_jobs, total_patents, ai_patents,
            keywords_json,
            s3_jobs_key, s3_patents_key,
            now, now,
            # UPDATE values
            company_id, company_name,
            hiring_score, innovation_score, tech_stack_score,
            leadership_score, composite_score,
            total_jobs, ai_jobs, total_patents, ai_patents,
            keywords_json,
            s3_jobs_key, s3_patents_key,
            now,
        )

        cur = self.conn.cursor()
        try:
            cur.execute(sql, params)
            self.conn.commit()
            return ticker_upper
        finally:
            cur.close()

    def get_by_ticker(self, ticker: str) -> Optional[Dict]:
        """
        Get signal scores for a company by ticker.

        Args:
            ticker: Company ticker symbol

        Returns:
            Signal scores dict or None if not found
        """
        sql = """
        SELECT
            company_id, company_name, ticker,
            hiring_score, innovation_score, tech_stack_score,
            leadership_score, composite_score,
            total_jobs, ai_jobs, total_patents, ai_patents,
            techstack_keywords,
            s3_jobs_key, s3_patents_key,
            created_at, updated_at
        FROM SIGNAL_SCORES
        WHERE ticker = %s
        """

        cur = self.conn.cursor(snowflake.connector.DictCursor)
        try:
            cur.execute(sql, (ticker.upper(),))
            row = cur.fetchone()
            if not row:
                return None
            return self._row_to_dict(row)
        finally:
            cur.close()

    def get_by_company_id(self, company_id: str) -> Optional[Dict]:
        """
        Get signal scores for a company by company ID.

        Args:
            company_id: Company UUID

        Returns:
            Signal scores dict or None if not found
        """
        sql = """
        SELECT
            company_id, company_name, ticker,
            hiring_score, innovation_score, tech_stack_score,
            leadership_score, composite_score,
            total_jobs, ai_jobs, total_patents, ai_patents,
            techstack_keywords,
            s3_jobs_key, s3_patents_key,
            created_at, updated_at
        FROM SIGNAL_SCORES
        WHERE company_id = %s
        """

        cur = self.conn.cursor(snowflake.connector.DictCursor)
        try:
            cur.execute(sql, (company_id,))
            row = cur.fetchone()
            if not row:
                return None
            return self._row_to_dict(row)
        finally:
            cur.close()

    def get_all(self) -> List[Dict]:
        """
        Get all signal scores.

        Returns:
            List of signal scores dicts
        """
        sql = """
        SELECT
            company_id, company_name, ticker,
            hiring_score, innovation_score, tech_stack_score,
            leadership_score, composite_score,
            total_jobs, ai_jobs, total_patents, ai_patents,
            techstack_keywords,
            s3_jobs_key, s3_patents_key,
            created_at, updated_at
        FROM SIGNAL_SCORES
        ORDER BY updated_at DESC
        """

        cur = self.conn.cursor(snowflake.connector.DictCursor)
        try:
            cur.execute(sql)
            return [self._row_to_dict(row) for row in cur.fetchall()]
        finally:
            cur.close()

    def delete_by_ticker(self, ticker: str) -> bool:
        """
        Delete signal scores for a company by ticker.

        Args:
            ticker: Company ticker symbol

        Returns:
            True if deleted, False if not found
        """
        sql = "DELETE FROM SIGNAL_SCORES WHERE ticker = %s"

        cur = self.conn.cursor()
        try:
            cur.execute(sql, (ticker.upper(),))
            self.conn.commit()
            return cur.rowcount > 0
        finally:
            cur.close()

    def _row_to_dict(self, row: Dict) -> Dict:
        """Convert Snowflake row to signal scores dict."""
        return {
            "company_id": row.get("COMPANY_ID"),
            "company_name": row.get("COMPANY_NAME"),
            "ticker": row.get("TICKER"),
            "hiring_score": float(row["HIRING_SCORE"]) if row.get("HIRING_SCORE") is not None else None,
            "innovation_score": float(row["INNOVATION_SCORE"]) if row.get("INNOVATION_SCORE") is not None else None,
            "tech_stack_score": float(row["TECH_STACK_SCORE"]) if row.get("TECH_STACK_SCORE") is not None else None,
            "leadership_score": float(row["LEADERSHIP_SCORE"]) if row.get("LEADERSHIP_SCORE") is not None else None,
            "composite_score": float(row["COMPOSITE_SCORE"]) if row.get("COMPOSITE_SCORE") is not None else None,
            "total_jobs": int(row.get("TOTAL_JOBS", 0)),
            "ai_jobs": int(row.get("AI_JOBS", 0)),
            "total_patents": int(row.get("TOTAL_PATENTS", 0)),
            "ai_patents": int(row.get("AI_PATENTS", 0)),
            "techstack_keywords": row.get("TECHSTACK_KEYWORDS") or [],
            "s3_jobs_key": row.get("S3_JOBS_KEY"),
            "s3_patents_key": row.get("S3_PATENTS_KEY"),
            "created_at": row.get("CREATED_AT"),
            "updated_at": row.get("UPDATED_AT"),
        }

    def close(self):
        """Close the database connection."""
        try:
            self.conn.close()
        except Exception:
            pass


def calculate_composite_score(
    hiring_score: Optional[float] = None,
    innovation_score: Optional[float] = None,
    tech_stack_score: Optional[float] = None,
    leadership_score: Optional[float] = None,
) -> Optional[float]:
    """
    Calculate composite score from individual signal scores.

    Uses equal weighting for available scores.
    Leadership score is optional (can be None/blank).

    Formula: Average of available scores
    - If all 4 available: (hiring + innovation + tech + leadership) / 4
    - If 3 available (no leadership): (hiring + innovation + tech) / 3
    - Returns None if no scores available

    Args:
        hiring_score: Job market/hiring score (0-100)
        innovation_score: Patent/innovation score (0-100)
        tech_stack_score: Tech stack score (0-100)
        leadership_score: Leadership score (0-100), optional

    Returns:
        Composite score (0-100) or None if no scores available
    """
    scores = []

    if hiring_score is not None:
        scores.append(hiring_score)
    if innovation_score is not None:
        scores.append(innovation_score)
    if tech_stack_score is not None:
        scores.append(tech_stack_score)
    if leadership_score is not None:
        scores.append(leadership_score)

    if not scores:
        return None

    return round(sum(scores) / len(scores), 2)

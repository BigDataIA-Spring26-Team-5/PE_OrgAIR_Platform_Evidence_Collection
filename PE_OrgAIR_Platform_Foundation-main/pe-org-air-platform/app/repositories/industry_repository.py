"""
Industry Repository - PE Org-AI-R Platform
app/repositories/industry_repository.py

Data access layer for Industry entity operations.
"""

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from uuid import UUID

from app.repositories.base import BaseRepository


class IndustryRepository(BaseRepository):
    """Repository for Industry CRUD operations."""

    TABLE_NAME = "INDUSTRIES"

    def get_by_id(self, industry_id: UUID) -> Optional[Dict[str, Any]]:
        """
        Retrieve an industry by ID.

        Args:
            industry_id: UUID of the industry

        Returns:
            Industry dict or None if not found
        """
        sql = """
            SELECT ID, NAME, SECTOR, H_R_BASE, CREATED_AT
            FROM INDUSTRIES
            WHERE ID = %s
        """
        row = self.execute_query(sql, (str(industry_id),), fetch_one=True)

        if not row:
            return None

        return self._row_to_dict(row)

    def get_all(self) -> List[Dict[str, Any]]:
        """
        Retrieve all industries.

        Returns:
            List of industry dicts
        """
        sql = """
            SELECT ID, NAME, SECTOR, H_R_BASE, CREATED_AT
            FROM INDUSTRIES
            ORDER BY NAME
        """
        rows = self.execute_query(sql, fetch_all=True) or []

        return [self._row_to_dict(row) for row in rows]

    def exists(self, industry_id: UUID) -> bool:
        """
        Check if an industry exists.

        Args:
            industry_id: UUID of the industry

        Returns:
            True if exists, False otherwise
        """
        sql = "SELECT 1 FROM INDUSTRIES WHERE ID = %s"
        row = self.execute_query(sql, (str(industry_id),), fetch_one=True)
        return row is not None

    def _row_to_dict(self, row: Dict[str, Any]) -> Dict[str, Any]:
        """Convert Snowflake row to industry dict."""
        return {
            "id": row["ID"],
            "name": row["NAME"],
            "sector": row["SECTOR"],
            "h_r_base": float(row["H_R_BASE"]) if row["H_R_BASE"] is not None else 0.0,
            "created_at": self.normalize_timestamp(row.get("CREATED_AT")),
        }

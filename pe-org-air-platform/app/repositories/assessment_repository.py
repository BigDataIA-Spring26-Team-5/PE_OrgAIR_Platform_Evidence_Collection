"""
Assessment Repository - PE Org-AI-R Platform
app/repositories/assessment_repository.py

Data access layer for Assessment entity operations.
"""

from datetime import date, datetime, timezone
from typing import Any, Dict, List, Optional, Tuple
from uuid import UUID, uuid4

from app.models.enumerations import AssessmentStatus, AssessmentType
from app.repositories.base import BaseRepository


class AssessmentRepository(BaseRepository):
    """Repository for Assessment CRUD operations."""

    TABLE_NAME = "ASSESSMENTS"

    def create(
        self,
        company_id: UUID,
        assessment_type: AssessmentType,
        assessment_date: date,
        primary_assessor: Optional[str] = None,
        secondary_assessor: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Create a new assessment.

        Args:
            company_id: UUID of the company
            assessment_type: Type of assessment
            assessment_date: Date of assessment
            primary_assessor: Primary assessor name
            secondary_assessor: Secondary assessor name

        Returns:
            Created assessment dict
        """
        assessment_id = uuid4()
        now = datetime.now(timezone.utc)

        sql = """
            INSERT INTO ASSESSMENTS (ID, COMPANY_ID, ASSESSMENT_TYPE, ASSESSMENT_DATE,
                                     STATUS, PRIMARY_ASSESSOR, SECONDARY_ASSESSOR,
                                     V_R_SCORE, CONFIDENCE_LOWER, CONFIDENCE_UPPER, CREATED_AT)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        params = (
            str(assessment_id),
            str(company_id),
            assessment_type.value,
            assessment_date,
            AssessmentStatus.DRAFT.value,
            primary_assessor,
            secondary_assessor,
            None,  # v_r_score
            None,  # confidence_lower
            None,  # confidence_upper
            now,
        )

        self.execute_query(sql, params, commit=True)

        return self.get_by_id(assessment_id)

    def get_by_id(self, assessment_id: UUID) -> Optional[Dict[str, Any]]:
        """
        Retrieve an assessment by ID.

        Args:
            assessment_id: UUID of the assessment

        Returns:
            Assessment dict or None if not found
        """
        sql = """
            SELECT ID, COMPANY_ID, ASSESSMENT_TYPE, ASSESSMENT_DATE,
                   STATUS, PRIMARY_ASSESSOR, SECONDARY_ASSESSOR,
                   V_R_SCORE, CONFIDENCE_LOWER, CONFIDENCE_UPPER, CREATED_AT
            FROM ASSESSMENTS
            WHERE ID = %s
        """
        row = self.execute_query(sql, (str(assessment_id),), fetch_one=True)

        if not row:
            return None

        return self._row_to_dict(row)

    def get_all(
        self,
        page: int = 1,
        page_size: int = 20,
        company_id: Optional[UUID] = None,
        assessment_type: Optional[AssessmentType] = None,
        status: Optional[AssessmentStatus] = None,
    ) -> Tuple[List[Dict[str, Any]], int]:
        """
        Retrieve paginated list of assessments with optional filters.

        Args:
            page: Page number (1-indexed)
            page_size: Number of items per page
            company_id: Optional filter by company
            assessment_type: Optional filter by type
            status: Optional filter by status

        Returns:
            Tuple of (list of assessment dicts, total count)
        """
        offset = (page - 1) * page_size

        # Build WHERE clause
        where_clauses = ["1=1"]  # Always true base clause
        params: List[Any] = []

        if company_id:
            where_clauses.append("COMPANY_ID = %s")
            params.append(str(company_id))

        if assessment_type:
            where_clauses.append("ASSESSMENT_TYPE = %s")
            params.append(assessment_type.value)

        if status:
            where_clauses.append("STATUS = %s")
            params.append(status.value)

        where_sql = " AND ".join(where_clauses)

        # Count query
        count_sql = f"SELECT COUNT(*) as TOTAL FROM ASSESSMENTS WHERE {where_sql}"
        count_result = self.execute_query(count_sql, tuple(params), fetch_one=True)
        total = count_result["TOTAL"] if count_result else 0

        # Data query
        data_sql = f"""
            SELECT ID, COMPANY_ID, ASSESSMENT_TYPE, ASSESSMENT_DATE,
                   STATUS, PRIMARY_ASSESSOR, SECONDARY_ASSESSOR,
                   V_R_SCORE, CONFIDENCE_LOWER, CONFIDENCE_UPPER, CREATED_AT
            FROM ASSESSMENTS
            WHERE {where_sql}
            ORDER BY CREATED_AT DESC
            LIMIT %s OFFSET %s
        """
        data_params = tuple(params) + (page_size, offset)
        rows = self.execute_query(data_sql, data_params, fetch_all=True) or []

        assessments = [self._row_to_dict(row) for row in rows]
        return assessments, total

    def update_status(
        self, assessment_id: UUID, new_status: AssessmentStatus
    ) -> Optional[Dict[str, Any]]:
        """
        Update the status of an assessment.

        Args:
            assessment_id: UUID of the assessment
            new_status: New status value

        Returns:
            Updated assessment dict or None if not found
        """
        sql = """
            UPDATE ASSESSMENTS
            SET STATUS = %s
            WHERE ID = %s
        """
        self.execute_query(sql, (new_status.value, str(assessment_id)), commit=True)
        return self.get_by_id(assessment_id)

    def exists(self, assessment_id: UUID) -> bool:
        """
        Check if an assessment exists.

        Args:
            assessment_id: UUID of the assessment

        Returns:
            True if exists, False otherwise
        """
        sql = "SELECT 1 FROM ASSESSMENTS WHERE ID = %s"
        row = self.execute_query(sql, (str(assessment_id),), fetch_one=True)
        return row is not None

    def _row_to_dict(self, row: Dict[str, Any]) -> Dict[str, Any]:
        """Convert Snowflake row to assessment dict."""
        return {
            "id": UUID(row["ID"]),
            "company_id": UUID(row["COMPANY_ID"]),
            "assessment_type": AssessmentType(row["ASSESSMENT_TYPE"]),
            "assessment_date": row["ASSESSMENT_DATE"],
            "status": AssessmentStatus(row["STATUS"]),
            "primary_assessor": row["PRIMARY_ASSESSOR"],
            "secondary_assessor": row["SECONDARY_ASSESSOR"],
            "v_r_score": float(row["V_R_SCORE"]) if row["V_R_SCORE"] is not None else None,
            "confidence_lower": float(row["CONFIDENCE_LOWER"]) if row["CONFIDENCE_LOWER"] is not None else None,
            "confidence_upper": float(row["CONFIDENCE_UPPER"]) if row["CONFIDENCE_UPPER"] is not None else None,
            "created_at": self.normalize_timestamp(row["CREATED_AT"]),
        }

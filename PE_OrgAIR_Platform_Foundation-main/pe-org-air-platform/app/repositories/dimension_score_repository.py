"""
Dimension Score Repository - PE Org-AI-R Platform
app/repositories/dimension_score_repository.py

Data access layer for DimensionScore entity operations.
"""

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from uuid import UUID, uuid4

from app.models.dimension import DIMENSION_WEIGHTS
from app.models.enumerations import Dimension
from app.repositories.base import BaseRepository


class DimensionScoreRepository(BaseRepository):
    """Repository for DimensionScore CRUD operations."""

    TABLE_NAME = "DIMENSION_SCORES"

    def create(
        self,
        assessment_id: UUID,
        dimension: Dimension,
        score: float,
        weight: Optional[float] = None,
        confidence: float = 0.8,
        evidence_count: int = 0,
    ) -> Dict[str, Any]:
        """
        Create a new dimension score.

        Args:
            assessment_id: UUID of the assessment
            dimension: Dimension enum value
            score: Score value (0-100)
            weight: Optional custom weight (defaults to DIMENSION_WEIGHTS)
            confidence: Confidence level (default 0.8)
            evidence_count: Number of evidence items (default 0)

        Returns:
            Created dimension score dict
        """
        score_id = uuid4()
        now = datetime.now(timezone.utc)

        # Use default weight if not provided
        if weight is None:
            weight = DIMENSION_WEIGHTS.get(dimension, 0.1)

        sql = """
            INSERT INTO DIMENSION_SCORES (ID, ASSESSMENT_ID, DIMENSION, SCORE,
                                          WEIGHT, CONFIDENCE, EVIDENCE_COUNT, CREATED_AT)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
        params = (
            str(score_id),
            str(assessment_id),
            dimension.value,
            score,
            weight,
            confidence,
            evidence_count,
            now,
        )

        self.execute_query(sql, params, commit=True)

        return self.get_by_id(score_id)

    def get_by_id(self, score_id: UUID) -> Optional[Dict[str, Any]]:
        """
        Retrieve a dimension score by ID.

        Args:
            score_id: UUID of the dimension score

        Returns:
            Dimension score dict or None if not found
        """
        sql = """
            SELECT ID, ASSESSMENT_ID, DIMENSION, SCORE, WEIGHT,
                   CONFIDENCE, EVIDENCE_COUNT, CREATED_AT
            FROM DIMENSION_SCORES
            WHERE ID = %s
        """
        row = self.execute_query(sql, (str(score_id),), fetch_one=True)

        if not row:
            return None

        return self._row_to_dict(row)

    def get_by_assessment_id(self, assessment_id: UUID) -> List[Dict[str, Any]]:
        """
        Retrieve all dimension scores for an assessment.

        Args:
            assessment_id: UUID of the assessment

        Returns:
            List of dimension score dicts
        """
        sql = """
            SELECT ID, ASSESSMENT_ID, DIMENSION, SCORE, WEIGHT,
                   CONFIDENCE, EVIDENCE_COUNT, CREATED_AT
            FROM DIMENSION_SCORES
            WHERE ASSESSMENT_ID = %s
            ORDER BY CREATED_AT
        """
        rows = self.execute_query(sql, (str(assessment_id),), fetch_all=True) or []

        return [self._row_to_dict(row) for row in rows]

    def update(
        self,
        score_id: UUID,
        score: Optional[float] = None,
        weight: Optional[float] = None,
        confidence: Optional[float] = None,
        evidence_count: Optional[int] = None,
        dimension: Optional[Dimension] = None,
    ) -> Optional[Dict[str, Any]]:
        """
        Update a dimension score.

        Args:
            score_id: UUID of the dimension score
            score: New score value
            weight: New weight value
            confidence: New confidence value
            evidence_count: New evidence count
            dimension: New dimension (also updates weight to default if weight not provided)

        Returns:
            Updated dimension score dict or None if not found
        """
        # Build update data
        update_data: Dict[str, Any] = {}
        if score is not None:
            update_data["SCORE"] = score
        if weight is not None:
            update_data["WEIGHT"] = weight
        if confidence is not None:
            update_data["CONFIDENCE"] = confidence
        if evidence_count is not None:
            update_data["EVIDENCE_COUNT"] = evidence_count
        if dimension is not None:
            update_data["DIMENSION"] = dimension.value
            # Update weight to default for new dimension if not explicitly set
            if weight is None:
                update_data["WEIGHT"] = DIMENSION_WEIGHTS.get(dimension, 0.1)

        if not update_data:
            return self.get_by_id(score_id)

        # Build SET clause
        set_clauses = [f"{col} = %s" for col in update_data.keys()]
        params = list(update_data.values())
        params.append(str(score_id))

        sql = f"""
            UPDATE DIMENSION_SCORES
            SET {', '.join(set_clauses)}
            WHERE ID = %s
        """

        self.execute_query(sql, tuple(params), commit=True)
        return self.get_by_id(score_id)

    def exists(self, score_id: UUID) -> bool:
        """
        Check if a dimension score exists.

        Args:
            score_id: UUID of the dimension score

        Returns:
            True if exists, False otherwise
        """
        sql = "SELECT 1 FROM DIMENSION_SCORES WHERE ID = %s"
        row = self.execute_query(sql, (str(score_id),), fetch_one=True)
        return row is not None

    def check_dimension_exists(self, assessment_id: UUID, dimension: Dimension) -> bool:
        """
        Check if a score for a specific dimension already exists for an assessment.

        Args:
            assessment_id: UUID of the assessment
            dimension: Dimension to check

        Returns:
            True if exists, False otherwise
        """
        sql = """
            SELECT 1 FROM DIMENSION_SCORES
            WHERE ASSESSMENT_ID = %s AND DIMENSION = %s
        """
        row = self.execute_query(
            sql, (str(assessment_id), dimension.value), fetch_one=True
        )
        return row is not None

    def _row_to_dict(self, row: Dict[str, Any]) -> Dict[str, Any]:
        """Convert Snowflake row to dimension score dict."""
        return {
            "id": UUID(row["ID"]),
            "assessment_id": UUID(row["ASSESSMENT_ID"]),
            "dimension": Dimension(row["DIMENSION"]),
            "score": float(row["SCORE"]) if row["SCORE"] is not None else 0.0,
            "weight": float(row["WEIGHT"]) if row["WEIGHT"] is not None else 0.1,
            "confidence": float(row["CONFIDENCE"]) if row["CONFIDENCE"] is not None else 0.8,
            "evidence_count": int(row["EVIDENCE_COUNT"]) if row["EVIDENCE_COUNT"] is not None else 0,
            "created_at": self.normalize_timestamp(row["CREATED_AT"]),
        }

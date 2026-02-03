# """
# Company Repository - PE Org-AI-R Platform
# app/repositories/company_repository.py

# Data access layer for Company entity operations with soft-delete support.
# """

# from datetime import datetime, timezone
# from typing import Any, Dict, List, Optional, Tuple
# from uuid import UUID, uuid4

# from app.repositories.base import BaseRepository


# class CompanyRepository(BaseRepository):
#     """Repository for Company CRUD operations with soft-delete support."""

#     TABLE_NAME = "COMPANIES"

#     def create(
#         self,
#         name: str,
#         industry_id: UUID,
#         ticker: Optional[str] = None,
#         position_factor: float = 0.0,
#     ) -> Dict[str, Any]:
#         """
#         Create a new company.

#         Args:
#             name: Company name
#             industry_id: UUID of the industry
#             ticker: Optional stock ticker symbol
#             position_factor: Position factor (-1.0 to 1.0)

#         Returns:
#             Created company dict
#         """
#         company_id = uuid4()
#         now = datetime.now(timezone.utc)

#         sql = """
#             INSERT INTO COMPANIES (ID, NAME, TICKER, INDUSTRY_ID, POSITION_FACTOR,
#                                    IS_DELETED, CREATED_AT, UPDATED_AT)
#             VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
#         """
#         params = (
#             str(company_id),
#             name,
#             ticker,
#             str(industry_id),
#             position_factor,
#             False,
#             now,
#             now,
#         )

#         self.execute_query(sql, params, commit=True)

#         return self.get_by_id(company_id)

#     def get_by_id(self, company_id: UUID) -> Optional[Dict[str, Any]]:
#         """
#         Retrieve a company by ID (excluding soft-deleted).

#         Args:
#             company_id: UUID of the company

#         Returns:
#             Company dict or None if not found
#         """
#         sql = """
#             SELECT ID, NAME, TICKER, INDUSTRY_ID, POSITION_FACTOR,
#                    IS_DELETED, CREATED_AT, UPDATED_AT
#             FROM COMPANIES
#             WHERE ID = %s AND IS_DELETED = FALSE
#         """
#         row = self.execute_query(sql, (str(company_id),), fetch_one=True)

#         if not row:
#             return None

#         return self._row_to_dict(row)

#     def get_all(
#         self,
#         page: int = 1,
#         page_size: int = 20,
#         industry_id: Optional[UUID] = None,
#     ) -> Tuple[List[Dict[str, Any]], int]:
#         """
#         Retrieve paginated list of companies.

#         Args:
#             page: Page number (1-indexed)
#             page_size: Number of items per page
#             industry_id: Optional filter by industry

#         Returns:
#             Tuple of (list of company dicts, total count)
#         """
#         offset = (page - 1) * page_size

#         # Build WHERE clause
#         where_clauses = ["IS_DELETED = FALSE"]
#         params: List[Any] = []

#         if industry_id:
#             where_clauses.append("INDUSTRY_ID = %s")
#             params.append(str(industry_id))

#         where_sql = " AND ".join(where_clauses)

#         # Count query
#         count_sql = f"SELECT COUNT(*) as TOTAL FROM COMPANIES WHERE {where_sql}"
#         count_result = self.execute_query(count_sql, tuple(params), fetch_one=True)
#         total = count_result["TOTAL"] if count_result else 0

#         # Data query
#         data_sql = f"""
#             SELECT ID, NAME, TICKER, INDUSTRY_ID, POSITION_FACTOR,
#                    IS_DELETED, CREATED_AT, UPDATED_AT
#             FROM COMPANIES
#             WHERE {where_sql}
#             ORDER BY CREATED_AT DESC
#             LIMIT %s OFFSET %s
#         """
#         data_params = tuple(params) + (page_size, offset)
#         rows = self.execute_query(data_sql, data_params, fetch_all=True) or []

#         companies = [self._row_to_dict(row) for row in rows]
#         return companies, total

#     def update(
#         self,
#         company_id: UUID,
#         name: Optional[str] = None,
#         ticker: Optional[str] = None,
#         industry_id: Optional[UUID] = None,
#         position_factor: Optional[float] = None,
#     ) -> Optional[Dict[str, Any]]:
#         """
#         Update a company.

#         Args:
#             company_id: UUID of the company
#             name: New name (optional)
#             ticker: New ticker (optional)
#             industry_id: New industry ID (optional)
#             position_factor: New position factor (optional)

#         Returns:
#             Updated company dict or None if not found
#         """
#         # Build update data
#         update_data: Dict[str, Any] = {}
#         if name is not None:
#             update_data["NAME"] = name
#         if ticker is not None:
#             update_data["TICKER"] = ticker
#         if industry_id is not None:
#             update_data["INDUSTRY_ID"] = str(industry_id)
#         if position_factor is not None:
#             update_data["POSITION_FACTOR"] = position_factor

#         if not update_data:
#             return self.get_by_id(company_id)

#         # Add updated_at
#         update_data["UPDATED_AT"] = datetime.now(timezone.utc)

#         # Build SET clause
#         set_clauses = [f"{col} = %s" for col in update_data.keys()]
#         params = list(update_data.values())
#         params.append(str(company_id))

#         sql = f"""
#             UPDATE COMPANIES
#             SET {', '.join(set_clauses)}
#             WHERE ID = %s AND IS_DELETED = FALSE
#         """

#         self.execute_query(sql, tuple(params), commit=True)
#         return self.get_by_id(company_id)

#     def soft_delete(self, company_id: UUID) -> bool:
#         """
#         Soft delete a company.

#         Args:
#             company_id: UUID of the company

#         Returns:
#             True if deleted, False if not found
#         """
#         sql = """
#             UPDATE COMPANIES
#             SET IS_DELETED = TRUE, UPDATED_AT = %s
#             WHERE ID = %s AND IS_DELETED = FALSE
#         """
#         affected = self.execute_query(
#             sql, (datetime.now(timezone.utc), str(company_id)), commit=True
#         )
#         return affected > 0 if affected else False

#     def is_deleted(self, company_id: UUID) -> bool:
#         """
#         Check if a company is soft-deleted.

#         Args:
#             company_id: UUID of the company

#         Returns:
#             True if deleted, False otherwise
#         """
#         sql = "SELECT IS_DELETED FROM COMPANIES WHERE ID = %s"
#         row = self.execute_query(sql, (str(company_id),), fetch_one=True)

#         if not row:
#             return False  # Company doesn't exist at all

#         return row["IS_DELETED"]

#     def exists(self, company_id: UUID) -> bool:
#         """
#         Check if a company exists (regardless of deletion status).

#         Args:
#             company_id: UUID of the company

#         Returns:
#             True if exists, False otherwise
#         """
#         sql = "SELECT 1 FROM COMPANIES WHERE ID = %s"
#         row = self.execute_query(sql, (str(company_id),), fetch_one=True)
#         return row is not None

#     def exists_active(self, company_id: UUID) -> bool:
#         """
#         Check if an active (non-deleted) company exists.

#         Args:
#             company_id: UUID of the company

#         Returns:
#             True if exists and active, False otherwise
#         """
#         sql = "SELECT 1 FROM COMPANIES WHERE ID = %s AND IS_DELETED = FALSE"
#         row = self.execute_query(sql, (str(company_id),), fetch_one=True)
#         return row is not None

#     def check_duplicate(
#         self,
#         name: str,
#         industry_id: UUID,
#         exclude_id: Optional[UUID] = None,
#     ) -> bool:
#         """
#         Check for duplicate company name in same industry.

#         Args:
#             name: Company name to check
#             industry_id: Industry UUID
#             exclude_id: Company ID to exclude (for updates)

#         Returns:
#             True if duplicate exists, False otherwise
#         """
#         sql = """
#             SELECT 1 FROM COMPANIES
#             WHERE NAME = %s AND INDUSTRY_ID = %s AND IS_DELETED = FALSE
#         """
#         params: List[Any] = [name, str(industry_id)]

#         if exclude_id:
#             sql += " AND ID != %s"
#             params.append(str(exclude_id))

#         row = self.execute_query(sql, tuple(params), fetch_one=True)
#         return row is not None

#     def _row_to_dict(self, row: Dict[str, Any]) -> Dict[str, Any]:
#         """Convert Snowflake row to company dict."""
#         return {
#             "id": row["ID"],
#             "name": row["NAME"],
#             "ticker": row["TICKER"],
#             "industry_id": row["INDUSTRY_ID"],
#             "position_factor": float(row["POSITION_FACTOR"]) if row["POSITION_FACTOR"] is not None else 0.0,
#             "is_deleted": row["IS_DELETED"],
#             "created_at": self.normalize_timestamp(row["CREATED_AT"]),
#             "updated_at": self.normalize_timestamp(row["UPDATED_AT"]),
#         }

from __future__ import annotations

from typing import List, Dict, Optional
from uuid import UUID, uuid4

from app.services.snowflake import get_snowflake_connection


class CompanyRepository:
    """
    Repository for accessing companies from Snowflake.
    """

    def __init__(self):
        self.conn = get_snowflake_connection()

    def get_all(self) -> List[Dict]:
        """
        Return all active (non-deleted) companies.
        """
        sql = """
        SELECT
            id,
            name,
            ticker,
            industry_id,
            position_factor,
            is_deleted,
            created_at,
            updated_at
        FROM companies
        WHERE is_deleted = FALSE
        ORDER BY name
        """

        cur = self.conn.cursor()
        try:
            cur.execute(sql)
            columns = [col[0].lower() for col in cur.description]
            return [dict(zip(columns, row)) for row in cur.fetchall()]
        finally:
            cur.close()

    def get_by_id(self, company_id: UUID) -> Dict | None:
        """
        Fetch a single company by ID.
        """
        sql = """
        SELECT
            id,
            name,
            ticker,
            industry_id,
            position_factor,
            is_deleted,
            created_at,
            updated_at
        FROM companies
        WHERE id = %s AND is_deleted = FALSE
        """

        cur = self.conn.cursor()
        try:
            cur.execute(sql, (str(company_id),))
            row = cur.fetchone()
            if not row:
                return None
            columns = [col[0].lower() for col in cur.description]
            return dict(zip(columns, row))
        finally:
            cur.close()

    def get_by_ticker(self, ticker: str) -> Dict | None:
        """
        Fetch a single company by ticker.
        """
        sql = """
        SELECT
            id,
            name,
            ticker,
            industry_id,
            position_factor,
            is_deleted,
            created_at,
            updated_at
        FROM companies
        WHERE ticker = %s AND is_deleted = FALSE
        """

        cur = self.conn.cursor()
        try:
            cur.execute(sql, (ticker,))
            row = cur.fetchone()
            if not row:
                return None
            columns = [col[0].lower() for col in cur.description]
            return dict(zip(columns, row))
        finally:
            cur.close()

    def get_by_industry(self, industry_id: UUID) -> List[Dict]:
        """
        Return all active companies for a specific industry.
        """
        sql = """
        SELECT
            id,
            name,
            ticker,
            industry_id,
            position_factor,
            is_deleted,
            created_at,
            updated_at
        FROM companies
        WHERE industry_id = %s AND is_deleted = FALSE
        ORDER BY name
        """

        cur = self.conn.cursor()
        try:
            cur.execute(sql, (str(industry_id),))
            columns = [col[0].lower() for col in cur.description]
            return [dict(zip(columns, row)) for row in cur.fetchall()]
        finally:
            cur.close()

    def exists(self, company_id: UUID) -> bool:
        """
        Check if a company exists (regardless of deleted status).
        """
        sql = "SELECT 1 FROM companies WHERE id = %s"
        cur = self.conn.cursor()
        try:
            cur.execute(sql, (str(company_id),))
            return cur.fetchone() is not None
        finally:
            cur.close()

    def is_deleted(self, company_id: UUID) -> bool:
        """
        Check if a company is soft-deleted.
        """
        sql = "SELECT is_deleted FROM companies WHERE id = %s"
        cur = self.conn.cursor()
        try:
            cur.execute(sql, (str(company_id),))
            row = cur.fetchone()
            return row is not None and row[0] is True
        finally:
            cur.close()

    def check_duplicate(
        self,
        name: str,
        industry_id: UUID,
        exclude_id: Optional[UUID] = None
    ) -> bool:
        """
        Check if a company with the same name exists in the same industry.
        """
        if exclude_id:
            sql = """
            SELECT 1 FROM companies 
            WHERE name = %s AND industry_id = %s AND id != %s AND is_deleted = FALSE
            """
            params = (name, str(industry_id), str(exclude_id))
        else:
            sql = """
            SELECT 1 FROM companies 
            WHERE name = %s AND industry_id = %s AND is_deleted = FALSE
            """
            params = (name, str(industry_id))

        cur = self.conn.cursor()
        try:
            cur.execute(sql, params)
            return cur.fetchone() is not None
        finally:
            cur.close()

    def create(
        self,
        name: str,
        industry_id: UUID,
        ticker: Optional[str] = None,
        position_factor: float = 0.0,
    ) -> Dict:
        """
        Create a new company and return its data.
        """
        company_id = str(uuid4())

        sql = """
        INSERT INTO companies (id, name, ticker, industry_id, position_factor)
        VALUES (%s, %s, %s, %s, %s)
        """

        cur = self.conn.cursor()
        try:
            cur.execute(sql, (company_id, name, ticker, str(industry_id), position_factor))
            self.conn.commit()
        finally:
            cur.close()

        return self.get_by_id(UUID(company_id))

    def update(
        self,
        company_id: UUID,
        name: Optional[str] = None,
        ticker: Optional[str] = None,
        industry_id: Optional[UUID] = None,
        position_factor: Optional[float] = None,
    ) -> Dict:
        """
        Update a company's fields and return updated data.
        """
        updates = []
        params = []

        if name is not None:
            updates.append("name = %s")
            params.append(name)
        if ticker is not None:
            updates.append("ticker = %s")
            params.append(ticker)
        if industry_id is not None:
            updates.append("industry_id = %s")
            params.append(str(industry_id))
        if position_factor is not None:
            updates.append("position_factor = %s")
            params.append(position_factor)

        if not updates:
            return self.get_by_id(company_id)

        updates.append("updated_at = CURRENT_TIMESTAMP()")
        params.append(str(company_id))

        sql = f"UPDATE companies SET {', '.join(updates)} WHERE id = %s"

        cur = self.conn.cursor()
        try:
            cur.execute(sql, tuple(params))
            self.conn.commit()
        finally:
            cur.close()

        return self.get_by_id(company_id)

    def soft_delete(self, company_id: UUID) -> None:
        """
        Soft delete a company by setting is_deleted = TRUE.
        """
        sql = """
        UPDATE companies 
        SET is_deleted = TRUE, updated_at = CURRENT_TIMESTAMP() 
        WHERE id = %s
        """

        cur = self.conn.cursor()
        try:
            cur.execute(sql, (str(company_id),))
            self.conn.commit()
        finally:
            cur.close()
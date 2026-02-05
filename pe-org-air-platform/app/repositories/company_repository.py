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
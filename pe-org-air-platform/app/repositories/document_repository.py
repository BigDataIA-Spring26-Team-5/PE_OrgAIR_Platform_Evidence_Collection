from typing import List, Dict, Optional
from uuid import uuid4
from datetime import datetime
import logging
from app.services.snowflake import get_snowflake_connection

logger = logging.getLogger(__name__)

class DocumentRepository:
    """Repository for document metadata in Snowflake"""

    def __init__(self):
        self.conn = get_snowflake_connection()

    def create(
        self,
        company_id: str,
        ticker: str,
        filing_type: str,
        filing_date: str,
        source_url: str,
        s3_key: str,
        content_hash: str,
        word_count: int = 0,
        status: str = "uploaded"
    ) -> Dict:
        """Create a new document record"""
        doc_id = str(uuid4())
        
        sql = """
        INSERT INTO documents (
            id, company_id, ticker, filing_type, filing_date,
            source_url, s3_key, content_hash, word_count, status, created_at
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP())
        """
        
        cur = self.conn.cursor()
        try:
            logger.info(f"  💾 Saving document metadata to Snowflake: {ticker}/{filing_type}/{filing_date}")
            cur.execute(sql, (
                doc_id, company_id, ticker, filing_type, filing_date,
                source_url, s3_key, content_hash, word_count, status
            ))
            self.conn.commit()
            logger.info(f"  ✅ Document saved with ID: {doc_id}")
            return self.get_by_id(doc_id)
        except Exception as e:
            logger.error(f"  ❌ Failed to save document: {e}")
            self.conn.rollback()
            raise
        finally:
            cur.close()

    def get_by_id(self, doc_id: str) -> Optional[Dict]:
        """Get document by ID"""
        sql = """
        SELECT id, company_id, ticker, filing_type, filing_date,
               source_url, local_path, s3_key, content_hash,
               word_count, chunk_count, status, error_message,
               created_at, processed_at
        FROM documents WHERE id = %s
        """
        cur = self.conn.cursor()
        try:
            cur.execute(sql, (doc_id,))
            row = cur.fetchone()
            if not row:
                return None
            columns = [col[0].lower() for col in cur.description]
            return dict(zip(columns, row))
        finally:
            cur.close()

    def get_by_ticker(self, ticker: str) -> List[Dict]:
        """Get all documents for a ticker"""
        sql = """
        SELECT id, company_id, ticker, filing_type, filing_date,
               source_url, s3_key, content_hash, word_count, chunk_count,
               status, error_message, created_at, processed_at
        FROM documents 
        WHERE ticker = %s
        ORDER BY filing_date DESC
        """
        cur = self.conn.cursor()
        try:
            cur.execute(sql, (ticker,))
            columns = [col[0].lower() for col in cur.description]
            return [dict(zip(columns, row)) for row in cur.fetchall()]
        finally:
            cur.close()

    def get_by_company_id(self, company_id: str) -> List[Dict]:
        """Get all documents for a company"""
        sql = """
        SELECT id, company_id, ticker, filing_type, filing_date,
               source_url, s3_key, content_hash, word_count, chunk_count,
               status, error_message, created_at, processed_at
        FROM documents 
        WHERE company_id = %s
        ORDER BY filing_date DESC
        """
        cur = self.conn.cursor()
        try:
            cur.execute(sql, (company_id,))
            columns = [col[0].lower() for col in cur.description]
            return [dict(zip(columns, row)) for row in cur.fetchall()]
        finally:
            cur.close()

    def exists_by_hash(self, content_hash: str) -> bool:
        """Check if a document with this hash already exists (deduplication)"""
        sql = "SELECT 1 FROM documents WHERE content_hash = %s"
        cur = self.conn.cursor()
        try:
            cur.execute(sql, (content_hash,))
            return cur.fetchone() is not None
        finally:
            cur.close()

    def exists_by_filing(self, ticker: str, filing_type: str, filing_date: str) -> bool:
        """Check if this specific filing already exists"""
        sql = """
        SELECT 1 FROM documents 
        WHERE ticker = %s AND filing_type = %s AND filing_date = %s
        """
        cur = self.conn.cursor()
        try:
            cur.execute(sql, (ticker, filing_type, filing_date))
            return cur.fetchone() is not None
        finally:
            cur.close()

    def update_status(self, doc_id: str, status: str, error_message: str = None) -> None:
        """Update document status"""
        if error_message:
            sql = """
            UPDATE documents 
            SET status = %s, error_message = %s, processed_at = CURRENT_TIMESTAMP()
            WHERE id = %s
            """
            params = (status, error_message, doc_id)
        else:
            sql = """
            UPDATE documents 
            SET status = %s, processed_at = CURRENT_TIMESTAMP()
            WHERE id = %s
            """
            params = (status, doc_id)
        
        cur = self.conn.cursor()
        try:
            cur.execute(sql, params)
            self.conn.commit()
        finally:
            cur.close()

    def update_chunk_count(self, doc_id: str, chunk_count: int) -> None:
        """Update the chunk count for a document"""
        sql = "UPDATE documents SET chunk_count = %s WHERE id = %s"
        cur = self.conn.cursor()
        try:
            cur.execute(sql, (chunk_count, doc_id))
            self.conn.commit()
        finally:
            cur.close()

    def update_word_count(self, doc_id: str, word_count: int) -> None:
        """Update the word count for a document"""
        sql = "UPDATE documents SET word_count = %s WHERE id = %s"
        cur = self.conn.cursor()
        try:
            cur.execute(sql, (word_count, doc_id))
            self.conn.commit()
        finally:
            cur.close()

    def update_after_parsing(self, doc_id: str, word_count: int, status: str = "parsed") -> None:
        """Update document after parsing"""
        sql = """
        UPDATE documents 
        SET word_count = %s, status = %s, processed_at = CURRENT_TIMESTAMP()
        WHERE id = %s
        """
        cur = self.conn.cursor()
        try:
            cur.execute(sql, (word_count, status, doc_id))
            self.conn.commit()
        finally:
            cur.close()

    def get_all(self, limit: int = 100, offset: int = 0) -> List[Dict]:
        """Get all documents with pagination"""
        sql = """
        SELECT id, company_id, ticker, filing_type, filing_date,
               source_url, s3_key, content_hash, word_count, chunk_count,
               status, error_message, created_at, processed_at
        FROM documents 
        ORDER BY created_at DESC
        LIMIT %s OFFSET %s
        """
        cur = self.conn.cursor()
        try:
            cur.execute(sql, (limit, offset))
            columns = [col[0].lower() for col in cur.description]
            return [dict(zip(columns, row)) for row in cur.fetchall()]
        finally:
            cur.close()

    def count_by_ticker(self, ticker: str) -> Dict[str, int]:
        """Get document counts by filing type for a ticker"""
        sql = """
        SELECT filing_type, COUNT(*) as count
        FROM documents
        WHERE ticker = %s
        GROUP BY filing_type
        """
        cur = self.conn.cursor()
        try:
            cur.execute(sql, (ticker,))
            return {row[0]: row[1] for row in cur.fetchall()}
        finally:
            cur.close()

    def get_company_stats(self, ticker: str) -> Dict:
        """Get detailed stats for a company"""
        sql = """
        SELECT 
            ticker,
            filing_type,
            COUNT(*) as doc_count,
            COALESCE(SUM(chunk_count), 0) as total_chunks,
            COALESCE(SUM(word_count), 0) as total_words
        FROM documents
        WHERE ticker = %s
        GROUP BY ticker, filing_type
        """
        cur = self.conn.cursor()
        try:
            cur.execute(sql, (ticker,))
            rows = cur.fetchall()
            
            stats = {
                "ticker": ticker,
                "form_10k": 0,
                "form_10q": 0,
                "form_8k": 0,
                "def_14a": 0,
                "total": 0,
                "chunks": 0,
                "word_count": 0
            }
            
            for row in rows:
                filing_type = row[1]
                count = row[2]
                chunks = row[3] or 0
                words = row[4] or 0
                
                stats["total"] += count
                stats["chunks"] += chunks
                stats["word_count"] += words
                
                if filing_type == "10-K":
                    stats["form_10k"] = count
                elif filing_type == "10-Q":
                    stats["form_10q"] = count
                elif filing_type == "8-K":
                    stats["form_8k"] = count
                elif filing_type in ["DEF 14A", "DEF14A"]:
                    stats["def_14a"] = count
            
            return stats
        finally:
            cur.close()

    def get_all_company_stats(self) -> List[Dict]:
        """Get stats for all companies"""
        sql = """
        SELECT DISTINCT ticker FROM documents ORDER BY ticker
        """
        cur = self.conn.cursor()
        try:
            cur.execute(sql)
            tickers = [row[0] for row in cur.fetchall()]
        finally:
            cur.close()
        
        return [self.get_company_stats(ticker) for ticker in tickers]

    def get_summary_statistics(self) -> Dict:
        """Get overall summary statistics"""
        sql = """
        SELECT 
            COUNT(DISTINCT ticker) as companies,
            COUNT(*) as total_docs,
            COALESCE(SUM(chunk_count), 0) as total_chunks,
            COALESCE(SUM(word_count), 0) as total_words
        FROM documents
        """
        cur = self.conn.cursor()
        try:
            cur.execute(sql)
            row = cur.fetchone()
            return {
                "companies_processed": row[0] or 0,
                "total_documents": row[1] or 0,
                "total_chunks": row[2] or 0,
                "total_words": row[3] or 0
            }
        finally:
            cur.close()

    def get_status_breakdown(self) -> Dict[str, int]:
        """Get document counts by status"""
        sql = """
        SELECT status, COUNT(*) as count
        FROM documents
        GROUP BY status
        """
        cur = self.conn.cursor()
        try:
            cur.execute(sql)
            return {row[0]: row[1] for row in cur.fetchall()}
        finally:
            cur.close()

    def get_freshness_by_ticker(self) -> List[Dict]:
        """Get last collected and last processed timestamps per ticker."""
        sql = """
        SELECT
            ticker,
            MAX(created_at) as last_collected,
            MAX(processed_at) as last_processed
        FROM documents
        GROUP BY ticker
        ORDER BY ticker
        """
        cur = self.conn.cursor()
        try:
            cur.execute(sql)
            columns = [col[0].lower() for col in cur.description]
            return [dict(zip(columns, row)) for row in cur.fetchall()]
        finally:
            cur.close()

    def delete_by_ticker(self, ticker: str) -> int:
        """Delete all documents for a ticker"""
        sql = "DELETE FROM documents WHERE ticker = %s"
        cur = self.conn.cursor()
        try:
            cur.execute(sql, (ticker,))
            self.conn.commit()
            return cur.rowcount
        finally:
            cur.close()

    def reset_status_by_ticker(self, ticker: str, from_status: str, to_status: str) -> int:
        """Reset document status for a ticker"""
        sql = """
        UPDATE documents 
        SET status = %s, processed_at = NULL 
        WHERE ticker = %s AND status = %s
        """
        cur = self.conn.cursor()
        try:
            cur.execute(sql, (to_status, ticker, from_status))
            self.conn.commit()
            return cur.rowcount
        finally:
            cur.close()

    def reset_chunk_count_by_ticker(self, ticker: str) -> int:
        """Reset chunk_count to NULL for a ticker"""
        sql = "UPDATE documents SET chunk_count = NULL WHERE ticker = %s"
        cur = self.conn.cursor()
        try:
            cur.execute(sql, (ticker,))
            self.conn.commit()
            return cur.rowcount
        finally:
            cur.close()


# Singleton
_repo: Optional[DocumentRepository] = None

def get_document_repository() -> DocumentRepository:
    global _repo
    if _repo is None:
        _repo = DocumentRepository()
    return _repo
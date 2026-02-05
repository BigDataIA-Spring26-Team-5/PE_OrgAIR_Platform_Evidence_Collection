from typing import List, Dict, Optional
from uuid import uuid4
import logging
from app.services.snowflake import get_snowflake_connection

logger = logging.getLogger(__name__)


class ChunkRepository:
    """Repository for document chunk METADATA in Snowflake (content stored in S3)"""

    def __init__(self):
        self.conn = get_snowflake_connection()

    def create(
        self,
        document_id: str,
        chunk_index: int,
        section: Optional[str],
        start_char: int,
        end_char: int,
        word_count: int,
        s3_key: str
    ) -> Dict:
        """Create a single chunk metadata record"""
        chunk_id = str(uuid4())
        
        sql = """
        INSERT INTO document_chunks (
            id, document_id, chunk_index, section,
            start_char, end_char, word_count, s3_key, created_at
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP())
        """
        
        cur = self.conn.cursor()
        try:
            cur.execute(sql, (
                chunk_id, document_id, chunk_index, section,
                start_char, end_char, word_count, s3_key
            ))
            self.conn.commit()
            return {"id": chunk_id, "chunk_index": chunk_index}
        except Exception as e:
            logger.error(f"Failed to save chunk metadata: {e}")
            self.conn.rollback()
            raise
        finally:
            cur.close()

    def create_batch(
        self,
        document_id: str,
        chunks: list,
        s3_key: str
    ) -> int:
        """Batch insert multiple chunk metadata records (MUCH FASTER)"""
        if not chunks:
            return 0
        
        sql = """
        INSERT INTO document_chunks (
            id, document_id, chunk_index, section,
            start_char, end_char, word_count, s3_key, created_at
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP())
        """
        
        # Prepare batch data
        batch_data = []
        for chunk in chunks:
            chunk_id = str(uuid4())
            batch_data.append((
                chunk_id,
                document_id,
                chunk.chunk_index,
                chunk.section,
                chunk.start_char,
                chunk.end_char,
                chunk.word_count,
                s3_key
            ))
        
        cur = self.conn.cursor()
        try:
            cur.executemany(sql, batch_data)
            self.conn.commit()
            return len(batch_data)
        except Exception as e:
            logger.error(f"Failed to batch insert chunks: {e}")
            self.conn.rollback()
            raise
        finally:
            cur.close()

    def get_by_document_id(self, document_id: str) -> List[Dict]:
        """Get all chunk metadata for a document"""
        sql = """
        SELECT id, document_id, chunk_index, section,
               start_char, end_char, word_count, s3_key, created_at
        FROM document_chunks
        WHERE document_id = %s
        ORDER BY chunk_index
        """
        cur = self.conn.cursor()
        try:
            cur.execute(sql, (document_id,))
            columns = [col[0].lower() for col in cur.description]
            return [dict(zip(columns, row)) for row in cur.fetchall()]
        finally:
            cur.close()

    def get_by_id(self, chunk_id: str) -> Optional[Dict]:
        """Get a chunk by ID"""
        sql = """
        SELECT id, document_id, chunk_index, section,
               start_char, end_char, word_count, s3_key, created_at
        FROM document_chunks
        WHERE id = %s
        """
        cur = self.conn.cursor()
        try:
            cur.execute(sql, (chunk_id,))
            row = cur.fetchone()
            if not row:
                return None
            columns = [col[0].lower() for col in cur.description]
            return dict(zip(columns, row))
        finally:
            cur.close()

    def delete_by_document_id(self, document_id: str) -> int:
        """Delete all chunk metadata for a document"""
        sql = "DELETE FROM document_chunks WHERE document_id = %s"
        cur = self.conn.cursor()
        try:
            cur.execute(sql, (document_id,))
            self.conn.commit()
            return cur.rowcount
        finally:
            cur.close()

    def delete_by_ticker(self, ticker: str) -> int:
        """Delete all chunk metadata for a ticker"""
        sql = """
        DELETE FROM document_chunks 
        WHERE document_id IN (SELECT id FROM documents WHERE ticker = %s)
        """
        cur = self.conn.cursor()
        try:
            cur.execute(sql, (ticker,))
            self.conn.commit()
            return cur.rowcount
        finally:
            cur.close()

    def count_by_ticker(self, ticker: str) -> int:
        """Get total chunk count for a ticker"""
        sql = """
        SELECT COUNT(*) 
        FROM document_chunks dc
        JOIN documents d ON dc.document_id = d.id
        WHERE d.ticker = %s
        """
        cur = self.conn.cursor()
        try:
            cur.execute(sql, (ticker,))
            row = cur.fetchone()
            return row[0] if row else 0
        finally:
            cur.close()

    def get_stats_by_ticker(self, ticker: str) -> Dict:
        """Get chunk statistics for a ticker"""
        sql = """
        SELECT 
            d.filing_type,
            COUNT(dc.id) as chunk_count,
            SUM(dc.word_count) as total_words
        FROM document_chunks dc
        JOIN documents d ON dc.document_id = d.id
        WHERE d.ticker = %s
        GROUP BY d.filing_type
        """
        cur = self.conn.cursor()
        try:
            cur.execute(sql, (ticker,))
            results = {}
            for row in cur.fetchall():
                results[row[0]] = {
                    "chunk_count": row[1],
                    "total_words": row[2] or 0
                }
            return results
        finally:
            cur.close()

    def get_total_chunks(self) -> int:
        """Get total number of chunks across all documents"""
        sql = "SELECT COUNT(*) FROM document_chunks"
        cur = self.conn.cursor()
        try:
            cur.execute(sql)
            row = cur.fetchone()
            return row[0] if row else 0
        finally:
            cur.close()


# Singleton
_repo: Optional[ChunkRepository] = None

def get_chunk_repository() -> ChunkRepository:
    global _repo
    if _repo is None:
        _repo = ChunkRepository()
    return _repo
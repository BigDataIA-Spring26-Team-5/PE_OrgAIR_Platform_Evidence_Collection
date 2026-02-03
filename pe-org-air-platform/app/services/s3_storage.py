# # """
# # S3 Storage Service - PE Org-AI-R Platform
# # app/services/s3_storage.py

# # Handles document storage operations with AWS S3.
# # """
# # import boto3
# # from botocore.exceptions import ClientError
# # from typing import Optional, BinaryIO
# # from app.config import settings


# # class S3Storage:
# #     """AWS S3 storage service for document management."""
    
# #     def __init__(self):
# #         self.client = boto3.client(
# #             's3',
# #             aws_access_key_id=settings.AWS_ACCESS_KEY_ID.get_secret_value(),
# #             aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY.get_secret_value(),
# #             region_name=settings.AWS_REGION,
# #         )
# #         self.bucket = settings.S3_BUCKET  # Matches your config.py
    
# #     def upload_document(
# #         self,
# #         file_obj: BinaryIO,
# #         key: str,
# #         content_type: str = "application/octet-stream"
# #     ) -> str:
# #         """
# #         Upload a document to S3.
        
# #         Args:
# #             file_obj: File-like object to upload
# #             key: S3 object key (path/filename)
# #             content_type: MIME type of the document
            
# #         Returns:
# #             S3 URI of the uploaded document
# #         """
# #         self.client.upload_fileobj(
# #             file_obj,
# #             self.bucket,
# #             key,
# #             ExtraArgs={"ContentType": content_type}
# #         )
# #         return f"s3://{self.bucket}/{key}"
    
# #     def download_document(self, key: str) -> Optional[bytes]:
# #         """
# #         Download a document from S3.
        
# #         Args:
# #             key: S3 object key
            
# #         Returns:
# #             Document content as bytes, or None if not found
# #         """
# #         try:
# #             response = self.client.get_object(Bucket=self.bucket, Key=key)
# #             return response['Body'].read()
# #         except ClientError as e:
# #             if e.response['Error']['Code'] == 'NoSuchKey':
# #                 return None
# #             raise
    
# #     def delete_document(self, key: str) -> bool:
# #         """
# #         Delete a document from S3.
        
# #         Args:
# #             key: S3 object key
            
# #         Returns:
# #             True if deleted successfully
# #         """
# #         try:
# #             self.client.delete_object(Bucket=self.bucket, Key=key)
# #             return True
# #         except ClientError:
# #             return False
    
# #     def get_presigned_url(self, key: str, expiration: int = 3600) -> str:
# #         """
# #         Generate a presigned URL for temporary access.
        
# #         Args:
# #             key: S3 object key
# #             expiration: URL expiration time in seconds (default: 1 hour)
            
# #         Returns:
# #             Presigned URL string
# #         """
# #         return self.client.generate_presigned_url(
# #             'get_object',
# #             Params={'Bucket': self.bucket, 'Key': key},
# #             ExpiresIn=expiration
# #         )
    
# #     def document_exists(self, key: str) -> bool:
# #         """Check if a document exists in S3."""
# #         try:
# #             self.client.head_object(Bucket=self.bucket, Key=key)
# #             return True
# #         except ClientError:
# #             return False


# # # Singleton instance
# # _storage: Optional[S3Storage] = None


# # def get_s3_storage() -> Optional[S3Storage]:
# #     """
# #     Get or create S3 storage instance.
    
# #     Returns:
# #         S3Storage instance if AWS is configured, None otherwise.
# #     """
# #     global _storage
# #     if _storage is None:
# #         try:
# #             _storage = S3Storage()
# #         except Exception:
# #             _storage = None
# #     return _storage

# from __future__ import annotations

# import os
# from pathlib import Path
# from typing import Optional

# import boto3


# class S3Storage:
#     def __init__(
#         self,
#         bucket_name: Optional[str] = None,
#         region: Optional[str] = None,
#     ):
#         self.bucket = bucket_name or os.getenv("S3_BUCKET")
#         if not self.bucket:
#             raise ValueError("S3_BUCKET_NAME not set")

#         self.region = region or os.getenv("AWS_REGION", "us-east-2")
#         self.client = boto3.client("s3", region_name=self.region)

#     def upload_file(self, local_path: Path, s3_key: str) -> str:
#         """
#         Upload file to S3. Returns s3_key.
#         """
#         self.client.upload_file(str(local_path), self.bucket, s3_key)
#         return s3_key


"""
Enhanced S3 Storage Service

Handles uploading files to S3 with folder structure:
- raw/          - Original SEC filings (PDF, TXT)
- parsed/       - Parsed document JSONs and content
- tables/       - Extracted tables (JSON)
- chunks/       - Document chunks (JSON)
"""

from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from typing import Optional, Dict, Any, List

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)


class S3Storage:
    """S3 Storage service for uploading SEC filing documents."""

    def __init__(
        self,
        bucket_name: Optional[str] = None,
        region: Optional[str] = None,
    ):
        self.bucket = bucket_name or os.getenv("S3_BUCKET")
        if not self.bucket:
            raise ValueError("S3_BUCKET not set in environment")

        self.region = region or os.getenv("AWS_REGION", "us-east-2")
        
        self.client = boto3.client(
            "s3",
            region_name=self.region,
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        )
        
        logger.info(f"S3Storage initialized: bucket={self.bucket}, region={self.region}")

    # =========================================================================
    # CORE UPLOAD METHODS
    # =========================================================================

    def upload_file(self, local_path: Path, s3_key: str) -> str:
        """Upload a local file to S3."""
        try:
            self.client.upload_file(str(local_path), self.bucket, s3_key)
            logger.info(f"Uploaded: {local_path} -> s3://{self.bucket}/{s3_key}")
            return s3_key
        except ClientError as e:
            logger.error(f"Failed to upload {local_path}: {e}")
            raise

    def upload_json(self, data: Dict[str, Any], s3_key: str) -> str:
        """Upload JSON data directly to S3."""
        try:
            json_bytes = json.dumps(data, indent=2, ensure_ascii=False).encode("utf-8")
            self.client.put_object(Bucket=self.bucket, Key=s3_key, Body=json_bytes, ContentType="application/json")
            logger.info(f"Uploaded JSON: s3://{self.bucket}/{s3_key}")
            return s3_key
        except ClientError as e:
            logger.error(f"Failed to upload JSON: {e}")
            raise

    def upload_text(self, content: str, s3_key: str) -> str:
        """Upload text content directly to S3."""
        try:
            self.client.put_object(Bucket=self.bucket, Key=s3_key, Body=content.encode("utf-8"), ContentType="text/plain")
            logger.info(f"Uploaded text: s3://{self.bucket}/{s3_key}")
            return s3_key
        except ClientError as e:
            logger.error(f"Failed to upload text: {e}")
            raise

    # =========================================================================
    # FOLDER-SPECIFIC UPLOAD METHODS
    # =========================================================================

    def upload_raw_file(self, local_path: Path, ticker: str, filing_type: str, filename: str) -> str:
        """Upload to raw/{ticker}/{filing_type}/{filename}"""
        s3_key = f"raw/{ticker}/{filing_type}/{filename}"
        return self.upload_file(local_path, s3_key)

    def upload_parsed_json(self, data: Dict[str, Any], ticker: str, doc_hash: str) -> str:
        """Upload to parsed/{ticker}/{hash}.json"""
        s3_key = f"parsed/{ticker}/{doc_hash}.json"
        return self.upload_json(data, s3_key)

    def upload_parsed_content(self, content: str, ticker: str, doc_hash: str) -> str:
        """Upload to parsed/{ticker}/{hash}_content.txt"""
        s3_key = f"parsed/{ticker}/{doc_hash}_content.txt"
        return self.upload_text(content, s3_key)

    def upload_tables_json(self, data: Dict[str, Any], ticker: str, doc_hash: str) -> str:
        """Upload to tables/{ticker}/{hash}_tables.json"""
        s3_key = f"tables/{ticker}/{doc_hash}_tables.json"
        return self.upload_json(data, s3_key)

    def upload_chunks_json(self, data: Dict[str, Any], ticker: str, doc_hash: str) -> str:
        """Upload to chunks/{ticker}/{hash}_chunks.json"""
        s3_key = f"chunks/{ticker}/{doc_hash}_chunks.json"
        return self.upload_json(data, s3_key)

    # =========================================================================
    # BATCH UPLOAD - ALL PDF OUTPUTS
    # =========================================================================

    def upload_all_outputs(
        self,
        pdf_path: Path,
        ticker: str,
        filing_type: str,
        doc_hash: str,
        doc_metadata: Dict[str, Any],
        full_content: str,
        tables_data: Optional[Dict[str, Any]],
        chunks_data: Optional[Dict[str, Any]],
    ) -> Dict[str, str]:
        """
        Upload all outputs to S3:
        - raw/{ticker}/{filing_type}/{filename}.pdf
        - parsed/{ticker}/{hash}.json
        - parsed/{ticker}/{hash}_content.txt
        - tables/{ticker}/{hash}_tables.json
        - chunks/{ticker}/{hash}_chunks.json
        """
        s3_keys = {}

        # 1. Raw PDF
        s3_keys["raw_pdf"] = self.upload_raw_file(pdf_path, ticker, filing_type, pdf_path.name)

        # 2. Parsed JSON
        s3_keys["parsed_json"] = self.upload_parsed_json(doc_metadata, ticker, doc_hash)

        # 3. Parsed content TXT
        s3_keys["parsed_content"] = self.upload_parsed_content(full_content, ticker, doc_hash)

        # 4. Tables JSON
        if tables_data and tables_data.get("tables"):
            s3_keys["tables_json"] = self.upload_tables_json(tables_data, ticker, doc_hash)

        # 5. Chunks JSON
        if chunks_data and chunks_data.get("chunks"):
            s3_keys["chunks_json"] = self.upload_chunks_json(chunks_data, ticker, doc_hash)

        logger.info(f"S3 upload complete: {len(s3_keys)} files for {ticker}/{doc_hash}")
        return s3_keys

    # =========================================================================
    # UTILITY METHODS
    # =========================================================================

    def list_files(self, prefix: str) -> List[str]:
        """List all files under a prefix."""
        try:
            response = self.client.list_objects_v2(Bucket=self.bucket, Prefix=prefix)
            return [obj["Key"] for obj in response.get("Contents", [])]
        except ClientError as e:
            logger.error(f"Failed to list files: {e}")
            return []

    def file_exists(self, s3_key: str) -> bool:
        """Check if file exists in S3."""
        try:
            self.client.head_object(Bucket=self.bucket, Key=s3_key)
            return True
        except ClientError:
            return False
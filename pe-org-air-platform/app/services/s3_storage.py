# """
# Enhanced S3 Storage Service

# Handles uploading files to S3 with folder structure:
# - raw/          - Original SEC filings (PDF, TXT)
# - parsed/       - Parsed document JSONs and content
# - json/       - Extracted tables (JSON)
# - chunks/       - Document chunks (JSON)
# """

# from __future__ import annotations

# import json
# import logging
# import os
# from pathlib import Path
# from typing import Optional, Dict, Any, List

# import boto3
# from botocore.exceptions import ClientError

# logger = logging.getLogger(__name__)


# class S3Storage:
#     """S3 Storage service for uploading SEC filing documents."""

#     def __init__(
#         self,
#         bucket_name: Optional[str] = None,
#         region: Optional[str] = None,
#     ):
#         self.bucket = bucket_name or os.getenv("S3_BUCKET")
#         if not self.bucket:
#             raise ValueError("S3_BUCKET not set in environment")

#         self.region = region or os.getenv("AWS_REGION", "us-east-2")
        
#         self.client = boto3.client(
#             "s3",
#             region_name=self.region,
#             aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
#             aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
#         )
        
#         logger.info(f"S3Storage initialized: bucket={self.bucket}, region={self.region}")

#     # =========================================================================
#     # CORE UPLOAD METHODS
#     # =========================================================================

#     def upload_file(self, local_path: Path, s3_key: str) -> str:
#         """Upload a local file to S3."""
#         try:
#             self.client.upload_file(str(local_path), self.bucket, s3_key)
#             logger.info(f"Uploaded: {local_path} -> s3://{self.bucket}/{s3_key}")
#             return s3_key
#         except ClientError as e:
#             logger.error(f"Failed to upload {local_path}: {e}")
#             raise

#     def upload_json(self, data: Dict[str, Any], s3_key: str) -> str:
#         """Upload JSON data directly to S3."""
#         try:
#             json_bytes = json.dumps(data, indent=2, ensure_ascii=False).encode("utf-8")
#             self.client.put_object(Bucket=self.bucket, Key=s3_key, Body=json_bytes, ContentType="application/json")
#             logger.info(f"Uploaded JSON: s3://{self.bucket}/{s3_key}")
#             return s3_key
#         except ClientError as e:
#             logger.error(f"Failed to upload JSON: {e}")
#             raise

#     def upload_text(self, content: str, s3_key: str) -> str:
#         """Upload text content directly to S3."""
#         try:
#             self.client.put_object(Bucket=self.bucket, Key=s3_key, Body=content.encode("utf-8"), ContentType="text/plain")
#             logger.info(f"Uploaded text: s3://{self.bucket}/{s3_key}")
#             return s3_key
#         except ClientError as e:
#             logger.error(f"Failed to upload text: {e}")
#             raise

#     # =========================================================================
#     # FOLDER-SPECIFIC UPLOAD METHODS
#     # =========================================================================

#     def upload_raw_file(self, local_path: Path, ticker: str, filing_type: str, filename: str) -> str:
#         """Upload to raw/{ticker}/{filing_type}/{filename}"""
#         s3_key = f"raw/{ticker}/{filing_type}/{filename}"
#         return self.upload_file(local_path, s3_key)

#     def upload_parsed_json(self, data: Dict[str, Any], ticker: str, doc_hash: str) -> str:
#         """Upload to parsed/{ticker}/{hash}.json"""
#         s3_key = f"parsed/{ticker}/{doc_hash}.json"
#         return self.upload_json(data, s3_key)

#     def upload_parsed_content(self, content: str, ticker: str, doc_hash: str) -> str:
#         """Upload to parsed/{ticker}/{hash}_content.txt"""
#         s3_key = f"parsed/{ticker}/{doc_hash}_content.txt"
#         return self.upload_text(content, s3_key)

#     def upload_tables_json(self, data: Dict[str, Any], ticker: str, doc_hash: str) -> str:
#         """Upload to json/{ticker}/{hash}_tables.json"""
#         s3_key = f"tables/{ticker}/{doc_hash}_tables.json"
#         return self.upload_json(data, s3_key)

#     def upload_chunks_json(self, data: Dict[str, Any], ticker: str, doc_hash: str) -> str:
#         """Upload to chunks/{ticker}/{hash}_chunks.json"""
#         s3_key = f"chunks/{ticker}/{doc_hash}_chunks.json"
#         return self.upload_json(data, s3_key)

#     # =========================================================================
#     # BATCH UPLOAD - ALL PDF OUTPUTS
#     # =========================================================================

#     def upload_all_outputs(
#         self,
#         pdf_path: Path,
#         ticker: str,
#         filing_type: str,
#         doc_hash: str,
#         doc_metadata: Dict[str, Any],
#         full_content: str,
#         tables_data: Optional[Dict[str, Any]],
#         chunks_data: Optional[Dict[str, Any]],
#     ) -> Dict[str, str]:
#         """
#         Upload all outputs to S3:
#         - raw/{ticker}/{filing_type}/{filename}.pdf
#         - parsed/{ticker}/{hash}.json
#         - parsed/{ticker}/{hash}_content.txt
#         - json/{ticker}/{hash}_tables.json
#         - chunks/{ticker}/{hash}_chunks.json
#         """
#         s3_keys = {}

#         # 1. Raw PDF
#         s3_keys["raw_pdf"] = self.upload_raw_file(pdf_path, ticker, filing_type, pdf_path.name)

#         # 2. Parsed JSON
#         s3_keys["parsed_json"] = self.upload_parsed_json(doc_metadata, ticker, doc_hash)

#         # 3. Parsed content TXT
#         s3_keys["parsed_content"] = self.upload_parsed_content(full_content, ticker, doc_hash)

#         # 4. Tables JSON
#         if tables_data and tables_data.get("tables"):
#             s3_keys["tables_json"] = self.upload_tables_json(tables_data, ticker, doc_hash)

#         # 5. Chunks JSON
#         if chunks_data and chunks_data.get("chunks"):
#             s3_keys["chunks_json"] = self.upload_chunks_json(chunks_data, ticker, doc_hash)

#         logger.info(f"S3 upload complete: {len(s3_keys)} files for {ticker}/{doc_hash}")
#         return s3_keys

#     # =========================================================================
#     # UTILITY METHODS
#     # =========================================================================

#     def list_files(self, prefix: str) -> List[str]:
#         """List all files under a prefix."""
#         try:
#             response = self.client.list_objects_v2(Bucket=self.bucket, Prefix=prefix)
#             return [obj["Key"] for obj in response.get("Contents", [])]
#         except ClientError as e:
#             logger.error(f"Failed to list files: {e}")
#             return []

#     def file_exists(self, s3_key: str) -> bool:
#         """Check if file exists in S3."""
#         try:
#             self.client.head_object(Bucket=self.bucket, Key=s3_key)
#             return True
#         except ClientError:
#             return False


# import boto3
# import hashlib
# import logging
# from typing import Optional, Tuple
# from botocore.exceptions import ClientError
# from app.config import settings

# logger = logging.getLogger(__name__)

# class S3StorageService:
#     def __init__(self):
#         self.s3_client = boto3.client(
#             's3',
#             aws_access_key_id=settings.AWS_ACCESS_KEY_ID.get_secret_value(),
#             aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY.get_secret_value(),
#             region_name=settings.AWS_REGION
#         )
#         self.bucket_name = settings.S3_BUCKET
#         logger.info(f"S3 Storage initialized with bucket: {self.bucket_name}")

#     def _generate_s3_key(self, ticker: str, filing_type: str, filing_date: str, filename: str, accession_number: str = "") -> str:
#         """
#         Generate S3 key path: sec/raw/{ticker}/{filing_type}/{filing_date}_{accession}.html
        
#         Structure:
#         sec/
#         └── raw/
#             └── {ticker}/
#                 └── {filing_type}/
#                     └── {filing_date}_{accession_number}.html
#         """
#         # Clean filing type: "10-K" -> "10-K", "DEF 14A" -> "DEF14A"
#         clean_filing_type = filing_type.replace(" ", "")
        
#         # Build filename with date and accession number
#         if accession_number:
#             clean_accession = accession_number.replace("-", "")
#             doc_filename = f"{filing_date}_{clean_accession}.html"
#         else:
#             # Fallback to original filename
#             doc_filename = f"{filing_date}_{filename}"
        
#         return f"sec/raw/{ticker}/{clean_filing_type}/{doc_filename}"

#     def _calculate_hash(self, content: bytes) -> str:
#         """Calculate SHA256 hash of content"""
#         return hashlib.sha256(content).hexdigest()

#     def upload_filing(
#         self,
#         ticker: str,
#         filing_type: str,
#         filing_date: str,
#         filename: str,
#         content: bytes,
#         content_type: str = "text/html",
#         accession_number: str = ""
#     ) -> Tuple[str, str]:
#         """
#         Upload a filing to S3.
        
#         S3 Path: sec/raw/{ticker}/{filing_type}/{filing_date}_{accession}.html
        
#         Returns: (s3_key, content_hash)
#         """
#         s3_key = self._generate_s3_key(ticker, filing_type, filing_date, filename, accession_number)
#         content_hash = self._calculate_hash(content)
        
#         logger.info(f"  📤 Uploading to S3: {s3_key}")
        
#         try:
#             self.s3_client.put_object(
#                 Bucket=self.bucket_name,
#                 Key=s3_key,
#                 Body=content,
#                 ContentType=content_type,
#                 Metadata={
#                     'ticker': ticker,
#                     'filing_type': filing_type,
#                     'filing_date': filing_date,
#                     'content_hash': content_hash
#                 }
#             )
#             logger.info(f"  ✅ Upload successful: {s3_key}")
#             return s3_key, content_hash
#         except ClientError as e:
#             logger.error(f"  ❌ S3 upload failed: {e}")
#             raise

#     def check_exists(self, s3_key: str) -> bool:
#         """Check if a file exists in S3"""
#         try:
#             self.s3_client.head_object(Bucket=self.bucket_name, Key=s3_key)
#             return True
#         except ClientError:
#             return False

#     def get_file(self, s3_key: str) -> Optional[bytes]:
#         """Download a file from S3"""
#         try:
#             response = self.s3_client.get_object(Bucket=self.bucket_name, Key=s3_key)
#             return response['Body'].read()
#         except ClientError as e:
#             logger.error(f"Failed to get file from S3: {e}")
#             return None

#     def delete_file(self, s3_key: str) -> bool:
#         """Delete a file from S3"""
#         try:
#             self.s3_client.delete_object(Bucket=self.bucket_name, Key=s3_key)
#             return True
#         except ClientError as e:
#             logger.error(f"Failed to delete file from S3: {e}")
#             return False

#     def list_files(self, prefix: str) -> list:
#         """List files in S3 with given prefix"""
#         try:
#             response = self.s3_client.list_objects_v2(
#                 Bucket=self.bucket_name,
#                 Prefix=prefix
#             )
#             return [obj['Key'] for obj in response.get('Contents', [])]
#         except ClientError as e:
#             logger.error(f"Failed to list S3 files: {e}")
#             return []


# # Singleton instance
# _s3_service: Optional[S3StorageService] = None

# def get_s3_service() -> S3StorageService:
#     global _s3_service
#     if _s3_service is None:
#         _s3_service = S3StorageService()
#     return _s3_service


import boto3
import hashlib
import logging
from typing import Optional, Tuple
from botocore.exceptions import ClientError
from app.config import settings

logger = logging.getLogger(__name__)

class S3StorageService:
    def __init__(self):
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=settings.AWS_ACCESS_KEY_ID.get_secret_value(),
            aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY.get_secret_value(),
            region_name=settings.AWS_REGION
        )
        self.bucket_name = settings.S3_BUCKET
        logger.info(f"S3 Storage initialized with bucket: {self.bucket_name}")

    def _generate_s3_key(self, ticker: str, filing_type: str, filing_date: str, filename: str, accession_number: str = "") -> str:
        """
        Generate S3 key path.
        
        For raw files: sec/raw/{ticker}/{filing_type}/{filing_date}_{accession}.html
        For parsed files: sec/parsed/{ticker}/{filing_type}/{filing_date}_{filename}
        """
        # Check if this is for parsed content
        if filing_type.startswith("parsed/"):
            # sec/parsed/{ticker}/{filing_type}/{filing_date}_{filename}
            actual_filing_type = filing_type.replace("parsed/", "")
            return f"sec/parsed/{ticker}/{actual_filing_type}/{filing_date}_{filename}"
        
        # Raw files: sec/raw/{ticker}/{filing_type}/{filing_date}_{accession}.html
        clean_filing_type = filing_type.replace(" ", "")
        
        if accession_number:
            clean_accession = accession_number.replace("-", "")
            doc_filename = f"{filing_date}_{clean_accession}.html"
        else:
            doc_filename = f"{filing_date}_{filename}"
        
        return f"sec/raw/{ticker}/{clean_filing_type}/{doc_filename}"

    def _calculate_hash(self, content: bytes) -> str:
        """Calculate SHA256 hash of content"""
        return hashlib.sha256(content).hexdigest()

    def upload_filing(
        self,
        ticker: str,
        filing_type: str,
        filing_date: str,
        filename: str,
        content: bytes,
        content_type: str = "text/html",
        accession_number: str = ""
    ) -> Tuple[str, str]:
        """
        Upload a filing to S3.
        
        S3 Path: sec/raw/{ticker}/{filing_type}/{filing_date}_{accession}.html
        
        Returns: (s3_key, content_hash)
        """
        s3_key = self._generate_s3_key(ticker, filing_type, filing_date, filename, accession_number)
        content_hash = self._calculate_hash(content)
        
        logger.info(f"  📤 Uploading to S3: {s3_key}")
        
        try:
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=s3_key,
                Body=content,
                ContentType=content_type,
                Metadata={
                    'ticker': ticker,
                    'filing_type': filing_type,
                    'filing_date': filing_date,
                    'content_hash': content_hash
                }
            )
            logger.info(f"  ✅ Upload successful: {s3_key}")
            return s3_key, content_hash
        except ClientError as e:
            logger.error(f"  ❌ S3 upload failed: {e}")
            raise

    def check_exists(self, s3_key: str) -> bool:
        """Check if a file exists in S3"""
        try:
            self.s3_client.head_object(Bucket=self.bucket_name, Key=s3_key)
            return True
        except ClientError:
            return False

    def get_file(self, s3_key: str) -> Optional[bytes]:
        """Download a file from S3"""
        try:
            response = self.s3_client.get_object(Bucket=self.bucket_name, Key=s3_key)
            return response['Body'].read()
        except ClientError as e:
            logger.error(f"Failed to get file from S3: {e}")
            return None

    def delete_file(self, s3_key: str) -> bool:
        """Delete a file from S3"""
        try:
            self.s3_client.delete_object(Bucket=self.bucket_name, Key=s3_key)
            return True
        except ClientError as e:
            logger.error(f"Failed to delete file from S3: {e}")
            return False

    def list_files(self, prefix: str) -> list:
        """List files in S3 with given prefix"""
        try:
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=prefix
            )
            return [obj['Key'] for obj in response.get('Contents', [])]
        except ClientError as e:
            logger.error(f"Failed to list S3 files: {e}")
            return []

    def upload_content(self, content: str, s3_key: str, content_type: str = "application/json") -> str:
        """
        Upload string content directly to S3.

        Args:
            content: String content to upload (JSON, text, etc.)
            s3_key: Full S3 key path
            content_type: MIME type of the content

        Returns:
            s3_key on success
        """
        try:
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=s3_key,
                Body=content.encode('utf-8'),
                ContentType=content_type
            )
            logger.info(f"  ✅ Uploaded to S3: {s3_key}")
            return s3_key
        except ClientError as e:
            logger.error(f"  ❌ S3 upload failed: {e}")
            raise

    def upload_json(self, data: dict, s3_key: str) -> str:
        """
        Upload a dictionary as JSON to S3.

        Args:
            data: Dictionary to serialize as JSON
            s3_key: Full S3 key path

        Returns:
            s3_key on success
        """
        import json
        content = json.dumps(data, indent=2, default=str)
        return self.upload_content(content, s3_key, content_type="application/json")


# Singleton instance
_s3_service: Optional[S3StorageService] = None

def get_s3_service() -> S3StorageService:
    global _s3_service
    if _s3_service is None:
        _s3_service = S3StorageService()
    return _s3_service
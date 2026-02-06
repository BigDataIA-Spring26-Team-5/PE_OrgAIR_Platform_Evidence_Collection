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

    def store_signal_data(
        self,
        signal_type: str,
        ticker: str,
        data: dict,
        timestamp: str = None
    ) -> str:
        """
        Store signal data to S3 with standardized path structure.

        Common method for all signal types to ensure consistent storage patterns.

        Args:
            signal_type: Type of signal ('jobs', 'patents', 'techstack')
            ticker: Company ticker symbol
            data: Dictionary containing signal data
            timestamp: Optional timestamp string (defaults to current UTC time)

        Returns:
            s3_key on success

        S3 Path Structure:
            signals/{signal_type}/{ticker}/{timestamp}.json

        Examples:
            signals/jobs/AAPL/20240115_143052.json
            signals/patents/MSFT/20240115_143052.json
            signals/techstack/GOOGL/20240115_143052.json
        """
        from datetime import datetime, timezone

        if timestamp is None:
            timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

        ticker = ticker.upper()
        s3_key = f"signals/{signal_type}/{ticker}/{timestamp}.json"

        logger.info(f"  💾 Storing {signal_type} data to S3: {s3_key}")

        return self.upload_json(data, s3_key)


# Singleton instance
_s3_service: Optional[S3StorageService] = None

def get_s3_service() -> S3StorageService:
    global _s3_service
    if _s3_service is None:
        _s3_service = S3StorageService()
    return _s3_service
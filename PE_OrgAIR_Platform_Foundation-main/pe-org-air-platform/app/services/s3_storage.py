"""
S3 Storage Service - PE Org-AI-R Platform
app/services/s3_storage.py

Handles document storage operations with AWS S3.
"""
import boto3
from botocore.exceptions import ClientError
from typing import Optional, BinaryIO
from app.config import settings


class S3Storage:
    """AWS S3 storage service for document management."""
    
    def __init__(self):
        self.client = boto3.client(
            's3',
            aws_access_key_id=settings.AWS_ACCESS_KEY_ID.get_secret_value(),
            aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY.get_secret_value(),
            region_name=settings.AWS_REGION,
        )
        self.bucket = settings.S3_BUCKET  # Matches your config.py
    
    def upload_document(
        self,
        file_obj: BinaryIO,
        key: str,
        content_type: str = "application/octet-stream"
    ) -> str:
        """
        Upload a document to S3.
        
        Args:
            file_obj: File-like object to upload
            key: S3 object key (path/filename)
            content_type: MIME type of the document
            
        Returns:
            S3 URI of the uploaded document
        """
        self.client.upload_fileobj(
            file_obj,
            self.bucket,
            key,
            ExtraArgs={"ContentType": content_type}
        )
        return f"s3://{self.bucket}/{key}"
    
    def download_document(self, key: str) -> Optional[bytes]:
        """
        Download a document from S3.
        
        Args:
            key: S3 object key
            
        Returns:
            Document content as bytes, or None if not found
        """
        try:
            response = self.client.get_object(Bucket=self.bucket, Key=key)
            return response['Body'].read()
        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchKey':
                return None
            raise
    
    def delete_document(self, key: str) -> bool:
        """
        Delete a document from S3.
        
        Args:
            key: S3 object key
            
        Returns:
            True if deleted successfully
        """
        try:
            self.client.delete_object(Bucket=self.bucket, Key=key)
            return True
        except ClientError:
            return False
    
    def get_presigned_url(self, key: str, expiration: int = 3600) -> str:
        """
        Generate a presigned URL for temporary access.
        
        Args:
            key: S3 object key
            expiration: URL expiration time in seconds (default: 1 hour)
            
        Returns:
            Presigned URL string
        """
        return self.client.generate_presigned_url(
            'get_object',
            Params={'Bucket': self.bucket, 'Key': key},
            ExpiresIn=expiration
        )
    
    def document_exists(self, key: str) -> bool:
        """Check if a document exists in S3."""
        try:
            self.client.head_object(Bucket=self.bucket, Key=key)
            return True
        except ClientError:
            return False


# Singleton instance
_storage: Optional[S3Storage] = None


def get_s3_storage() -> Optional[S3Storage]:
    """
    Get or create S3 storage instance.
    
    Returns:
        S3Storage instance if AWS is configured, None otherwise.
    """
    global _storage
    if _storage is None:
        try:
            _storage = S3Storage()
        except Exception:
            _storage = None
    return _storage
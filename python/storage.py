"""Abstract storage interface for different storage backends"""
from abc import ABC, abstractmethod
from typing import List, Dict, Any
import json
import os


class Storage(ABC):
    """Abstract base class for storage backends"""
    
    @abstractmethod
    def upload_documents(self, documents: List[Dict[str, Any]], filename: str) -> bool:
        """Upload documents to storage backend"""
        pass
    
    @abstractmethod
    def is_available(self) -> bool:
        """Check if storage backend is available"""
        pass


class MinIOStorage(Storage):
    """MinIO/S3-compatible storage backend"""
    
    def __init__(self):
        import boto3
        from botocore.config import Config
        
        self.endpoint = os.getenv("MINIO_ENDPOINT", "http://localhost:9002")
        self.access_key = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
        self.secret_key = os.getenv("MINIO_SECRET_KEY", "minioadmin")
        self.bucket_name = os.getenv("BUCKET_NAME", "extracteddata")
        
        self.client = boto3.client(
            's3',
            endpoint_url=self.endpoint,
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key,
            config=Config(
                signature_version='s3v4',
                connect_timeout=60,
                read_timeout=60,
                retries={'max_attempts': 3}
            )
        )
        
        # Create bucket if it doesn't exist
        try:
            self.client.head_bucket(Bucket=self.bucket_name)
        except:
            self.client.create_bucket(Bucket=self.bucket_name)
    
    def upload_documents(self, documents: List[Dict[str, Any]], filename: str) -> bool:
        """Upload documents as JSON Lines format"""
        try:
            content = "\n".join(json.dumps(doc) for doc in documents)
            self.client.put_object(
                Bucket=self.bucket_name,
                Key=filename,
                Body=content.encode('utf-8'),
                ContentType='application/x-ndjson'
            )
            return True
        except Exception as e:
            import logging
            logging.getLogger(__name__).error(f"Failed to upload to MinIO: {e}")
            return False
    
    def is_available(self) -> bool:
        """Check if MinIO is available"""
        try:
            self.client.head_bucket(Bucket=self.bucket_name)
            return True
        except:
            return False


def get_storage() -> Storage:
    """Factory function to get storage backend based on configuration"""
    storage_type = os.getenv("STORAGE_TYPE", "minio").lower()
    
    if storage_type == "minio" or storage_type == "s3":
        return MinIOStorage()
    else:
        raise ValueError(f"Unknown storage type: {storage_type}. Use 'minio' or 's3'")


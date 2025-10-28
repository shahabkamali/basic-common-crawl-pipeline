from abc import ABC, abstractmethod
import os
import gzip
import json
import time
import random
import tempfile
import boto3
from botocore.config import Config
from typing import Optional, Dict


class StorageWriter(ABC):

    @abstractmethod
    def write_jsonl_sharded(self, date_prefix: str, obj: dict) -> bool:
        pass


class ObjectStoreWriter(StorageWriter):
    def __init__(
        self,
        endpoint_url: Optional[str] = None,
        access_key: Optional[str] = None,
        secret_key: Optional[str] = None,
        bucket_name: str = "extracted-documents",
    ):
        self.endpoint_url = endpoint_url or os.getenv("MINIO_ENDPOINT", "http://localhost:9002")
        self.access_key = access_key or os.getenv("MINIO_ACCESS_KEY", "minioadmin")
        self.secret_key = secret_key or os.getenv("MINIO_SECRET_KEY", "minioadmin")
        self.bucket_name = bucket_name or os.getenv("MINIO_BUCKET", "extracted-documents")
        
        self.s3_client = boto3.client(
            's3',
            endpoint_url=self.endpoint_url,
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key,
            config=Config(retries={"max_attempts": 5, "mode": "adaptive"})
        )
        
        self._ensure_bucket_exists()

        # Rollover thresholds (uncompressed)
        self.max_shard_bytes = int(os.getenv("MAX_SHARD_BYTES", str(128 * 1024 * 1024)))  # 128 MB
        self.max_shard_docs = int(os.getenv("MAX_SHARD_DOCS", "50000"))

        # Per-day shard buffers: date_prefix -> { 'lines': List[str], 'bytes': int, 'count': int }
        self._day_buffers: Dict[str, dict] = {}
    
    def _ensure_bucket_exists(self):
        try:
            self.s3_client.head_bucket(Bucket=self.bucket_name)
        except:
            try:
                self.s3_client.create_bucket(Bucket=self.bucket_name)
            except:
                pass

    def _flush_day_shard(self, date_prefix: str) -> bool:
        buf = self._day_buffers.get(date_prefix)
        if not buf or not buf['lines']:
            return True

        # Unique shard name to avoid collision after restarts
        timestamp_ms = int(time.time() * 1000)
        rand = random.getrandbits(32)
        key = f"documents/{date_prefix}/shard-{timestamp_ms}-{rand:08x}.jsonl.gz"

        try:
            # Stream-compress using spooled temp file to limit RAM usage
            with tempfile.SpooledTemporaryFile(max_size=8 * 1024 * 1024) as tmp:
                with gzip.GzipFile(fileobj=tmp, mode='wb') as gz:
                    for line in buf['lines']:
                        gz.write(line.encode('utf-8'))
                tmp.seek(0)
                self.s3_client.put_object(
                    Bucket=self.bucket_name,
                    Key=key,
                    Body=tmp.read(),
                    ContentType='application/x-ndjson',
                    ContentEncoding='gzip'
                )

            # Reset buffer for next shard
            buf['lines'] = []
            buf['bytes'] = 0
            buf['count'] = 0
            return True
        except Exception as e:
            print(f"Error flushing shard {key}: {e}")
            return False

    def write_jsonl_sharded(self, date_prefix: str, obj: dict) -> bool:
        # Ensure buffer exists
        if date_prefix not in self._day_buffers:
            self._day_buffers[date_prefix] = {'lines': [], 'bytes': 0, 'count': 0}

        line = json.dumps(obj, ensure_ascii=False) + "\n"

        buf = self._day_buffers[date_prefix]
        buf['lines'].append(line)
        buf['bytes'] += len(line.encode('utf-8'))
        buf['count'] += 1

        # Rollover if thresholds reached
        if buf['bytes'] >= self.max_shard_bytes or buf['count'] >= self.max_shard_docs:
            return self._flush_day_shard(date_prefix)

        return True

    def flush_all(self) -> bool:
        """Flush all pending buffers. Call on shutdown."""
        ok = True
        for date_prefix in list(self._day_buffers.keys()):
            ok = self._flush_day_shard(date_prefix) and ok
        return ok


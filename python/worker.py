import io
import json
import os
import hashlib
from prometheus_client import start_http_server
import trafilatura
from warcio.archiveiterator import WARCIterator
from prometheus_client import Counter
from langdetect import detect, LangDetectException

from commoncrawl import BASE_URL, CCDownloader, Downloader
from rabbitmq import QUEUE_NAME, rabbitmq_channel
from storage import ObjectStoreWriter


# Prometheus counters
batch_counter = Counter("worker_batches", "Number of consumed batches")
document_counter = Counter("worker_documents", "Number of documents processed")
records_processed_counter = Counter("worker_records_processed", "Number of WARC records processed")
extraction_success_counter = Counter("worker_extraction_success", "Documents with successful text extraction")
extraction_failed_counter = Counter("worker_extraction_failed", "Documents with failed text extraction")
written_to_store_counter = Counter("worker_written_to_store", "Documents successfully written to object store")
filtered_too_short_counter = Counter("worker_filtered_too_short", "Documents filtered because too short (<500 chars)")
filtered_too_long_counter = Counter("worker_filtered_too_long", "Documents filtered because too long (>1M chars)")
filtered_non_english_counter = Counter("worker_filtered_non_english", "Documents filtered because language is not English")


def passes_filters(text: str, min_length: int, max_length: int) -> tuple[bool, str, int]:
    """Apply length first, then language. Returns (ok, reason, length)."""
    if not text:
        return False, "empty", 0
    length = len(text)
    if length < min_length:
        return False, "too_short", length
    if length > max_length:
        return False, "too_long", length

    try:
        lang = detect(text[:2000])
    except LangDetectException:
        return False, "lang_unknown", length
    if lang != "en":
        return False, "non_english", length
    return True, "ok", length


def process_batch(downloader: Downloader, storage_writer: ObjectStoreWriter, ch, method, _properties, body):
    print("Received batch of size", len(body))
    batch = json.loads(body)
    
    # Get document length filters from environment with defaults
    min_length = int(os.getenv("MIN_DOCUMENT_LENGTH", "500"))
    max_length = int(os.getenv("MAX_DOCUMENT_LENGTH", "1000000"))
    
    for item in batch:
        document_counter.inc()
        
        try:
            data = downloader.download_and_unzip(
                item["metadata"]["filename"],
                int(item["metadata"]["offset"]),
                int(item["metadata"]["length"]),
            )
            
            for record in WARCIterator(io.BytesIO(data)):
                records_processed_counter.inc()
                
                if record.rec_type == "response":
                    try:
                        text = trafilatura.extract(record.content_stream().read())
                        ok, reason, text_length = passes_filters(text, min_length, max_length)
                        if not ok:
                            if reason == "non_english" or reason == "lang_unknown":
                                filtered_non_english_counter.inc()
                            elif reason == "too_short" or reason == "empty":
                                filtered_too_short_counter.inc()
                            elif reason == "too_long":
                                filtered_too_long_counter.inc()
                            continue

                        if ok:
                            extraction_success_counter.inc()
                            
                            # Create document structure
                            url = item.get("surt_url", "")
                            timestamp = item.get("timestamp", "")
                            document = {
                                "url": url,
                                "timestamp": timestamp,
                                "text": text,
                                "metadata": item.get("metadata", {}),
                                "text_length": text_length
                            }

                            timestamp_clean = timestamp.replace(":", "")
                            date_prefix = timestamp_clean[:8]
                            
                            # Write into sharded JSONL (gz) for that day
                            success = storage_writer.write_jsonl_sharded(
                                date_prefix=date_prefix,
                                obj=document
                            )
                            
                            if success:
                                written_to_store_counter.inc()
                    except Exception as e:
                        extraction_failed_counter.inc()
                        print(f"Extraction error: {e}")
        except Exception as e:
            print(f"Download error: {e}")
            # Continue processing other documents in the batch
    
    # Ensure buffers are flushed after processing this batch
    try:
        storage_writer.flush_all()
    except Exception as _e:
        pass
    batch_counter.inc()
    ch.basic_ack(delivery_tag=method.delivery_tag)


def main() -> None:
    start_http_server(9001)
    downloader = CCDownloader(BASE_URL)
    
    # Initialize object store writer
    storage_writer = ObjectStoreWriter(
        endpoint_url=os.getenv("MINIO_ENDPOINT", "http://localhost:9002"),
        access_key=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
        secret_key=os.getenv("MINIO_SECRET_KEY", "minioadmin"),
        bucket_name=os.getenv("MINIO_BUCKET", "extracted-documents")
    )
    
    channel = rabbitmq_channel()
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(
        queue=QUEUE_NAME,
        on_message_callback=lambda ch, method, properties, body: process_batch(
            downloader, storage_writer, ch, method, properties, body
        ),
    )
    channel.start_consuming()


if __name__ == "__main__":
    main()

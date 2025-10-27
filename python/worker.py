import io
import json
import logging
import os
from datetime import datetime
from prometheus_client import start_http_server
import trafilatura
from warcio.archiveiterator import WARCIterator
from prometheus_client import Counter

from commoncrawl import BASE_URL, CCDownloader, Downloader
from storage import get_storage
from rabbitmq import QUEUE_NAME, rabbitmq_channel

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

batch_counter = Counter("worker_batches", "Number of consumed batches")
urls_processed_counter = Counter("worker_urls_processed", "Total URLs processed")
warc_records_counter = Counter("worker_warc_records", "WARC records processed")
text_extraction_attempts = Counter("worker_text_extraction_attempts", "Text extraction attempts")
text_extraction_success = Counter("worker_text_extraction_success", "Successfully extracted text")
text_length_filtered = Counter("worker_text_length_filtered", "Text filtered by length constraints")
uploads_to_storage = Counter("worker_uploads_to_storage", "Documents uploaded to storage")


def process_batch(downloader: Downloader, storage, ch, method, _properties, body):
    logger.info("Received batch of size %d", len(body))
    batch = json.loads(body)
    
    min_text_length = int(os.getenv("MIN_TEXT_LENGTH", "500"))
    max_text_length = int(os.getenv("MAX_TEXT_LENGTH", "1000000"))
    
    documents_to_upload = []
    
    for item in batch:
        urls_processed_counter.inc()
        data = downloader.download_and_unzip(
            item["metadata"]["filename"],
            int(item["metadata"]["offset"]),
            int(item["metadata"]["length"]),
        )
        for record in WARCIterator(io.BytesIO(data)):
            warc_records_counter.inc()
            if record.rec_type == "response":
                text_extraction_attempts.inc()
                text = trafilatura.extract(record.content_stream().read())
                if text:
                    text_extraction_success.inc()
                    # Apply length filter
                    if min_text_length <= len(text) <= max_text_length:
                        # Store document with metadata
                        document = {
                            "url": item.get("surt_url", ""),
                            "timestamp": item.get("timestamp", ""),
                            "text": text,
                            "text_length": len(text),
                            "processed_at": datetime.utcnow().isoformat(),
                            "metadata": item.get("metadata", {})
                        }
                        documents_to_upload.append(document)
                    else:
                        text_length_filtered.inc()
    
    # Upload documents to storage as JSON Lines (JSONL) format
    if documents_to_upload and storage and storage.is_available():
        batch_timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        filename = f"documents/batch_{batch_timestamp}.jsonl"
        
        if storage.upload_documents(documents_to_upload, filename):
            uploads_to_storage.inc(len(documents_to_upload))
            logger.info(f"Uploaded {len(documents_to_upload)} documents to storage: {filename}")
    
    batch_counter.inc()
    logger.info("Processed batch successfully")
    ch.basic_ack(delivery_tag=method.delivery_tag)


def main() -> None:
    prometheus_port = int(os.getenv("WORKER_METRICS_PORT", os.getenv("PROMETHEUS_PORT", "9001")))
    start_http_server(prometheus_port)
    logger.info("Started worker metrics server on port %d", prometheus_port)
    
    # Initialize storage backend
    try:
        storage = get_storage()
        if storage.is_available():
            logger.info(f"Storage available: {type(storage).__name__}")
        else:
            logger.warning("Storage backend not available")
            storage = None
    except Exception as e:
        logger.warning(f"Could not initialize storage: {e}")
        storage = None
    
    downloader = CCDownloader(BASE_URL)
    logger.info("Connecting to RabbitMQ queue: %s", QUEUE_NAME)
    channel = rabbitmq_channel()
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(
        queue=QUEUE_NAME,
        on_message_callback=lambda ch, method, properties, body: process_batch(
            downloader, storage, ch, method, properties, body
        ),
    )
    logger.info("Worker started, waiting for messages...")
    channel.start_consuming()


if __name__ == "__main__":
    main()

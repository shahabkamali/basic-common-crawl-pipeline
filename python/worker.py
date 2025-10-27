import io
import json
from prometheus_client import start_http_server
import trafilatura
from warcio.archiveiterator import WARCIterator
from prometheus_client import Counter

from commoncrawl import BASE_URL, CCDownloader, Downloader
from rabbitmq import QUEUE_NAME, rabbitmq_channel


# Prometheus counters
batch_counter = Counter("worker_batches", "Number of consumed batches")
document_counter = Counter("worker_documents", "Number of documents processed")
records_processed_counter = Counter("worker_records_processed", "Number of WARC records processed")
extraction_success_counter = Counter("worker_extraction_success", "Documents with successful text extraction")
extraction_failed_counter = Counter("worker_extraction_failed", "Documents with failed text extraction")


def process_batch(downloader: Downloader, ch, method, _properties, body):
    print("Received batch of size", len(body))
    batch = json.loads(body)
    
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
                        if text:
                            extraction_success_counter.inc()
                        else:
                            extraction_failed_counter.inc()
                    except Exception as e:
                        extraction_failed_counter.inc()
                        print(f"Extraction error: {e}")
        except Exception as e:
            print(f"Download error: {e}")
            # Continue processing other documents in the batch
    
    batch_counter.inc()
    ch.basic_ack(delivery_tag=method.delivery_tag)


def main() -> None:
    start_http_server(9001)
    downloader = CCDownloader(BASE_URL)
    channel = rabbitmq_channel()
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(
        queue=QUEUE_NAME,
        on_message_callback=lambda ch, method, properties, body: process_batch(
            downloader, ch, method, properties, body
        ),
    )
    channel.start_consuming()


if __name__ == "__main__":
    main()

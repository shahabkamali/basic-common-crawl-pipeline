from abc import ABC, abstractmethod
import json
import argparse
from typing import Any, Mapping, Sequence
from prometheus_client import Counter, start_http_server

from commoncrawl import (
    BASE_URL,
    CRAWL_PATH,
    CCDownloader,
    CSVIndexReader,
    Downloader,
    IndexReader,
)
from rabbitmq import QUEUE_NAME, MessageQueueChannel, RabbitMQChannel


BATCH_SIZE = 50

# Prometheus counters
batch_counter = Counter("batcher_batches", "Number of published batches")
total_documents_counter = Counter("batcher_total_documents", "Total documents scanned")
filtered_by_language_counter = Counter("batcher_filtered_language", "Documents filtered by language")
filtered_by_status_counter = Counter("batcher_filtered_status", "Documents filtered by status")
filtered_by_missing_info_counter = Counter("batcher_filtered_missing_info", "Documents filtered by missing language info")
passed_filters_counter = Counter("batcher_passed_filters", "Documents that passed all filters")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Batcher")
    parser.add_argument(
        "--cluster-idx-filename", type=str, help="Input file path", required=True
    )
    return parser.parse_args()


def publish_batch(
    channel: MessageQueueChannel,
    batch: Sequence[Mapping[str, Any]],
) -> None:
    print("Pushing batch of size", len(batch))
    channel.basic_publish(
        exchange="",
        routing_key=QUEUE_NAME,
        body=json.dumps(batch),
    )
    batch_counter.inc()


def process_index(
    index: IndexReader,
    channel: MessageQueueChannel,
    downloader: Downloader,
    batch_size: int,
) -> None:
    found_urls = []
    for cdx_chunk in index:
        data = downloader.download_and_unzip(
            url=cdx_chunk[1], start=int(cdx_chunk[2]), length=int(cdx_chunk[3])
        ).decode("utf-8")
        for line in data.split("\n"):
            if line == "":
                continue
            values = line.split(" ")
            metadata = json.loads("".join(values[2:]))
            
            total_documents_counter.inc()
            
            # Track filtering at each stage
            if "languages" not in metadata:
                filtered_by_missing_info_counter.inc()
                continue
            
            if "eng" not in metadata["languages"]:
                filtered_by_language_counter.inc()
                continue
            
            if metadata.get("status") != "200":
                filtered_by_status_counter.inc()
                continue
            
            # Document passed all filters
            passed_filters_counter.inc()
            found_urls.append(
                {
                    "surt_url": values[0],
                    "timestamp": values[1],
                    "metadata": metadata,
                }
            )
            if len(found_urls) >= batch_size:
                publish_batch(channel, found_urls)
                found_urls = []

    if len(found_urls) > 0:
        publish_batch(channel, found_urls)


def main() -> None:
    args = parse_args()
    start_http_server(9000)
    channel = RabbitMQChannel()
    downloader = CCDownloader(f"{BASE_URL}/{CRAWL_PATH}")
    index_reader = CSVIndexReader(args.cluster_idx_filename)
    process_index(index_reader, channel, downloader, BATCH_SIZE)


if __name__ == "__main__":
    main()

from batcher import (
    process_index,
    total_documents_counter,
    filtered_by_language_counter,
    filtered_by_status_counter,
    filtered_by_missing_info_counter,
    passed_filters_counter,
    batch_counter,
)
from commoncrawl import Downloader, IndexReader
from rabbitmq import MessageQueueChannel


def reset_counters():
    """Reset all Prometheus counters for testing"""
    total_documents_counter._value._value = 0
    filtered_by_language_counter._value._value = 0
    filtered_by_status_counter._value._value = 0
    filtered_by_missing_info_counter._value._value = 0
    passed_filters_counter._value._value = 0
    batch_counter._value._value = 0


class FakeReader(IndexReader):
    def __init__(self, data):
        self.data = data

    def __iter__(self):
        return iter(self.data)


class FakeDownloader(Downloader):
    def __init__(self, row: str):
        self.row = row

    def download_and_unzip(self, url: str, start: int, length: int) -> bytes:
        return f"{self.row}".encode("utf-8")


class ChannelSpy(MessageQueueChannel):
    def __init__(self):
        self.num_called = 0

    def basic_publish(self, exchange, routing_key, body):
        self.num_called += 1


def test_filter_non_english_documents():
    reader = FakeReader(
        [
            ["0,100,22,165)/ 20240722120756", "cdx-00000.gz", "0", "188224", "1"],
            [
                "101,141,199,66)/robots.txt 20240714155331",
                "cdx-00000.gz",
                "188224",
                "178351",
                "2",
            ],
            ["104,223,1,100)/ 20240714230020", "cdx-00000.gz", "366575", "178055", "3"],
        ]
    )
    channel = ChannelSpy()
    downloader = FakeDownloader(
        '0,100,22,165)/ 20240722120756 {"url": "http://165.22.100.0/", "mime": "text/html", "mime-detected": "text/html", "status": "301", "digest": "DCNYNIFG5SBRCVS5PCUY4YY2UM2WAQ4R", "length": "689", "offset": "3499", "filename": "crawl-data/CC-MAIN-2024-30/segments/1720763517846.73/crawldiagnostics/CC-MAIN-20240722095039-20240722125039-00443.warc.gz", "redirect": "https://157.245.55.71/"}\n'
    )
    process_index(reader, channel, downloader, 2)
    assert channel.num_called == 0


def test_filter_bad_status_code():
    reader = FakeReader(
        [
            ["0,100,22,165)/ 20240722120756", "cdx-00000.gz", "0", "188224", "1"],
            [
                "101,141,199,66)/robots.txt 20240714155331",
                "cdx-00000.gz",
                "188224",
                "178351",
                "2",
            ],
            ["104,223,1,100)/ 20240714230020", "cdx-00000.gz", "366575", "178055", "3"],
        ]
    )
    channel = ChannelSpy()
    downloader = FakeDownloader(
        '0,100,22,165)/ 20240722120756 {"url": "http://165.22.100.0/", "mime": "text/html", "mime-detected": "text/html", "status": "301", "languages": "eng", "digest": "DCNYNIFG5SBRCVS5PCUY4YY2UM2WAQ4R", "length": "689", "offset": "3499", "filename": "crawl-data/CC-MAIN-2024-30/segments/1720763517846.73/crawldiagnostics/CC-MAIN-20240722095039-20240722125039-00443.warc.gz", "redirect": "https://157.245.55.71/"}\n'
    )
    process_index(reader, channel, downloader, 2)
    assert channel.num_called == 0

def test_publish_all_urls():
    reader = FakeReader(
        [
            ["0,100,22,165)/ 20240722120756", "cdx-00000.gz", "0", "188224", "1"],
            [
                "101,141,199,66)/robots.txt 20240714155331",
                "cdx-00000.gz",
                "188224",
                "178351",
                "2",
            ],
            ["104,223,1,100)/ 20240714230020", "cdx-00000.gz", "366575", "178055", "3"],
        ]
    )
    channel = ChannelSpy()
    downloader = FakeDownloader(
        '0,100,22,165)/ 20240722120756 {"url": "http://165.22.100.0/", "mime": "text/html", "mime-detected": "text/html", "status": "200", "languages": ["eng"], "digest": "DCNYNIFG5SBRCVS5PCUY4YY2UM2WAQ4R", "length": "689", "offset": "3499", "filename": "crawl-data/CC-MAIN-2024-30/segments/1720763517846.73/crawldiagnostics/CC-MAIN-20240722095039-20240722125039-00443.warc.gz", "redirect": "https://157.245.55.71/"}\n'
        '0,100,22,165)/robots.txt 20240722120755 {"url": "http://165.22.100.0/robots.txt", "mime": "text/html", "mime-detected": "text/html", "status": "200", "languages": ["eng"], "digest": "LYEE2BXON4MCQCP5FDVDNILOWBKCZZ6G", "length": "700", "offset": "4656", "filename": "crawl-data/CC-MAIN-2024-30/segments/1720763517846.73/robotstxt/CC-MAIN-20240722095039-20240722125039-00410.warc.gz", "redirect": "https://157.245.55.71/robots.txt"}'
    )
    process_index(reader, channel, downloader, 2)
    assert channel.num_called == 3


def test_prometheus_total_documents_counter():
    """Test that total_documents counter increments for every document"""
    reset_counters()
    reader = FakeReader([["url 20240722120756", "cdx-00000.gz", "0", "188224", "1"]])
    channel = ChannelSpy()
    downloader = FakeDownloader('url 20240722120756 {"status": "200", "languages": ["eng"]}')
    process_index(reader, channel, downloader, 2)
    
    assert total_documents_counter._value._value == 1


def test_prometheus_filtered_by_missing_language_counter():
    reset_counters()
    reader = FakeReader([["url 20240722120756", "cdx-00000.gz", "0", "188224", "1"]])
    channel = ChannelSpy()
    downloader = FakeDownloader('url 20240722120756 {"status": "200"}')
    process_index(reader, channel, downloader, 2)
    
    assert filtered_by_missing_info_counter._value._value == 1
    assert passed_filters_counter._value._value == 0


def test_prometheus_filtered_by_language_counter():
    """Test that counter increments when document is not English"""
    reset_counters()
    reader = FakeReader([["url 20240722120756", "cdx-00000.gz", "0", "188224", "1"]])
    channel = ChannelSpy()
    downloader = FakeDownloader('url 20240722120756 {"status": "200", "languages": ["fra"]}')
    process_index(reader, channel, downloader, 2)
    
    assert filtered_by_language_counter._value._value == 1
    assert passed_filters_counter._value._value == 0


def test_prometheus_filtered_by_status_counter():
    """Test that counter increments when document is not status 200"""
    reset_counters()
    reader = FakeReader([["url 20240722120756", "cdx-00000.gz", "0", "188224", "1"]])
    channel = ChannelSpy()
    downloader = FakeDownloader('url 20240722120756 {"status": "404", "languages": ["eng"]}')
    process_index(reader, channel, downloader, 2)
    
    assert filtered_by_status_counter._value._value == 1
    assert passed_filters_counter._value._value == 0


def test_prometheus_passed_filters_counter():
    """Test that counter increments when document passes all filters"""
    reset_counters()
    reader = FakeReader([["url 20240722120756", "cdx-00000.gz", "0", "188224", "1"]])
    channel = ChannelSpy()
    downloader = FakeDownloader('url 20240722120756 {"status": "200", "languages": ["eng"]}')
    process_index(reader, channel, downloader, 2)
    
    assert filtered_by_missing_info_counter._value._value == 0
    assert filtered_by_language_counter._value._value == 0
    assert filtered_by_status_counter._value._value == 0
    assert passed_filters_counter._value._value == 1


def test_prometheus_batch_counter():
    """Test that batch counter increments when batch is published"""
    reset_counters()
    reader = FakeReader([["url 20240722120756", "cdx-00000.gz", "0", "188224", "1"]])
    channel = ChannelSpy()
    downloader = FakeDownloader('url 20240722120756 {"status": "200", "languages": ["eng"]}')
    process_index(reader, channel, downloader, 2)
    
    assert batch_counter._value._value == 1
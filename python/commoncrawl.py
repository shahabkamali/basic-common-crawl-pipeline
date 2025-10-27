from abc import ABC, abstractmethod
import csv
import gzip
import os
from typing import Generator, List, Optional
import requests


COMMONCRAWL_VERSION = os.getenv("COMMONCRAWL_VERSION", "CC-MAIN-2024-30")
COMMONCRAWL_BASE_URL = os.getenv("COMMONCRAWL_BASE_URL", "https://data.commoncrawl.org")
CRAWL_PATH = os.getenv("CRAWL_PATH", f"cc-index/collections/{COMMONCRAWL_VERSION}/indexes")
BASE_URL = COMMONCRAWL_BASE_URL


class Downloader(ABC):
    @abstractmethod
    def download_and_unzip(self, url: str, start: int, length: int) -> bytes:
        pass


class CCDownloader(Downloader):
    def __init__(self, base_url: str) -> None:
        self.base_url = base_url

    def download_and_unzip(self, url: str, start: int, length: int) -> bytes:
        headers = {"Range": f"bytes={start}-{start+length-1}"}
        response = requests.get(f"{self.base_url}/{url}", headers=headers)
        response.raise_for_status()
        buffer = response.content
        return gzip.decompress(buffer)


class IndexReader(ABC):
    @abstractmethod
    def __iter__(self):
        pass


class CSVIndexReader(IndexReader):
    def __init__(self, filename: str) -> None:
        self.file = open(filename, "r")
        self.reader = csv.reader(self.file, delimiter="\t")

    def __iter__(self):
        return self

    def __next__(self):
        return next(self.reader)

    def __del__(self) -> None:
        self.file.close()


def download_cluster_idx(crawl_version: str, output_path: str) -> None:
    """
    Download the cluster.idx file for a given Common Crawl version.
    
    Args:
        crawl_version: The Common Crawl version (e.g., "CC-MAIN-2024-30")
        output_path: Path where the file should be saved
    """
    # Construct the download URL
    url = f"{COMMONCRAWL_BASE_URL}/cc-index/collections/{crawl_version}/indexes/cluster.idx"
    
    # Ensure output directory exists
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    # Download the file
    response = requests.get(url, stream=True)
    response.raise_for_status()
    
    # Write to file
    with open(output_path, 'wb') as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)


def test_can_read_index(tmp_path):
    filename = tmp_path / "test.csv"
    index = "0,100,22,165)/ 20240722120756	cdx-00000.gz	0	188224	1\n\
101,141,199,66)/robots.txt 20240714155331	cdx-00000.gz	188224	178351	2\n\
104,223,1,100)/ 20240714230020	cdx-00000.gz	366575	178055	3"
    filename.write_text(index)
    reader = CSVIndexReader(filename)
    assert list(reader) == [
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

import os
import datetime
import sys
import json
import hashlib
import time
import logging
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from pipeline.worker import Worker, ListConsumerWorker, run
from scripts.configure import Plumbing


class ESConsumer(ListConsumerWorker):
    """
    takes lists of filepaths and prints them.
    """

    INPUT_BATCH_MSGS = 1  # process 10 messages at a time

    def __init__(self, process_name: str, descr: str):
        super().__init__(process_name, descr)
        self.folders_path = []
        self.index_name = "mediacloud_search_text"
        self.settings = {
            "settings": {"number_of_shards": 1, "number_of_replicas": 0},
            "mappings": {
                "properties": {
                    "domain": {"type": "keyword"},
                    "first_captured": {"type": "date"},
                    "host": {"type": "keyword"},
                    "language": {"type": "keyword"},
                    "publication_date": {"type": "date"},
                    "snippet": {"type": "text", "fielddata": True},
                    "surt_url": {"type": "keyword"},
                    "text_extraction_method": {"type": "keyword"},
                    "title": {"type": "text", "fielddata": True},
                    "tld": {"type": "keyword"},
                    "url": {"type": "keyword"},
                }
            },
        }
        self.es_url = os.environ.get("ELASTIC_URL")
        self.create_elasticsearch_index(self.es_url)

    def process_message(self, chan, method, properties, decoded):
        self.folders_path.append(decoded)

    def end_of_batch(self, chan):
        documents = []
        folders_path = self.folders_path
        for folder_path in folders_path:
            for root, dirs, files in os.walk(folder_path):
                for file_name in files:
                    print(f"Files:{file_name}")
                    if file_name.endswith(".json"):
                        file_path = os.path.join(root, file_name)
                        content = self.read_file_content(file_path)
                        documents.append(content)
            self.ingest_to_elasticsearch(documents)
            self.send_to_archiving_queue(chan, folder_path)

        self.folders_path = []
        sys.stdout.flush()
        return None

    def read_file_content(self, filepath):
        with open(filepath, "r") as file:
            content = file.read()
        try:
            item = json.loads(content)
            url = item["rss_entry"]["link"]
            pub_date_str = item.get("rss_entry").get("pub_date", "")
            pub_date = datetime.datetime.strptime(
                pub_date_str, "%a, %d %b %Y %H:%M:%S %z")

            if pub_date:
                es_date = pub_date.strftime("%Y-%m-%dT%H:%M:%S")
            else:
                es_date = None

            url_hash = hashlib.sha256(url.encode("utf-8")).hexdigest()
            document = {
                "_index": self.index_name,
                "_id": url_hash,
                "_source": {
                    "domain": item.get("rss_entry").get("domain"),
                    "first_captured": es_date,
                    "host": item.get("rss_entry").get("domain"),
                    "language": item.get("language"),
                    "publication_date": es_date,
                    "snippet": None,
                    "surt_url": None,
                    "text_extraction_method": None,
                    "title": item.get("rss_entry").get("title", ""),
                    "tld": item.get("tld"),
                    "url": url,
                    "version": "1.0",
                },
            }
        except json.JSONDecodeError as e:
            print(f"Error decoding JSON content: {e}")

        return document

    def ingest_to_elasticsearch(self, documents=None):
        es = Elasticsearch(self.es_url)
        if documents:
            bulk(es, documents)

    def create_elasticsearch_index(self, es_url):
        es = Elasticsearch(self.es_url)
        es.indices.create(index=self.index_name,
                          body=self.settings, ignore=400)

    def send_to_archiving_queue(self, chan, items):
        worker = Worker("archiving-gen", "publish folder to arhchiving Queue")
        print("sending to archiving queue...")
        print(items)
        worker.send_items(chan, items)
        print("sleeping...")
        sys.stdout.flush()
        time.sleep(1)


if __name__ == "__main__":
    run(ESConsumer, "elastic-consume", "get filepaths from Queue")

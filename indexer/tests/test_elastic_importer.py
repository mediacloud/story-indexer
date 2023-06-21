import os
from typing import Any

import pytest
from elasticsearch import Elasticsearch


@pytest.fixture(scope="class", autouse=True)
def set_env() -> None:
    os.environ["ELASTICSEARCH_HOST"] = "http://localhost:9200"
    os.environ["index_name"] = "mediacloud_search_text"


@pytest.fixture(scope="class")
def elasticsearch_client() -> Any:
    elasticsearch_host = os.environ.get("ELASTICSEARCH_HOST")
    client = Elasticsearch(hosts=[elasticsearch_host])
    assert client.ping(), "Failed to connect to Elasticsearch"

    return client


class TestElasticsearchConnection:
    def test_create_index(self, elasticsearch_client: Any) -> None:
        index_name = os.environ.get("index_name")
        if elasticsearch_client.indices.exists(index=index_name):
            elasticsearch_client.indices.delete(index=index_name)

        settings = {
            "settings": {"number_of_shards": 1, "number_of_replicas": 0},
            "mappings": {
                "properties": {
                    "original_url": {"type": "keyword"},
                    "url": {"type": "keyword"},
                    "normalized_url": {"type": "keyword"},
                    "canonical_domain": {"type": "keyword"},
                    "publication_date": {"type": "date"},
                    "language": {"type": "keyword"},
                    "full_language": {"type": "keyword"},
                    "text_extraction": {"type": "keyword"},
                    "article_title": {"type": "text", "fielddata": True},
                    "normalized_article_title": {"type": "text", "fielddata": True},
                    "text_content": {"type": "text"},
                    "is_homepage": {"type": "keyword"},
                    "is_shortened": {"type": "keyword"},
                }
            },
        }
        elasticsearch_client.indices.create(index=index_name, body=settings)
        assert elasticsearch_client.indices.exists(index=index_name)

    def test_index_document(self, elasticsearch_client: Any) -> None:
        index_name = os.environ.get("index_name")
        document = {
            "article_title": "Test Document",
            "text_content": "Lorem ipsum dolor sit amet.",
            "canonical_domain": "example.com",
            "publication_date": "2023-06-21T10:00:00",
            "language": "en",
            "full_language": "English",
            "text_extraction": "html",
            "normalized_article_title": "Test Document",
            "original_url": "http://www.example.com/index.html",
            "url": "http://www.example.com/index.html",
            "normalized_url": "http://www.example.com/index.html",
            "is_homepage": "false",
            "is_shortened": "false",
        }
        response = elasticsearch_client.index(index=index_name, body=document)
        assert response["result"] == "created"
        assert "_id" in response

    @classmethod
    def teardown_class(cls) -> None:
        elasticsearch_host = os.environ.get("ELASTICSEARCH_HOST")
        index_name = os.environ.get("index_name")
        elasticsearch_client = Elasticsearch(hosts=[elasticsearch_host])
        if elasticsearch_client.indices.exists(index=index_name):
            elasticsearch_client.indices.delete(index=index_name)

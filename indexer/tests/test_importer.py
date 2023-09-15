import dataclasses
import hashlib
import os
from typing import Any, Dict, List, Mapping, Optional, Union, cast

import pytest
from elasticsearch import Elasticsearch

from indexer.workers.importer import (
    ElasticsearchConnector,
    ElasticsearchImporter,
    es_mappings,
    es_settings,
)


@pytest.fixture(scope="class", autouse=True)
def set_env() -> None:
    os.environ["ELASTICSEARCH_HOST"] = "http://localhost:9200"
    os.environ["ELASTICSEARCH_INDEX_NAMES"] = ",".join(
        ["test_mediacloud_search_text", "test_mediacloud_search_text_2023"]
    )


@pytest.fixture(scope="class")
def elasticsearch_client() -> Any:
    hosts = os.environ.get("ELASTICSEARCH_HOST")
    assert hosts is not None, "ELASTICSEARCH_HOST is not set"
    client = Elasticsearch(hosts=hosts)
    assert client.ping(), "Failed to connect to Elasticsearch"

    return client


test_data: Mapping[str, Optional[Union[str, bool]]] = {
    "original_url": "http://example.com",
    "normalized_url": "http://example.com",
    "url": "http://example.com",
    "canonical_domain": "example.com",
    "publication_date": "2023-06-27",
    "language": "en",
    "full_language": "English",
    "text_extraction": "Lorem ipsum",
    "article_title": "Example Article",
    "normalized_article_title": "example article",
    "text_content": "Lorem ipsum dolor sit amet",
}


class TestElasticsearchConnection:
    def test_create_index(self, elasticsearch_client: Any) -> None:
        index_names_str = os.environ.get("ELASTICSEARCH_INDEX_NAMES")
        assert index_names_str is not None, "ELASTICSEARCH_INDEX_NAMES is not set"
        index_names = index_names_str.split(",")
        for index_name in index_names:
            if elasticsearch_client.indices.exists(index=index_name):
                elasticsearch_client.indices.delete(index=index_name)
            elasticsearch_client.indices.create(
                index=index_name, mappings=es_mappings, settings=es_settings
            )
            assert elasticsearch_client.indices.exists(index=index_name)

    def test_index_document(self, elasticsearch_client: Any) -> None:
        index_names_str = os.environ.get("ELASTICSEARCH_INDEX_NAMES")
        assert index_names_str is not None, "ELASTICSEARCH_INDEX_NAMES is not set"
        index_names = index_names_str.split(",")
        for index_name in index_names:
            response = elasticsearch_client.index(index=index_name, document=test_data)
            assert response["result"] == "created"
            assert "_id" in response

    @classmethod
    def teardown_class(cls) -> None:
        elasticsearch_host = cast(str, os.environ.get("ELASTICSEARCH_HOST"))
        index_names_str = os.environ.get("ELASTICSEARCH_INDEX_NAMES")
        assert index_names_str is not None, "ELASTICSEARCH_INDEX_NAMES is not set"
        index_names = index_names_str.split(",")
        elasticsearch_client = Elasticsearch(hosts=[elasticsearch_host])
        for index_name in index_names:
            if elasticsearch_client.indices.exists(index=index_name):
                elasticsearch_client.indices.delete(index=index_name)


@pytest.fixture(scope="class")
def elasticsearch_connector() -> ElasticsearchConnector:
    elasticsearch_host = cast(str, os.environ.get("ELASTICSEARCH_HOST"))
    index_names = os.environ.get("ELASTICSEARCH_INDEX_NAMES")
    assert index_names is not None, "ELASTICSEARCH_INDEX_NAMES is not set"
    connector = ElasticsearchConnector(
        elasticsearch_host, index_names, es_mappings, es_settings
    )
    return connector


class TestElasticsearchImporter:
    @pytest.fixture
    def importer(self) -> ElasticsearchImporter:
        return ElasticsearchImporter("test_importer", "elasticsearch import worker")

    target_indexes = ["test_mediacloud_search_text", "test_mediacloud_search_text_2023"]

    @pytest.mark.parametrize("target_index", target_indexes)
    def test_import_story_success(
        self,
        importer: ElasticsearchImporter,
        elasticsearch_connector: ElasticsearchConnector,
        target_index: str,
    ) -> None:
        importer.connector = elasticsearch_connector
        url = test_data.get("url")
        assert isinstance(url, str)
        id = hashlib.sha256(url.encode("utf-8")).hexdigest()
        response = importer.import_story(id, test_data, target_index=target_index)
        if response is not None:
            assert response.get("result") == "created"
        else:
            raise AssertionError("No response received")

    def test_index_routing(
        self,
        importer: ElasticsearchImporter,
        elasticsearch_connector: ElasticsearchConnector,
    ) -> None:
        importer.connector = elasticsearch_connector
        assert (
            importer.index_routing("Tue 27 Jun 2023, 12:00AM")
            == "mediacloud_search_text_2023"
        )
        assert importer.index_routing(None) == "mediacloud_search_text_other"
        assert (
            importer.index_routing("Tue 27 Jun 2022, 12:00AM")
            == "mediacloud_search_text_2022"
        )
        assert (
            importer.index_routing("Tue 27 Jun 2020, 12:00AM")
            == "mediacloud_search_text_other"
        )

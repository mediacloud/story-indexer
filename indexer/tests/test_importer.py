import dataclasses
import hashlib
import os
from typing import Any, Dict, List, Mapping, Optional, Union, cast
from urllib.parse import urlparse

import pytest
from elastic_transport import NodeConfig
from elasticsearch import Elasticsearch

from indexer.workers.importer import (
    ElasticsearchConnector,
    ElasticsearchImporter,
    es_mappings,
    es_settings,
)


@pytest.fixture(scope="class", autouse=True)
def set_env() -> None:
    os.environ["ELASTICSEARCH_HOSTS"] = ",".join(
        ["http://localhost:9200", "http://localhost:9201", "http://localhost:9202"]
    )
    os.environ["ELASTICSEARCH_INDEX_NAMES"] = ",".join(
        ["test_mediacloud_search_text", "test_mediacloud_search_text_2023"]
    )


def create_and_delete_indices(client: Elasticsearch, index_names: list) -> None:
    for index_name in index_names:
        if client.indices.exists(index=index_name):
            client.indices.delete(index=index_name)
        client.indices.create(
            index=index_name, mappings=es_mappings, settings=es_settings
        )


@pytest.fixture(scope="class")
def elasticsearch_client() -> Any:
    hosts = os.environ.get("ELASTICSEARCH_HOSTS")
    assert hosts is not None, "ELASTICSEARCH_HOSTS is not set"

    host_urls: Any = [host_url for host_url in hosts.split(",")]
    host_configs: Any = []
    for host_url in host_urls:
        parsed_url = urlparse(str(host_url))
        host = parsed_url.hostname
        scheme = parsed_url.scheme
        port = parsed_url.port
        if host and scheme and port:
            node_config = NodeConfig(scheme=scheme, host=host, port=port)
            host_configs.append(node_config)

    client = Elasticsearch(hosts=host_configs)
    assert client.ping(), "Failed to connect to Elasticsearch"
    index_names_str = os.environ.get("ELASTICSEARCH_INDEX_NAMES")
    assert index_names_str is not None, "ELASTICSEARCH_INDEX_NAMES is not set"
    index_names = index_names_str.split(",")

    create_and_delete_indices(client, index_names)

    yield client


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
        index_names = elasticsearch_client.indices.get_alias().keys()
        for index_name in index_names:
            assert elasticsearch_client.indices.exists(index=index_name)

    def test_index_document(self, elasticsearch_client: Any) -> None:
        index_names = elasticsearch_client.indices.get_alias().keys()
        for index_name in index_names:
            response = elasticsearch_client.index(index=index_name, document=test_data)
            assert response["result"] == "created"
            assert "_id" in response


@pytest.fixture(scope="class")
def elasticsearch_connector() -> ElasticsearchConnector:
    elasticsearch_hosts = cast(str, os.environ.get("ELASTICSEARCH_HOSTS"))
    index_names = os.environ.get("ELASTICSEARCH_INDEX_NAMES")
    assert index_names is not None, "ELASTICSEARCH_INDEX_NAMES is not set"
    connector = ElasticsearchConnector(
        elasticsearch_hosts, index_names, es_mappings, es_settings
    )
    return connector


class TestElasticsearchImporter:
    @pytest.fixture
    def importer(self) -> ElasticsearchImporter:
        return ElasticsearchImporter("test_importer", "elasticsearch import worker")

    def test_import_story_success(
        self,
        importer: ElasticsearchImporter,
        elasticsearch_connector: ElasticsearchConnector,
    ) -> None:
        importer.connector = elasticsearch_connector
        url = test_data.get("url")
        assert isinstance(url, str)
        id = hashlib.sha256(url.encode("utf-8")).hexdigest()
        response = importer.import_story(id, test_data)
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
        assert importer.index_routing("2023-06-27") == "mediacloud_search_text_2023"
        assert importer.index_routing(None) == "mediacloud_search_text_other"
        assert importer.index_routing("2022-06-27") == "mediacloud_search_text_2022"
        assert importer.index_routing("2020-06-27") == "mediacloud_search_text_other"

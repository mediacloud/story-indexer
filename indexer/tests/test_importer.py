import hashlib
import os
from datetime import datetime
from typing import Any, Mapping, Optional, Union, cast
from urllib.parse import urlparse

import pytest
from elastic_transport import NodeConfig
from elasticsearch import ConflictError, Elasticsearch

from indexer.worker import QuarantineException

# from indexer.elastic import create_elasticsearch_client
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
    os.environ["ELASTICSEARCH_INDEX_NAME_PREFIX"] = "test_mediacloud_search_text"


def recreate_indices(client: Elasticsearch, index_name: str) -> None:
    if client.indices.exists(index=index_name):
        client.indices.delete(index=index_name)
    client.indices.create(index=index_name, mappings=es_mappings, settings=es_settings)


@pytest.fixture(scope="class")
def elasticsearch_client() -> Any:
    hosts: Any = os.environ.get("ELASTICSEARCH_HOSTS")
    assert hosts is not None, "ELASTICSEARCH_HOSTS is not set"
    client = Elasticsearch(hosts.split(","))
    assert client.ping(), "Failed to connect to Elasticsearch"
    index_name_prefix = os.environ.get("ELASTICSEARCH_INDEX_NAME_PREFIX")
    assert index_name_prefix is not None, "ELASTICSEARCH_INDEX_NAME_PREFIX is not set"

    recreate_indices(client, f"{index_name_prefix}_older")

    yield client

    recreate_indices(client, f"{index_name_prefix}_older")


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
    "indexed_date": datetime.now().isoformat(),
}

url = test_data.get("url")
assert isinstance(url, str)
test_id = hashlib.sha256(url.encode("utf-8")).hexdigest()


class TestElasticsearchConnection:
    def test_create_index(self, elasticsearch_client: Any) -> None:
        index_names = list(elasticsearch_client.indices.get_alias().keys())
        index_name = index_names[0]
        assert elasticsearch_client.indices.exists(index=index_name)

    def test_index_document(self, elasticsearch_client: Any) -> None:
        index_names = list(elasticsearch_client.indices.get_alias().keys())
        index_name = index_names[0]
        response = elasticsearch_client.create(
            index=index_name, id=test_id, document=test_data
        )
        assert response["result"] == "created"
        assert "_id" in response

        with pytest.raises(ConflictError) as exc_info:
            elasticsearch_client.create(
                index=index_name, id=test_id, document=test_data
            )
        assert "ConflictError" in str(exc_info.type)
        assert "version_conflict_engine_exception" in str(exc_info.value)

    def test_index_document_with_none_date(self, elasticsearch_client: Any) -> None:
        index_names = list(elasticsearch_client.indices.get_alias().keys())
        index_name = index_names[0]
        test_data_with_none_date = {
            **test_data,
            "id": "adrferdiyhyu9",
            "publication_date": None,
        }
        response = elasticsearch_client.create(
            index=index_name,
            id=test_data_with_none_date["id"],
            document=test_data_with_none_date,
        )
        assert response["result"] == "created"
        assert "_id" in response

        with pytest.raises(ConflictError) as exc_info:
            elasticsearch_client.create(
                index=index_name, id=test_id, document=test_data_with_none_date
            )
        assert "ConflictError" in str(exc_info.type)
        assert "version_conflict_engine_exception" in str(exc_info.value)


@pytest.fixture(scope="class")
def elasticsearch_connector(elasticsearch_client: Any) -> ElasticsearchConnector:
    connector = ElasticsearchConnector(elasticsearch_client, es_mappings, es_settings)
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
        importer.index_name_prefix = os.environ.get("ELASTICSEARCH_INDEX_NAME_PREFIX")
        test_import_data = {**test_data, "url": "http://example_import_story.com"}
        response = importer.import_story(test_import_data)
        if response is not None:
            assert response.get("result") == "created"

        with pytest.raises(QuarantineException) as exc_info:
            importer.import_story(test_import_data)
        assert "ConflictError" in str(exc_info.value)

    def test_index_routing(
        self,
        importer: ElasticsearchImporter,
        elasticsearch_connector: ElasticsearchConnector,
    ) -> None:
        importer.connector = elasticsearch_connector
        importer.index_name_prefix = os.environ.get("ELASTICSEARCH_INDEX_NAME_PREFIX")
        assert importer.index_name_prefix is not None
        assert (
            importer.index_routing("2023-06-27") == f"{importer.index_name_prefix}_2023"
        )
        assert importer.index_routing(None) == f"{importer.index_name_prefix}_other"
        assert (
            importer.index_routing("2022-06-27") == f"{importer.index_name_prefix}_2022"
        )
        assert (
            importer.index_routing("2020-06-27")
            == f"{importer.index_name_prefix}_older"
        )
        assert (
            importer.index_routing("2026-06-27")
            == f"{importer.index_name_prefix}_other"
        )

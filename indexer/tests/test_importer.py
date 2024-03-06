import os
from datetime import datetime
from typing import Any, Mapping, Optional, Union

import pytest
from elasticsearch import ConflictError, Elasticsearch
from mcmetadata.urls import unique_url_hash

from indexer.workers.importer import ElasticsearchImporter, truncate_str


@pytest.fixture(scope="class", autouse=True)
def set_env() -> None:
    os.environ["ELASTICSEARCH_HOSTS"] = ",".join(
        ["http://localhost:9210", "http://localhost:9211", "http://localhost:9212"]
    )


@pytest.fixture(scope="class")
def elasticsearch_client() -> Any:
    hosts: Any = os.environ.get("ELASTICSEARCH_HOSTS")
    assert hosts is not None, "ELASTICSEARCH_HOSTS is not set"
    client = Elasticsearch(hosts.split(","))
    assert client.ping(), "Failed to connect to Elasticsearch"
    yield client


test_index_template: Any = {
    "name": "test_mediacloud_search_template",
    "index_patterns": "test_mediacloud_search*",
    "template": {
        "settings": {
            "index.lifecycle.name": "mediacloud-lifecycle-policy",
            "index.lifecycle.rollover_alias": "mc_search",
            "number_of_shards": 1,
            "number_of_replicas": 1,
        },
        "mappings": {
            "properties": {
                "article_title": {
                    "type": "text",
                    "fields": {"keyword": {"type": "keyword"}},
                    "fielddata": True,
                },
                "canonical_domain": {"type": "keyword"},
                "full_language": {"type": "keyword"},
                "indexed_date": {"type": "date"},
                "language": {
                    "type": "text",
                    "fields": {"keyword": {"type": "keyword"}},
                },
                "original_url": {"type": "keyword"},
                "publication_date": {"type": "date", "ignore_malformed": True},
                "text_content": {
                    "type": "text",
                    "fields": {"keyword": {"type": "keyword"}},
                    "fielddata": True,
                },
                "url": {"type": "keyword"},
            }
        },
    },
}

test_ilm_policy: Any = {
    "name": "test_mediacloud-lifecycle-policy",
    "policy": {"phases": {"hot": {"actions": {"rollover": {"max_age": "10m"}}}}},
}

test_initial_index: Any = {
    "name": "test_mediacloud_search-000001",
    "aliases": {"test_mc_search": {"is_write_index": True}},
}

test_data: Mapping[str, Optional[Union[str, bool]]] = {
    "original_url": "http://example.com",
    "url": "http://example.com",
    "canonical_domain": "example.com",
    "publication_date": "2023-06-27",
    "language": "en",
    "full_language": "English",
    "article_title": "Example Article",
    "text_content": "Lorem ipsum dolor sit amet",
    "indexed_date": datetime.now().isoformat(),
}


def test_truncate_str() -> None:
    s = "e\u0301"  # é
    assert truncate_str(s, 0) == ""
    assert truncate_str(s, 1) == ""
    assert truncate_str(s, 2) == "é"
    # Without normalizing to "NFC", e and \u0301 are kept separate
    assert truncate_str(s, 1, normalize=False) == "e"
    assert truncate_str(s, 2, normalize=False) == "é"


class TestElasticsearchConf:
    def test_create_index_template(self, elasticsearch_client: Any) -> None:
        name = test_index_template.get("name")
        template = test_index_template.get("template")
        index_patterns = test_index_template.get("index_patterns")

        result = elasticsearch_client.indices.put_index_template(
            name=name, index_patterns=index_patterns, template=template
        )
        assert result.get("acknowledged") is True

    def test_create_ilm_policy(self, elasticsearch_client: Any) -> None:
        name = test_ilm_policy.get("name")
        policy = test_ilm_policy.get("policy")
        result = elasticsearch_client.ilm.put_lifecycle(name=name, policy=policy)
        assert result.get("acknowledged") is True

    def test_create_initial_index(self, elasticsearch_client: Any) -> None:
        index = test_initial_index.get("name")
        aliases = test_initial_index.get("aliases")
        index_exists = elasticsearch_client.indices.exists(index=index)
        if not index_exists:
            result = elasticsearch_client.indices.create(index=index, aliases=aliases)
            assert result.get("acknowledged") is True


class TestElasticsearchConnection:
    def test_index_document(self, elasticsearch_client: Any) -> None:
        index_name_alias = "test_mc_search"
        test_id = unique_url_hash(str(test_data.get("url")))
        response = elasticsearch_client.create(
            index=index_name_alias, id=test_id, document=test_data
        )
        assert response["result"] == "created"
        assert "_id" in response

        with pytest.raises(ConflictError) as exc_info:
            elasticsearch_client.create(
                index=index_name_alias, id=test_id, document=test_data
            )
        assert "ConflictError" in str(exc_info.type)
        assert "version_conflict_engine_exception" in str(exc_info.value)

    def test_index_document_with_none_date(self, elasticsearch_client: Any) -> None:
        index_name_alias = "test_mc_search"
        test_data_with_none_date = {
            **test_data,
            "id": "adrferdiyhyu9",
            "publication_date": None,
        }
        test_id = unique_url_hash(str(test_data.get("url")))
        response = elasticsearch_client.create(
            index=index_name_alias,
            id=test_data_with_none_date["id"],
            document=test_data_with_none_date,
        )
        assert response["result"] == "created"
        assert "_id" in response

        with pytest.raises(ConflictError) as exc_info:
            elasticsearch_client.create(
                index=index_name_alias, id=test_id, document=test_data_with_none_date
            )
        assert "ConflictError" in str(exc_info.type)
        assert "version_conflict_engine_exception" in str(exc_info.value)


class TestElasticsearchImporter:
    @pytest.fixture
    def importer(self) -> ElasticsearchImporter:
        importer = ElasticsearchImporter("test_importer", "elasticsearch import worker")
        importer.elasticsearch_hosts = os.environ.get("ELASTICSEARCH_HOSTS")
        return importer

    def test_import_story_success(self, importer: ElasticsearchImporter) -> None:
        test_import_data = {**test_data, "url": "http://example_import_story.com"}
        assert importer.import_story(test_import_data)

    def test_import_story_extra_fields(self, importer: ElasticsearchImporter) -> None:
        test_extra_data = {
            "normalized_article_title": "the basic principles of would or could",
            "normalized_url": "http://damienafsoe.ttblogs.com/4775282/the-basic-principles-of-would-or-could",
            "text_extraction_method": "trafilatura",
        }
        test_import_data = {
            **test_data,
            **test_extra_data,
            "url": "https://damienafsoe.ttblogs.com/4775282/the-basic-principles-of-would-or-could",
        }
        id = importer.import_story(test_import_data)
        assert id
        search_response = importer.elasticsearch_client().search(
            index="test_mc_search",
            body={
                "query": {"bool": {"filter": {"term": {"_id": id}}}},
            },
        )
        assert search_response["hits"]["hits"], "No document in search response"

        search_response_keys = search_response["hits"]["hits"][0]["_source"]
        for key in test_extra_data:
            assert (
                key not in search_response_keys.keys()
            ), f"Unexpected field '{key}' found in response."

    def test_import_story_duplicate(self, importer: ElasticsearchImporter) -> None:
        test_import_data = {**test_data, "url": "https://www.edbernerzeitung.ch12/"}
        url = test_import_data["url"]
        assert isinstance(url, str), "url must be a string"
        search_response = importer.elasticsearch_client().search(
            index="test_mc_search",
            body={
                "query": {"bool": {"filter": {"term": {"_id": unique_url_hash(url)}}}},
                "size": 0,
            },
        )
        assert search_response["hits"]["total"]["value"] == 0, "Document already exists"
        assert importer.import_story(test_import_data)
        assert not importer.import_story(test_import_data)

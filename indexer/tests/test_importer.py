import dataclasses
import os
from typing import Any, Dict, Optional, Union, cast

import pytest
from elasticsearch import ElasticsearchException
from pika.adapters.blocking_connection import BlockingChannel
from pytest_mock import MockFixture, mocker

from indexer.elastic_conf import ElasticsearchConnector
from indexer.story import BaseStory, ContentMetadata
from indexer.workers.importer import ElasticsearchImporter


@pytest.fixture(scope="class", autouse=True)
def set_env() -> None:
    os.environ["ELASTICSEARCH_HOST"] = "http://localhost:9200"
    os.environ["INDEX_NAME"] = "mediacloud_search_text"


@pytest.fixture(scope="class")
def elasticsearch_connector() -> ElasticsearchConnector:
    elasticsearch_host = cast(str, os.environ.get("ELASTICSEARCH_HOST"))
    index_name = cast(str, os.environ.get("index_name"))
    return ElasticsearchConnector(
        elasticsearch_host=elasticsearch_host,
        index_name=index_name,
    )


@pytest.fixture
def test_data() -> Dict[str, Optional[Union[str, bool]]]:
    return {
        "original_url": "http://example.com",
        "url": "http://example.com",
        "normalized_url": "http://example.com",
        "canonical_domain": "example.com",
        "publication_date": "2023-06-27",
        "language": "en",
        "full_language": "English",
        "text_extraction": "Lorem ipsum",
        "article_title": "Example Article",
        "normalized_article_title": "example article",
        "text_content": "Lorem ipsum dolor sit amet",
        "is_homepage": False,
        "is_shortened": False,
    }


class TestElasticsearchImporter:
    def test_import_story_successful(
        self,
        elasticsearch_connector: ElasticsearchConnector,
        test_data: Dict[str, Optional[Union[str, bool]]],
    ) -> None:
        importer = ElasticsearchImporter(connector=elasticsearch_connector)
        response = importer.import_story(test_data)
        assert response.get("result") == "created"

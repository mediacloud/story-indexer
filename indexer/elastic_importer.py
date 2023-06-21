import logging
import os
from typing import Any, Dict, List

from elasticsearch import Elasticsearch


class ElasticsearchConnector:
    def __init__(self, elasticsearch_host: str, index_name: str):
        self.elasticsearch_client = Elasticsearch(hosts=[elasticsearch_host])
        self.index_name = index_name

    def create_index(self) -> None:
        self.elasticsearch_client.indices.create(index=self.index_name)

    def index_document(self, document: Dict[str, Any]) -> Dict[str, Any]:
        response: Dict[str, Any] = self.elasticsearch_client.index(
            index=self.index_name, body=document
        )
        return response


class ElasticsearchImporter:
    def __init__(self, connector: ElasticsearchConnector):
        self.connector = connector

    def import_stories(self, stories: List[Dict[str, Any]]) -> None:
        """
        Load story object from queue and import to elastic search
        """
        raise NotImplementedError("The _process_story method is not implemented yet")

    def _process_story(self, story: Dict[str, Any]) -> Dict[str, Any]:
        """
        Process story and extract metadata
        """
        raise NotImplementedError("The _process_story method is not implemented yet")

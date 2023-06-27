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

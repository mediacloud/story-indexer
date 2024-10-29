## Elasticsearch Reindexing Guide

The _reindex API in Elasticsearch allows you to copy documents from one index to another. This can be useful when you need to change the mappings of an index, upgrade Elasticsearch versions, or simply migrate data.
This huide covers reindexing using two methods;

1. Using the Kibana Dev Tools
2. Using `curl` and Elasticsearch `reindex` API as per the script [here](bin/run-elastic-reindex.sh)

### Reindexing with Kibana Dev Tools

The Kibana Dev Tools provides an interactive environment to execute Elasticsearch queries and API commands

#### Steps

1. Open Kibana and navigate to Dev Tools > Console

2. Use the following `POST` request to reindex documents from the source index to the destination index

```
POST _reindex
{
  "source": {
    "index": "source_index_name"
  },
  "dest": {
    "index": "dest_index_name",
    "op_type": "create"
  }
}

```

**Replace source-index-name and dest-index-name with the names of your source and destination indices.
***Set the "op-type":"create" to avoid overwriting existing documents in the destination index. To allow overwriting use "op_type":"index"

## Elasticsearch Reindexing Guide

The _reindex API in Elasticsearch allows you to copy documents from one index to another. This can be useful when you need to change the mappings of an index, upgrade Elasticsearch versions, or simply migrate data.
This huide covers reindexing using two methods;

1. Using the Kibana Dev Tools
2. Using `curl` and Elasticsearch `reindex` API as per the script [here](../../bin/run-elastic-reindex.sh)

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

*** Replace source-index-name and dest-index-name with the names of your source and destination indices.

*** Set the "op-type":"create" to avoid overwriting existing documents in the destination index. To allow overwriting use "op_type":"index"

3. Use the following GET request from the Kibana Dev Tools console to get the Reindexing status

```
GET _tasks/<task_id>

```

### Reindexing with Curl & Elasticsearch Reindex API

The Elasticsearch Reindex API provides for a REST endpoint to re-index documents.
The bash script available [here](../../bin/run-elastic-reindex.sh), allows for re-indexing by performing the necessary checks, and initiating the re-indexing process asynchronously.

The script returns a `task ID` that can be used to monitor the Reindexing status via curl command

```
curl -X GET "http://localhost:9200/_tasks/<task_id>"
```

#### Reindexing Limited Number of Documents for Testing

The Elasticsearch Reindex API provides for a `max_docs` argument to specify the maximum number of documents to reindex.

```
{
    "source": {
        "index": "mc_search-000002"
    },
    "dest": {
        "index": "mc_search-000002-test",
        "op_type": "create"
    },
    "max_docs": 10
}
```

The [script](../../bin/run-elastic-reindex.sh) provides for an optional argument `-m` to specify the number of documents to re-index.

#### Reindexing from multiple sources

Elasticsearch recommends to index one document at a time if we have many indices to reindex from, as referenced [here](https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-reindex.html#docs-reindex-from-multiple-sources).

### Reindexing select documents with a query

To reindex documents based on specific criteria, you can utilize the query parameter in the reindex request. This allows you to specify a query that filters the documents being reindexed. Here’s how you can structure your request:

```
POST _reindex
{
  "source": {
    "index": "mc_search-000002",
    "query": {
      "match": {
        "canonical-domain": "mediacloud.org"
      }
    }
  },
  "dest": {
    "index": "mc_search-000002-test"
  }
}
```

Example in the bash script [here](../../bin/run-elastic-reindex.sh)

```
bin/run-elastic-reindex.sh -s mc_search-000003 mc_search-000004 -d reindexed -m 1000 -q '{
   "match": {
      "canonical_domain": "okezone.com"
   }
}'
```

#### Slicing

The Reindex API supports Sliced scroll to parallelize the [reindexing process](https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-reindex.html#docs-reindex-slice), thereby improving efficiency.
We can perfom slicing Manually (providing the no.of slices for each request) or Automatically (let Elasticsearch chose the number of slices to use).

```
curl -s -X POST "$ES_HOST/_reindex?slices=auto&wait_for_completion=false"
```

#### Throttling

The Reindex API supports throttling during reindexing by setting the `requests_per_second` to throttle the rate at which `_reindex` issues batches of index operations.

##### Rethrotting During Reindex

Based on the clusture monitoring stats, you can adjust the throttling dynamically using the _rethrottle API. This allows us to manage the load to our clusture.

```
POST _reindex/<task_id>/_rethrottle?requests_per_second=10
```

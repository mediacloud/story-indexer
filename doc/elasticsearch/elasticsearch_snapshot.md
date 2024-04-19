## Elasticsearch Sansphot with AWS S3

### Register Snapshot

Use the Elasticsearch API to register our snapshot repository.

To register the AWS S3 as a snapshot repository, we need to
  1. AWS Setup
  2. Snapshot Repository Registration

#### AWS Setup

Create S3 buckets, for the various stages of development with policies to manage snapshot and restore process to S3;
Current buckets:

   `dev - test-mediacloud-elasticsearch-snapshots`

   `staging - staging-mediacloud-elasticsearch-snapshots`

   `prod - mediacloud-elasticsearch-snapshots`

AWS IAM User Policy

`
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Action": [
                "s3:ListBucket",
                "s3:GetBucketLocation",
                "s3:ListBucketMultipartUploads",
                "s3:ListBucketVersions"
            ],
            "Effect": "Allow",
            "Resource": [
                "arn:aws:s3:::S3_BUCKET_NAME",
            ]
        },
        {
            "Action": [
                "s3:GetObject",
                "s3:PutObject",
                "s3:DeleteObject",
                "s3:AbortMultipartUpload",
                "s3:ListMultipartUploadParts"
            ],
            "Effect": "Allow",
            "Resource": [
                "arn:aws:s3:::S3_BUCKET_NAME/*",
            ]
        }
    ]
}
`

#### Snapshot Repository Registration

```sh
curl -X PUT "http://localhost:9200/\_snapshot/mc_ec_s3_repository" -H "Content-Type: application/json" -d '{
"type": "s3",
"settings": {
"bucket": "S3_BUCKET_NAME",
}
}'
```

#### Verify Snapshot Repository

``` sh
curl -X POST 'http://localhost:9200/_snapshot/mc_ec_s3_repository/_verify'

```

#### Get Registered Snapshot Repository

``` sh
curl -X GET "http://localhost:9200/_snapshot/*?pretty"

```

### Manually create snapshots

To create the snapshot without SLM policy using Elasticsearch snapshot API

```sh
curl X -POST "http://localhost:9200/_snapshot/mc_ec_s3_repository/snapshot_{now/d}?wait_for_completion=true"
```

### SLM (Snapshot Lifecycle Manager)

We create snapshots using Elasticsearch's Snapshot API, the policy defined [here](../../conf/elasticsearch/templates/create_slm_policy.json)

The snapshots are incremental in nature, therefore this means that every 2 week snapshot taken builds from the previous snapshot's data segments. Each of the snapshots taken is full snapshot, hence can be used independently for restore operations.

Our policy defines a retention policy of, minimum 5 snaps and maximum 10. This should allow us to have at minimum 1 month & 1/2 backdated data to restore from.
PS: We can change the retention policy based on storage costs going forward.

```
"retention": {
        "min_count": 5,
        "max_count": 10
    }

```

### Snapshot restoration

To restore the snapshots published to S3 using SLM, we can use the [Snapshot & Restore API](https://www.elastic.co/guide/en/elasticsearch/reference/current/snapshots-restore-snapshot.html) as follows

1. Get the list of available snapshots

```
curl -x GET "http://localhost:9200/_snapshot
```

2. Restore an index

If we're restoring data to a pre-existing cluster, we can use either of the following methods.

Delete and Restore - Delete an existing index before restoring it

    ```
    # Delete an index
    curl -X DELETE "localhost:9200/<index_name>?pretty"

    # Restore Index
    curl -X POST "localhost:9200/_snapshot/<repository_name>/<snapshot_id>/_restore?pretty" -H 'Content-Type: application/json' -d'
    {
    "indices": "<my-index-name>"
    }
    '
    ```

Rename on restore - To avoid deleting existing data, we can rename the indices on restore. e.g rename "mc
-search_000001" to "mc_search_restored_000001"

    ```
    curl -X POST "localhost:9200/_snapshot/<repository_name>/<snapshot_id>/_restore?pretty" -H 'Content-Type: application/json' -d'
    {
    "indices": "mc_search-*",
    "rename_pattern": "mc_search-(.+)",
    "rename_replacement": "mc_search_restored-$1"
    }
    '

    ```

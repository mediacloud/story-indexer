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

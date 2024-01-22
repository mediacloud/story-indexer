"""
BlobStore provider for AWS S3 Object Store
"""

import boto3
import botocore.exceptions

import indexer.blobstore


class S3Error(indexer.blobstore.BlobStoreError):
    """
    thrown on operation failure
    """


class S3Store(indexer.blobstore.BlobStore):
    PROVIDER = "S3"  # for config names
    EXCEPTIONS = [botocore.exceptions.BotoCoreError, S3Error]

    def __init__(self, store_name: str):
        super().__init__(store_name)
        self.s3_bucket = self._conf_val("BUCKET")
        self.s3 = boto3.client(
            "s3",
            region_name=self._conf_val("REGION"),
            aws_access_key_id=self._conf_val("ACCESS_KEY_ID"),
            aws_secret_access_key=self._conf_val("SECRET_ACCESS_KEY"),
        )

    def store_from_local_file(self, local_path: str, remote_path: str) -> None:
        # mypy says it doesn't return a value:
        self.s3.upload_file(local_path, self.s3_bucket, remote_path)

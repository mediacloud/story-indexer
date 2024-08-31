"""
BlobStore provider for AWS S3 Object Store
"""

import logging
from typing import TYPE_CHECKING, Any, Generator

import boto3
import botocore.exceptions

from indexer.blobstore import BlobStore, FileObj

if TYPE_CHECKING:
    from mypy_boto3_s3.client import S3Client
else:
    S3Client = Any


logger = logging.getLogger(__name__)


class S3Store(BlobStore):
    """
    Class for stores with AWS S3 API using boto3.

    Can add non-AWS services by subclassing and overriding PROVIDER
    and URL_FORMAT to allow multiple providers at one time!
    """

    PROVIDER = "S3"  # for config names, may be overridden by subclasses!

    URL_FORMAT = "https://s3.{region}.amazonaws.com"

    # used in client code to avoid catching all Exceptions!!
    EXCEPTIONS = [botocore.exceptions.BotoCoreError]

    def __init__(self, store_name: str, bucket: str | None = None):
        super().__init__(store_name, bucket)

        region = self._conf_val("REGION")

        # Backblaze B2 calls this "storage application key id"
        access_key_id = self._conf_val("ACCESS_KEY_ID")
        # Backblaze B2 calls this "storage application key"
        secret_access_key = self._conf_val("SECRET_ACCESS_KEY")
        endpoint_url = self.URL_FORMAT.format(region=region)

        self._s3 = boto3.client(
            "s3",
            endpoint_url=endpoint_url,
            aws_access_key_id=access_key_id,
            aws_secret_access_key=secret_access_key,
        )

    def upload_file(self, local_path: str, remote_key: str) -> None:
        # mypy says it doesn't return a value:
        self._s3.upload_file(local_path, self.bucket, remote_key)

    def upload_fileobj(self, fileobj: FileObj, remote_key: str) -> None:
        self._s3.upload_fileobj(Bucket=self.bucket, Key=remote_key, Fileobj=fileobj)

    def _key_generator(self, prefix: str) -> Generator[str, None, None]:
        """
        originally written as generator, keeping, just in case
        """
        marker = ""
        while True:
            res = self._s3.list_objects(
                Bucket=self.bucket, Prefix=prefix, Marker=marker
            )
            if "Contents" not in res:
                return
            for item in res["Contents"]:
                key = item["Key"]
                # XXX filter based on ending?
                logger.debug("match: %s", key)
                yield key
            if not res["IsTruncated"]:
                return
            marker = key  # see https://github.com/boto/boto3/issues/470
            logger.debug("object list truncated; next marker: %s", marker)
            if not marker:
                return

    def list_objects(self, prefix: str = "") -> list[str]:
        return list(self._key_generator(prefix))

    def download_file(self, remote_key: str, local_fname: str) -> None:
        self._s3.download_file(self.bucket, remote_key, local_fname)

    def download_fileobj(self, remote_key: str, fileobj: FileObj) -> None:
        self._s3.download_fileobj(Bucket=self.bucket, Key=remote_key, Fileobj=fileobj)


class B2Store(S3Store):
    """
    BackBlaze using S3 compatible API
    """

    PROVIDER = "B2"  # for config names
    URL_FORMAT = "https://s3.{region}.backblazeb2.com"

"""
Simple API for file transfer to/from archival storage sites.

Original use/purpose/need was to archive WARC files, and queuer had
its own S3 code to read input (csv) files from S3 (using s3: URLs) for
simplicity.

But when adding BackBlaze "B2" support, it became clear that
easy, parallel support for multiple providers was worth pursuing.

BlobStores should be viewed as read-only key/value byte object
stores and may lack "normal" filesystem ops like append, rename,
and may lack directory structure (and instead have only string
prefix matching).

The first provider implemented as S3, and the second completed
(BackBlaze B2) provides an S3 compatible API, so the method names
are exactly those provided by boto.

Only the operations needed/used are implemented.

There are two use models:

A program that reads or writes from/to particular, homogeneous
collections of store objects.  In this case the "store" argument, used
to find configuration might be the name of the application, or the
name of a store (ie; ARCHIVE).  The environment should contain
complete configuration for at least one set of STORE_PROVIDER_VARIABLE
variables, which MUST include a bucket name.  If the application
accesses more than one bucket, "store" should refer to the purpose of
each particular bucket.  The "blobstores()" function returns a list of
BlobStore instances for all providers for which there is complete
configuration.

A program that acesses existing objects (e.g. specified on the command
line) specified by PROVIDER://BUCKET/NAME_OR_PREFIX, and the BlobStore
object is acquired using "blobstore_by_url()".  The "store" name
should be either the name of the application (HIST for hist-queuer) or
of the class of applications the keys in the configuration applies to
(QUEUER).  The configuration need not specify a bucket, since it will
be included in the URL.  blobstore_by_url returns the "path" part of
the URL, which may be treated as a single object name, or a prefix
(ie; wildcard) used with "bs.list_objects(prefix)" to retrieve all
matching objects.
"""

import importlib
import logging
import os
import pkgutil
from typing import IO, Any, Type

FileObj = IO[Any]

logger = logging.getLogger(__name__)


def _conf_var(store_name: str, provider: str, conf_item: str) -> str:
    return f"{store_name.upper()}_{provider}_{conf_item}"


def _conf_val(store_name: str, provider: str, conf_item: str) -> str:
    """
    throws KeyError if value not configured
    """
    return os.environ[_conf_var(store_name, provider, conf_item)]


class BlobStore:
    """
    base class for BlobStore providers.
    """

    # used for config vars, and to build PROVIDERS map (for urls),
    PROVIDER: str

    # List of exceptions that methods can raise, used in client code
    # to avoid catching Exception!
    # There is no typing annotation for variable length heterogenous tuple!
    # so must use "except tuple(obj.EXCEPTIONS) as e:"
    EXCEPTIONS: list[Type[Exception]]

    def __init__(self, store_name: str, _bucket: str | None = None):
        self.store_name = store_name

        if _bucket:
            self.bucket = _bucket
        else:
            self.bucket = self._conf_val("BUCKET")

        # subclasses MUST raise KeyError if full config not available!

    def _conf_var(self, conf_item: str) -> str:
        return _conf_var(self.store_name, self.PROVIDER, conf_item)

    def _conf_val(self, conf_item: str) -> str:
        """
        throws KeyError if value not configured
        MAYBE look without provider as prefix????
        """
        return _conf_val(self.store_name, self.PROVIDER, conf_item)

    def upload_file(self, local_path: str, remote_key: str) -> None:
        raise NotImplementedError

    def list_objects(self, prefix: str) -> list[str]:
        raise NotImplementedError

    def download_file(self, key: str, local_fname: str) -> None:
        raise NotImplementedError

    def download_fileobj(self, key: str, fileobj: FileObj) -> None:
        raise NotImplementedError


PROVIDERS: dict[str, type[BlobStore]] = {}


class BlobStoreError(Exception):
    """
    class for BlobStore errors
    """


def _find_providers() -> None:
    if PROVIDERS:
        return

    # loop for all .py files in indexer.blobstore directory
    for finder, modname, ispkg in pkgutil.iter_modules(path=__path__):
        try:
            fullmodname = f"{__name__}.{modname}"
            module = importlib.import_module(fullmodname)
        except ImportError as e:
            # in case dependencies not installed for unused providers
            logger.debug("import of %s failed: %r", fullmodname, e)
            continue

        # look for all for BlobStore subclasses in module
        for name in dir(module):
            if name[0].isupper():
                cls = getattr(module, name)
                if (
                    name != "BlobStore"
                    and isinstance(cls, type)
                    and issubclass(cls, BlobStore)
                ):
                    # cls is a BlobStore subclass definition
                    logger.debug(
                        "found BlobStore provider %s class %s", cls.PROVIDER, name
                    )
                    PROVIDERS[cls.PROVIDER] = cls


def blobstores(store_name: str, max: int | None = None) -> list[BlobStore]:
    """
    return all blobstore providers that have complete configuration
    (including bucket).

    Config variables are of the form:
    {STORE_NAME}_{PROVIDER}_{VARIABLE_NAME}
    """
    results: list[BlobStore] = []

    _find_providers()
    for name, cls in PROVIDERS.items():
        logger.debug("trying BlobStore provider %s", name)
        try:
            bs = cls(store_name)  # instantiate class
            results.append(bs)
            if isinstance(max, int) and len(results) == max:
                return results
        except KeyError as e:
            logger.debug("missing config for BlobStore %s: %r", name, e)
            continue

    if len(results) == 0:
        logger.warning("no %s blobstores configured", store_name)
    return results


def blobstore(store_name: str) -> BlobStore | None:
    """
    backwards compatible call.
    will have no users after importer updated?
    """
    stores = blobstores(store_name, max=1)
    if len(stores) == 0:
        return None
    return stores[0]


def split_url(url: str) -> tuple[str, str, str]:
    """
    take schema://bucket/key
    return (schema, bucket, key) from URL
    """
    # will raise TypeError if :// not found
    schema, path = url.split("://", 1)
    match path.split("/", 1):
        case [bucket, key]:
            pass
        case [bucket]:
            key = ""
    return (schema, bucket, key)


def is_blobstore_url(path: str) -> bool:
    """
    return true if looks like a URL and schema has a known provider name
    """
    match path.split("://", 1):
        case [schema, _]:
            _find_providers()
            return schema.upper() in PROVIDERS
    return False


def blobstore_by_url(store: str, url: str) -> tuple[BlobStore, str, str]:
    """
    take provider://bucket/key...
    returns (BlobStore, schema, key_or_prefix)

    NOTE!
    * config for STORE_PROVIDER_XXX must have access to bucket in URL
    * key_or_prefix may be empty
    * key can be passed to bs.list_objects(key) for prefix expansion
    """

    schema, bucket, key = split_url(url)  # may raise TypeError

    _find_providers()
    cls = PROVIDERS.get(schema.upper())
    if cls is None:
        raise BlobStoreError(f"bad schema {schema}")
    bs = cls(store, bucket)  # may raise KeyError
    return (bs, schema, key)

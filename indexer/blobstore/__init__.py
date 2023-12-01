"""
Simple API for archive/cold storage.

Enhance as needed.

NOTE! Back ends like DO NOT have FULL filesystem semantics!
"""

import importlib
import logging
import os
import pkgutil
from typing import List, Optional, Type

logger = logging.getLogger(__name__)


class BlobStore:
    """
    base class for Providers
    """

    # for config var prefixes
    PROVIDER: str

    # List of exceptions that methods can raise.
    # There is no typing annotation for variable length heterogenous tuple!
    # so must use "except tuple(obj.EXCEPTIONS) as e:"
    EXCEPTIONS: List[Type[Exception]]

    def __init__(self, store_name: str):
        self.store_name = store_name
        # subclasses MUST raise KeyError if full config not available!

    def _conf_var(self, conf_item: str) -> str:
        return f"{self.store_name.upper()}_{self.PROVIDER}_{conf_item}"

    def _conf_val(self, conf_item: str) -> str:
        """
        throws KeyError if value not configured
        MAYBE look without provider as prefix????
        """
        return os.environ[self._conf_var(conf_item)]

    def store_from_local_file(self, local_path: str, remote_path: str) -> None:
        raise NotImplementedError


def blobstore(store_name: str) -> Optional[BlobStore]:
    """
    return the first blobstore provider that has complete configuration.

    store_name is a prefix for config vars for a specific use ie; ARCHIVER
    """
    for finder, modname, ispkg in pkgutil.iter_modules(path=__path__):
        try:
            n = f"{__name__}.{modname}"
            m = importlib.import_module(n)
        except ImportError as e:
            # in case dependencies not installed for unused providers
            logger.debug("import of %s failed: %r", n, e)
            continue

        # look for BlobStore subclasses in module and try to instantiate them!
        for name in dir(m):
            if name[0].isupper():
                value = getattr(m, name)
                if (
                    name != "BlobStore"
                    and isinstance(value, type)
                    and issubclass(value, BlobStore)
                ):
                    logger.debug("found BlobStore class %s", name)
                    try:
                        s = value(store_name)  # instantiate class
                        for attr in ("PROVIDER", "EXCEPTIONS"):  # paranoia
                            if not hasattr(s, attr):
                                raise AttributeError(f"{n}.{name} missing {attr}")
                        return s
                    except (ImportError, KeyError) as e:
                        logger.debug("could not instantiate BlobStore %s: %r", name, e)
                        continue

    logger.warning("no %s blobstore configuration?", store_name)
    return None

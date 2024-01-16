"""
Simple API for file transfer to archive/cold storage.
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

    def authorize(self) -> None:
        """
        place authorization code here!
        """

    def store_from_local_file(self, local_path: str, remote_path: str) -> None:
        raise NotImplementedError


class BlobStoreError(RuntimeError):
    """
    class for BlobStore errors
    """


def blobstore(store_name: str) -> Optional[BlobStore]:
    """
    return the first blobstore provider that has complete configuration.

    store_name is a prefix for config vars for a specific use ie; ARCHIVER
    """
    for finder, modname, ispkg in pkgutil.iter_modules(path=__path__):
        try:
            fullmodname = f"{__name__}.{modname}"
            module = importlib.import_module(fullmodname)
        except ImportError as e:
            # in case dependencies not installed for unused providers
            logger.debug("import of %s failed: %r", fullmodname, e)
            continue

        # look for BlobStore subclasses in module and try to instantiate them!
        for name in dir(module):
            if name[0].isupper():
                value = getattr(module, name)
                if (
                    name != "BlobStore"
                    and isinstance(value, type)
                    and issubclass(value, BlobStore)
                ):
                    logger.debug("found BlobStore class %s", name)
                    try:
                        bs = value(store_name)  # instantiate class
                    except ImportError as e:
                        logger.debug("error importing BlobStore %s: %r", name, e)
                        continue
                    except KeyError as e:
                        logger.debug("missing config for BlobStore %s: %r", name, e)
                        continue

                    try:
                        bs.authorize()
                        return bs
                    except tuple(value.EXCEPTIONS) as e:
                        logger.debug("error authorizng BlobStore %s: %r", name, e)
                    except KeyError as e:
                        logger.debug("missing config for BlobStore %s: %r", name, e)

    logger.warning("no working blobstore for %s", store_name)
    return None

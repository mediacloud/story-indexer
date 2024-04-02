"""
Black magic incantations for reqeusts to allow fetching from as
many sites as possible.

BE VERY CAREFUL making any changes, because it can/will effect what
sites will talk to us!!!

YOU HAVE BEEN WARNED!!!

ALSO: some mypy complaints have been disabled.  Getting the type
signatures right could take hours, and the results would likely be
easily broken by new versions of Python.

In a file of it's own:
1. To hide the nastiness
2. _could_ be pushed into mcmetadata, and used by rss-fetcher???
"""

import logging
import ssl
from typing import MutableMapping

import requests
import urllib3
from mcmetadata.webpages import MEDIA_CLOUD_USER_AGENT
from requests.structures import CaseInsensitiveDict

# Headers as close as possible to Scrapy (only Host is out of place).
HEADERS: MutableMapping[str, str | bytes] = CaseInsensitiveDict(
    {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9",
        "Accept-Language": "en",
        "User-Agent": MEDIA_CLOUD_USER_AGENT,
        "Accept-Encoding": "gzip, deflate",
        # NOTE: Both Connection: close and keep-alive cause npr Akamai https connections to hang!!
        # (http connections seem to hang regardless)
    }
)

logger = logging.getLogger(__name__)


# https://stackoverflow.com/questions/71603314/ssl-error-unsafe-legacy-renegotiation-disabled
class CustomHttpAdapter(requests.adapters.HTTPAdapter):
    """
    Transport adapter" that allows us to use custom ssl_context.
    """

    def __init__(self, ssl_context=None, **kwargs):  # type: ignore[no-untyped-def]
        self.ssl_context = ssl_context
        super().__init__(**kwargs)

    def init_poolmanager(self, connections: int, maxsize: int, block: bool = False) -> None:  # type: ignore[override]
        self.poolmanager = urllib3.poolmanager.PoolManager(
            num_pools=connections,
            maxsize=maxsize,
            block=block,
            ssl_context=self.ssl_context,
        )


def legacy_ssl_session() -> requests.Session:
    """
    return a requests "session" object that behaves like OpenSSL 1.1.1
    while using OpenSSL 3.0, in order to match Scrapy behavior (and
    most browsers).  DON'T USE THIS IF YOU WANT TO KEEP DATA PRIVATE!!
    """
    ctx = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
    ctx.options |= int(getattr(ssl, "OP_LEGACY_SERVER_CONNECT", 0x4))

    # https://stackoverflow.com/questions/33770129/how-do-i-disable-the-ssl-check-in-python-3-x
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE

    session = requests.session()
    # maybe change retry count (since we retry in an hour)??
    session.mount("https://", CustomHttpAdapter(ctx))
    session.headers = HEADERS

    return session


# https://secariolabs.com/logging-raw-http-requests-in-python/
def log_http_requests() -> None:
    """
    calling this function once to patch code paths so that HTTP
    request data is logged.  This is useful to see what is being sent.
    """
    import http

    old_send = http.client.HTTPConnection.send

    def new_send(self, data):  # type: ignore[no-untyped-def]
        logger.debug("HTTP %s", data.decode("utf-8").strip())
        return old_send(self, data)

    http.client.HTTPConnection.send = new_send  # type: ignore[method-assign]

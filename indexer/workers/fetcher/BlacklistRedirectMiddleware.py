from typing import Any
from urllib.parse import urljoin, urlparse

import tldextract
from mcmetadata.urls import NON_NEWS_DOMAINS
from scrapy.downloadermiddlewares.redirect import (
    BaseRedirectMiddleware,
    _build_redirect_request,
)
from scrapy.exceptions import IgnoreRequest, NotConfigured
from scrapy.http import HtmlResponse
from scrapy.utils.httpobj import urlparse_cached
from scrapy.utils.response import get_meta_refresh
from w3lib.url import safe_url_string


class BlacklistRedirectMiddleware(BaseRedirectMiddleware):  # type: ignore[no-any-unimported]
    """
    Handle redirection of requests based on response status
    and meta-refresh html tag. Very Minor Edit of scrapy's builtin RedirectMiddleware
    """

    def process_response(self, request: Any, response: Any, spider: Any) -> Any:
        if (
            request.meta.get("dont_redirect", False)
            or response.status in getattr(spider, "handle_httpstatus_list", [])
            or response.status in request.meta.get("handle_httpstatus_list", [])
            or request.meta.get("handle_httpstatus_all", False)
        ):
            return response

        allowed_status = (301, 302, 303, 307, 308)
        if "Location" not in response.headers or response.status not in allowed_status:
            return response

        location = safe_url_string(response.headers["Location"])
        if response.headers["Location"].startswith(b"//"):
            request_scheme = urlparse(request.url).scheme
            location = request_scheme + "://" + location.lstrip("/")

        redirected_url = urljoin(request.url, location)

        tld = tldextract.extract(redirected_url)
        if f"{tld.domain}.{tld.suffix}" in NON_NEWS_DOMAINS:
            raise IgnoreRequest("Redirect to blacklisted domain")

        if response.status in (301, 307, 308) or request.method == "HEAD":
            redirected = _build_redirect_request(request, url=redirected_url)
            return self._redirect(redirected, request, spider, response.status)

        redirected = self._redirect_request_using_get(request, redirected_url)
        return self._redirect(redirected, request, spider, response.status)

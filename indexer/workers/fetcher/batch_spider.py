import csv
import datetime
import logging
from pathlib import Path
from typing import Any, Callable, Dict, Generator, List

import scrapy
from scrapy.crawler import CrawlerProcess
from scrapy.http import Response
from scrapy.spidermiddlewares.httperror import HttpError
from twisted.python.failure import Failure

from indexer.story import BaseStory
from indexer.workers.fetcher.rss_utils import RSSEntry

logger = logging.getLogger(__name__)


class BatchSpider(scrapy.Spider):  # type: ignore[no-any-unimported]
    """
    This spider is given a batch_index, loads the corresponding batchfile from the disk,
    then fetches the urls in that batch's stories, and then saves the corresponding html and http_metadata
    back to disk using the Story interface
    """

    name: str = "BatchSpider"

    custom_settings: Dict[str, Any] = {
        "COOKIES_ENABLED": False,
        "AUTOTHROTTLE_ENABLED": True,
        "AUTOTHROTTLE_DEBUG": True,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 2,
        "AUTOTHROTTLE_TARGET_CONCURRENCY": 10,
        # donut bother with retrying on 500s
        "RETRY_HTTP_CODES": [502, 503, 504, 522, 524, 408, 429],
        "USER_AGENT": "mediacloud bot for open academic research (+https://mediacloud.org)",
    }

    def __init__(
        self, batch: List[BaseStory], cb: Callable, *args: List, **kwargs: Dict
    ) -> None:
        super(BatchSpider, self).__init__(*args, **kwargs)

        self.batch = batch
        self.cb = cb

    def start_requests(self) -> Generator:
        for story in self.batch:
            url = story.rss_entry().link

            yield scrapy.Request(
                url=url,
                callback=self.parse,
                errback=self.on_error,
                cb_kwargs={"story": story},
            )

    # Using ANY for now here as well, just while the import situation is unclear
    def parse(self, response: Any, story: BaseStory) -> None:
        with story.raw_html() as raw_html:
            raw_html.html = response.body
            raw_html.encoding = response.encoding

        with story.http_metadata() as http_metadata:
            http_metadata.response_code = response.status
            http_metadata.final_url = response.url
            http_metadata.encoding = response.encoding
            http_metadata.fetch_timestamp = datetime.datetime.now().timestamp()

        self.cb(story)

    # Any here because I can't quite crack how the twisted failure object is scoped in this context
    def on_error(self, failure: Any) -> None:
        if failure.check(HttpError):
            story = failure.request.cb_kwargs["story"]

            with story.http_metadata() as http_metadata:
                http_metadata.response_code = failure.value.response.status
                http_metadata.fetch_timestamp = datetime.datetime.now().timestamp()

            self.cb(story)

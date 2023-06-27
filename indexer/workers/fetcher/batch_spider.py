import csv
import datetime
import logging
from typing import Any, Dict, Generator, List

import scrapy
from scrapy.crawler import CrawlerProcess
from scrapy.http import Response
from scrapy.spidermiddlewares.httperror import HttpError
from twisted.python.failure import Failure

from indexer.app import App
from indexer.path import DATAPATH_BY_DATE
from indexer.story import BaseStory, DiskStory

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
    }

    def __init__(
        self, fetch_date: str, batch_index: int, *args: List, **kwargs: Dict
    ) -> None:
        super(BatchSpider, self).__init__(*args, **kwargs)
        self.batch_index = batch_index

        data_path = DATAPATH_BY_DATE(fetch_date)
        batch_path = data_path + f"batch_{batch_index}.csv"

        self.batch = []
        logger.info(f"Loading batch {self.batch_index} from disk")
        with open(batch_path, "r") as batch_file:
            batch_reader = csv.DictReader(batch_file)
            for row in batch_reader:
                self.batch.append(str.encode(row["serialized_story"]))

    def start_requests(self) -> Generator:
        for entry in self.batch:
            story = DiskStory.load(entry)
            url = story.rss_entry().link

            yield scrapy.Request(
                url=url,
                callback=self.parse,
                errback=self.on_error,
                cb_kwargs={"entry": entry},
            )

    # Using ANY for now here as well, just while the import situation is unclear
    def parse(self, response: Any, entry: bytes) -> None:
        story = DiskStory.load(entry)

        with story.raw_html() as raw_html:
            raw_html.html = response.body

        with story.http_metadata() as http_metadata:
            http_metadata.response_code = response.status
            http_metadata.fetch_timestamp = datetime.datetime.now().timestamp()

    # Any here because I can't quite crack how the twisted failure object is scoped in this context
    def on_error(self, failure: Any) -> None:
        if failure.check(HttpError):
            entry = failure.request.cb_kwargs["entry"]
            story = DiskStory.load(entry)
            with story.http_metadata() as http_metadata:
                http_metadata.response_code = failure.value.response.status
                http_metadata.fetch_timestamp = datetime.datetime.now().timestamp()

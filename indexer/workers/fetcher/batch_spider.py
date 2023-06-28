import csv
import datetime
import logging
from pathlib import Path
from typing import Any, Dict, Generator, List

import scrapy
from scrapy.crawler import CrawlerProcess
from scrapy.http import Response
from scrapy.spidermiddlewares.httperror import HttpError
from twisted.python.failure import Failure

from indexer.app import App
from indexer.path import DATAPATH_BY_DATE
from indexer.story import BaseStory, DiskStory, StoryFactory, uuid_by_link
from indexer.workers.fetcher.rss_utils import RSSEntry

logger = logging.getLogger(__name__)

Story = StoryFactory()


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
        self.batch_path = data_path + f"batch_{batch_index}"

        self.batch = []
        logger.info(f"Loading batch {self.batch_index} from disk")
        with open(self.batch_path + ".csv", "r") as batch_file:
            batch_reader = csv.DictReader(batch_file)
            for row in batch_reader:
                entry = row
                self.batch.append(entry)

    def start_requests(self) -> Generator:
        for entry in self.batch:
            story_loc = self.batch_path + "/" + uuid_by_link(entry["link"])
            serialized = Path(story_loc).read_bytes()

            story = Story.load(serialized)
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

        with story.http_metadata() as http_metadata:
            http_metadata.response_code = response.status
            http_metadata.fetch_timestamp = datetime.datetime.now().timestamp()

        uuid = story.uuid()
        assert isinstance(uuid, str)
        save_loc = self.batch_path + "/" + uuid
        Path(save_loc).write_bytes(story.dump())

    # Any here because I can't quite crack how the twisted failure object is scoped in this context
    def on_error(self, failure: Any) -> None:
        if failure.check(HttpError):
            story = failure.request.cb_kwargs["story"]

            with story.http_metadata() as http_metadata:
                http_metadata.response_code = failure.value.response.status
                http_metadata.fetch_timestamp = datetime.datetime.now().timestamp()

            save_loc = self.batch_path + "/" + story.uuid()
            Path(save_loc).write_bytes(story.dump())

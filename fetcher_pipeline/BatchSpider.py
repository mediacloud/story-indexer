import scrapy
from scrapy.crawler import CrawlerProcess
from scrapy.spidermiddlewares.httperror import HttpError

import datetime

from pathlib import Path

import json
from filesystem.filesystem_interface import pipeline_filesystem_interface
from filesystem.state import BatchState, WorkState


class BatchSpider(scrapy.Spider):
    '''This spider loads a batch of urls from file, then runs wild on it.
    '''
    name = "main"

    custom_settings = {
        'COOKIES_ENABLED': False,
        'AUTOTHROTTLE_ENABLED': True,
        'AUTOTHROTTLE_DEBUG': True,
        'CONCURRENT_REQUESTS_PER_DOMAIN': 2,
        'AUTOTHROTTLE_TARGET_CONCURRENCY': 10,
        # donut bother with retrying on 500s
        'RETRY_HTTP_CODES': [502, 503, 504, 522, 524, 408, 429]
    }

    def __init__(self, date, batch_index, limit=None, send_items=None, chan=None, *args, **kwargs):
        super(BatchSpider, self).__init__(*args, **kwargs)
        self.fs = pipeline_filesystem_interface(date)
        self.batch_index = batch_index
        self.limit = limit
        self.send_items = send_items
        self.chan = chan

    def start_requests(self):
        # Some kind of logging utility goes here
        # Path(f"work/batch_{self.batch_index}_start_tstamp").write_text(str(datetime.datetime.now().timestamp()))

        # Get the batch, and update all the neccesary flags.
        batch = self.fs.get_batch(self.batch_index)

        print(f"Found a batch of size {len(batch)}")

        if self.limit == None:
            self.limit = len(batch)

        i = -1
        for entry in batch[:self.limit]:
            i += 1
            yield scrapy.Request(url=entry["link"],
                                 callback=self.parse,
                                 errback=self.on_error,
                                 cb_kwargs={"rss_entry": entry})

    def parse(self, response, rss_entry=None):
        if response.status < 400:
            raw_html = response.body

            original_link = rss_entry["link"]

            meta = {
                "response_status": response.status,
                "fetch_timestamp": datetime.datetime.now().timestamp(),
                "rss_entry": rss_entry,
                "fetch_batch": self.batch_index
            }

            # Store HTML and http meta in filesystem
            self.fs.put_fetched(original_link, raw_html, meta)

            # Then put the http meta in the queue
            self.send_items(self.chan, [original_link])

    def on_error(self, failure):

        if failure.check(HttpError):
            rss_entry = failure.request.cb_kwargs["rss_entry"]
            meta = {
                "response_status": failure.value.response.status,
                "fetch_timestamp": datetime.datetime.now().timestamp(),
                "rss_entry": rss_entry,
                "fetch_batch": self.batch_index
            }
            original_link = rss_entry["link"]

            self.fs.put_fetch_error(original_link, meta)

"""
metadata parser pipeline worker
"""

import datetime as dt
import gzip
import json
import logging

# PyPI:
import mcmetadata
from pika.adapters.blocking_connection import BlockingChannel

# local:
from indexer.story import BaseStory
from indexer.worker import StoryWorker, run

logger = logging.getLogger(__name__)


class Parser(StoryWorker):
    def process_story(
        self,
        chan: BlockingChannel,
        story: BaseStory,
    ) -> None:
        rss = story.rss_entry()
        raw = story.raw_html()

        link = rss.link
        if link:
            # XXX want Story method to retrieve unicode string!!
            if raw.html:
                html = raw.html.decode("utf-8")  # XXX wrong!
            else:
                html = ""  # XXX error?

            # metadata dict
            # may raise mcmetadata.exceptions.BadContentError
            mdd = mcmetadata.extract(link, html)

            with story.content_metadata() as cmd:
                # XXX assumes identical item names!!
                #       could copy items individually with type checking
                #       if mcmetadata returned TypedDict?
                for key, val in mdd.items():
                    setattr(cmd, key, val)
        # XXX else quarantine?!

        self.send_story(chan, story)
        self.incr("parsed-stories")


if __name__ == "__main__":
    run(Parser, "parser", "metadata parser worker")

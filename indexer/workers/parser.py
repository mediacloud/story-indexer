"""
metadata parser pipeline worker
"""

import datetime as dt
import gzip
import json
import logging

import chardet

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
                result = chardet.detect(raw.html)
                en_type = result["encoding"]
                if en_type:
                    html = raw.html.decode(en_type)
            else:
                html = ""  # XXX error?

            # metadata dict
            # may raise mcmetadata.exceptions.BadContentError
            mdd = mcmetadata.extract(link, html)
            logger.info(f"MDD keys {mdd.keys()}")

            with story.content_metadata() as cmd:
                keys_to_skip = ["text_extraction_method", "version"]
                # XXX assumes identical item names!!
                #       could copy items individually with type checking
                #       if mcmetadata returned TypedDict?
                for key, val in mdd.items():
                    if key not in keys_to_skip:
                        setattr(cmd, key, val)
        # XXX else quarantine?!

        self.send_story(chan, story)


if __name__ == "__main__":
    run(Parser, "parser", "metadata parser worker")

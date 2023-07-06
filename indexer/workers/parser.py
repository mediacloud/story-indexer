"""
metadata parser pipeline worker
"""

import logging

# PyPI:
import mcmetadata
from pika.adapters.blocking_connection import BlockingChannel

# local:
from indexer.story import BaseStory
from indexer.worker import StoryWorker, run

logger = logging.getLogger(__name__)


def parse(story: BaseStory) -> bool:
    """
    Separate function for easy exit, non-queue usage, unit testing.
    False return indicates an permanent error (one that won't go away on retry).
    """
    rss = story.rss_entry()

    link = rss.link
    if not link:
        return False

    raw = story.raw_html()
    html = raw.unicode
    if html is None:
        return False

    # metadata dict
    # may raise mcmetadata.exceptions.BadContentError
    mdd = mcmetadata.extract(link, html)

    with story.content_metadata() as cmd:
        # XXX assumes identical item names!!
        #       could copy items individually with type checking
        #       if mcmetadata returned TypedDict?
        for key, val in mdd.items():
            setattr(cmd, key, val)

    return True


class Parser(StoryWorker):
    def process_story(
        self,
        chan: BlockingChannel,
        story: BaseStory,
    ) -> None:
        parse(story)  # XXX check return, and quarantine?!
        self.send_story(chan, story)


if __name__ == "__main__":
    run(Parser, "parser", "metadata parser worker")

"""
metadata parser pipeline worker
"""

import logging

# PyPI:
import mcmetadata
from pika.adapters.blocking_connection import BlockingChannel

# local:
from indexer.story import BaseStory
from indexer.worker import QuarantineException, StoryWorker, run

logger = logging.getLogger(__name__)


class Parser(StoryWorker):
    def process_story(
        self,
        chan: BlockingChannel,
        story: BaseStory,
    ) -> None:
        rss = story.rss_entry()
        raw = story.raw_html()

        link = rss.link  # XXX prefer final URL??
        if not link:
            raise QuarantineException("no link")

        html = raw.unicode
        if not html:
            raise QuarantineException("no html")

        logger.info("parsing %s: %d characters", link, len(html))

        # metadata dict
        # may raise mcmetadata.exceptions.BadContentError
        #   What to do?
        #     translate to QuarantineException (with loss of detail),
        #     call (_)quarantine directly (with exception),
        #     or let fail from repeated retries???
        try:
            mdd = mcmetadata.extract(link, html)
        except mcmetadata.exceptions.BadContentError as e:
            raise QuarantineException(getattr(e, "message", repr(e)))

        extraction_label = mdd["text_extraction_method"]

        # Really slapdash solution for the sake of testing.
        if mdd["publication_date"] is not None:
            mdd["publication_date"] = mdd["publication_date"].strftime("%Y-%m-%d")
        else:
            mdd["publication_date"] = "None"

        logger.info("parsed %s with %s date %s", link, extraction_label, mdd["publication_date"])

        with story.content_metadata() as cmd:
            # XXX assumes identical item names!!
            #       could copy items individually with type checking
            #       if mcmetadata returned TypedDict?
            for key, val in mdd.items():
                if hasattr(cmd, key):  # avoid hardwired exceptions list?!
                    setattr(cmd, key, val)

        self.send_story(chan, story)
        self.incr("parsed-stories", labels=[("method", extraction_label)])


if __name__ == "__main__":
    run(Parser, "parser", "metadata parser worker")

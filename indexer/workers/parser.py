"""
metadata parser pipeline worker
"""

import logging

# PyPI:
import mcmetadata

# local:
from indexer.story import BaseStory
from indexer.worker import QuarantineException, StorySender, StoryWorker, run

logger = logging.getLogger("parser")


class Parser(StoryWorker):
    def process_story(self, sender: StorySender, story: BaseStory) -> None:
        def story_status(status: str) -> None:
            # one place for label format:
            self.incr("stories", labels=[("status", status)])

        hmd = story.http_metadata()
        final_url = hmd.final_url
        if not final_url:
            rss = story.rss_entry()
            # want notice level!
            logger.warning("rss link %s final_url %r", rss.link, final_url)
            story_status("no-url")
            raise QuarantineException("no url")

        raw = story.raw_html()
        try:
            html = raw.unicode
        except UnicodeError:
            # careful printing exception! may contain entire string!!
            logger.warning("unicode error: %s", final_url)  # want notice!
            story_status("unicode")
            # PLB: noone has seemed concerned, so discarding
            return

        if not html:
            logger.warning("no HTML: %s", final_url)  # want notice!
            story_status("no-html")
            raise QuarantineException("no html")

        logger.info("parsing %s: %d characters", final_url, len(html))

        try:
            mdd = mcmetadata.extract(final_url, html)
        except mcmetadata.exceptions.BadContentError:
            story_status("too-short")
            # No quarantine error here, just discard
            return

        # Really slapdash solution for the sake of testing
        # ^^^^ is this still the case, or is it now "standard operating procedure"??
        if mdd["publication_date"] is not None:
            pub_date = mdd["publication_date"].strftime("%Y-%m-%d")
        else:
            pub_date = "None"
        mdd["publication_date"] = pub_date

        method = mdd["text_extraction_method"]
        logger.info("parsed %s with %s date %s", final_url, method, pub_date)

        with story.content_metadata() as cmd:
            # assumes identical item names!!
            #       could copy items individually with type checking
            #       if mcmetadata returned TypedDict?
            # NOTE! url and final_url are only different if
            # mcmetadata.extract() does the fetch
            for key, val in mdd.items():
                if hasattr(cmd, key):  # avoid hardwired exceptions list?!
                    setattr(cmd, key, val)

        sender.send_story(story)
        story_status(f"OK-{method}")


if __name__ == "__main__":
    run(Parser, "parser", "metadata parser worker")

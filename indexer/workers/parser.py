"""
metadata parser pipeline worker
"""

import logging
from collections import Counter

# PyPI:
import mcmetadata

# local:
from indexer.story import BaseStory
from indexer.worker import QuarantineException, StorySender, StoryWorker, run

QUARANTINE_DECODE_ERROR = True  # discard if False

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
        except UnicodeError as e:
            # careful printing exception! may contain entire string!!
            err = type(e).__name__
            logger.warning("unicode error %s: %s", err, final_url)  # want notice!
            story_status("no-decode")
            if QUARANTINE_DECODE_ERROR:
                raise QuarantineException(err)
            else:
                return

        if not html:
            logger.warning("no HTML: %s", final_url)  # want notice!
            story_status("no-html")
            raise QuarantineException("no html")

        logger.info("parsing %s: %d characters", final_url, len(html))

        extract_stats: Counter[str] = Counter()
        try:
            mdd = mcmetadata.extract(final_url, html, stats_accumulator=extract_stats)
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

        skip_items = {"total", "fetch"}  # stackable, no fetch done
        for item, sec in extract_stats.items():
            if item not in skip_items:
                # was tempted to replace 'content' with method,
                # but is really sum of all methods tried!!
                self.timing("extract", sec * 1000, labels=[("step", item)])


if __name__ == "__main__":
    run(Parser, "parser", "metadata parser worker")

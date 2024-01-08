"""
metadata parser pipeline worker
"""

import logging
from collections import Counter

# PyPI:
import mcmetadata
from bs4.dammit import UnicodeDammit

# local:
from indexer.app import run
from indexer.story import BaseStory
from indexer.storyapp import StorySender, StoryWorker
from indexer.worker import QuarantineException

QUARANTINE_DECODE_ERROR = True  # discard if False

logger = logging.getLogger("parser")


class Parser(StoryWorker):
    def process_story(self, sender: StorySender, story: BaseStory) -> None:
        hmd = story.http_metadata()
        final_url = hmd.final_url
        if not final_url:
            rss = story.rss_entry()
            # want notice level!
            # should NOT have gotten here
            logger.warning("rss link %s final_url %r", rss.link, final_url)
            self.incr_stories("no-url", final_url or "")
            raise QuarantineException("no url")

        # Deal with the character encoding miasma.

        # HTTP Content-Type header can be plain wrong (just a http
        # server configuration), or bad ("iso-utf-8"), and chardet
        # makes bad guesses with some frequency, when the HTML <meta>
        # tag says utf-8.

        # Placing this in RawHtml.unicode and guess_encoding() means
        # that the routines may want to alter the RawHtml object,
        # which requires a "with" context, when the caller might have
        # already entered one, and hides data alteration from the
        # user.  If it turns out that this needs to be dealt with in
        # multiple places, reconsider.

        # scrapy uses their own w3lib, which may be more all
        # encompasing, but it's less easy to use (would require
        # cribbing code from scrapy). requests has a deprecated
        # get_encodings_from_content function, which has moved to
        # requests_toolbelt.utils.deprecated.get_encodings_from_content().

        # Since "historical" HTML (saved on AWS S3 by legacy system)
        # needs this (doesn't even come with Content-Type info), and
        # any fetcher using "requests" will need this as well, and
        # since it's a purely computational bound operation rather
        # than an I/O bound operation, it seems to belong here, rather
        # than in multiple fetchers.

        # ALSO: the user-friendly dammit interface seems to be
        # UnicodeDammit, which returns the decoded string as well as
        # the encoding, so might as well put the call where we use the
        # result.

        with self.timer("encoding"):
            raw = story.raw_html()
            encoding = raw.encoding or hmd.encoding
            if encoding:
                kde = [encoding]
            else:
                kde = None
            try:
                raw_html = raw.html or ""
                ud = UnicodeDammit(raw_html, is_html=True, known_definite_encodings=kde)
            except UnicodeError as e:
                # careful printing exception! may contain entire string!!
                err = type(e).__name__
                self.incr_stories("no-decode", final_url)  # want level=NOTICE
                if QUARANTINE_DECODE_ERROR:
                    raise QuarantineException(err)
                else:
                    return

        # XXX also unicode_markup??
        html = ud.markup  # decoded HTML
        logger.info("parsing %s: %d characters", final_url, len(html))
        if not html:
            # should NOT have happened!
            self.incr_stories("no-html", final_url)  # want level=NOTICE
            raise QuarantineException("no html")

        with raw:
            # Scrapy removes BOM, so do it here too.
            # this will happen with historical data from S3
            # and HTML fetched w/ requests.
            if ud.detector.markup:
                old_len = len(raw_html)
                new_len = len(ud.detector.markup)
                if new_len != old_len:
                    # maybe log "BOM removed" and give URL?
                    logger.debug("new len %d, was %d", new_len, old_len)
                    raw.html = ud.detector.markup

            encoding = ud.original_encoding
            if encoding and raw.encoding != encoding:
                logger.debug("encoding %s, was %s", encoding, raw.encoding)
                raw.encoding = encoding

        extract_stats: Counter[str] = Counter()
        try:
            mdd = mcmetadata.extract(final_url, html, stats_accumulator=extract_stats)
        except mcmetadata.exceptions.BadContentError:
            self.incr_stories("too-short", final_url)
            # No quarantine error here, just discard
            return

        # change datetime object to JSON-safe string
        if mdd["publication_date"] is not None:
            pub_date = mdd["publication_date"].strftime("%Y-%m-%d")
        else:
            pub_date = "None"  # XXX change this to None??? Please!!!
        mdd["publication_date"] = pub_date

        method = mdd["text_extraction_method"]
        logger.info("parsed %s with %s date %s", final_url, method, pub_date)

        with story.content_metadata() as cmd:
            for key, val in mdd.items():
                if hasattr(cmd, key):  # avoid hardwired exceptions
                    setattr(cmd, key, val)

        sender.send_story(story)
        self.incr_stories(f"OK-{method}", final_url)

        skip_items = {"total", "fetch"}  # stackable, no fetch done
        for item, sec in extract_stats.items():
            if item not in skip_items:
                # was tempted to replace 'content' with method,
                # but is really sum of all methods tried!!
                self.timing("extract", sec * 1000, labels=[("step", item)])


if __name__ == "__main__":
    run(Parser, "parser", "metadata parser worker")

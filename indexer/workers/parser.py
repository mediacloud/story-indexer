"""
metadata parser pipeline worker
"""

import datetime as dt
import io
import logging
from collections import Counter
from typing import Any

# PyPI:
import mcmetadata
from bs4.dammit import UnicodeDammit

# xml.etree.ElementTree.iterparse doesn't have recover argument
# (fatal error on control characters in element text), so using lxml
from lxml.etree import iterparse

# local:
from indexer.app import run
from indexer.story import NEED_CANONICAL_URL, BaseStory
from indexer.storyapp import StorySender, StoryWorker
from indexer.worker import QuarantineException

QUARANTINE_DECODE_ERROR = True  # discard if False
FEED_TAGS = ("rss", "feed", "rdf", "channel")  # must be tuple for startswith

logger = logging.getLogger("parser")


class CannotDecode(Exception):
    """could not decode"""


class Parser(StoryWorker):
    def _log_url(self, story: BaseStory) -> str:
        url = story.http_metadata().final_url
        if url and url != NEED_CANONICAL_URL:
            return url
        return story.rss_entry().link or "UNKNOWN"

    def _decode_content(self, story: BaseStory) -> str:
        """
        Deal with the character encoding miasma.
        Returns str for document body
        """

        hmd = story.http_metadata()
        log_url = self._log_url(story)

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
                self.incr_stories("no-decode", log_url)  # want level=NOTICE
                if QUARANTINE_DECODE_ERROR:
                    raise QuarantineException(err)
                else:
                    raise CannotDecode()

        # XXX also ud.unicode_markup??
        html = ud.markup  # decoded HTML
        assert isinstance(html, str)

        logger.info("parsing %s: %d characters", log_url, len(html))
        if not html:
            # can get here from batch fetcher, or if body was just a BOM
            self.incr_stories("no-html", log_url)  # want level=NOTICE
            raise CannotDecode("no-html")

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

        return html

    def _save_metadata(self, story: BaseStory, mdd: dict[str, Any], html: str) -> bool:
        # XXX check for empty text_content?
        # (will be discarded by importer)

        cmd = story.content_metadata()
        with cmd:
            for key, val in mdd.items():
                if hasattr(cmd, key):  # avoid hardwired exceptions
                    setattr(cmd, key, val)

            cmd.parsed_date = dt.datetime.utcnow().isoformat()

        return True

    def _extract_canonical_url(self, story: BaseStory) -> str:
        """
        Here with story from Nov/Dec 2021, pulled from S3 without URL.
        Some S3 objects are feeds, and cause loooong parse times (over
        30 minutes), causing RabbitMQ to close connection, so try to
        detect feeds, and extract canonical URL at the same time.

        Returns empty string after counting and logging stories to discard.
        """
        html_bytes = story.raw_html().html or b""
        # XXX err if no content?

        first = True
        canonical_url = og_url = ""
        for event, element in iterparse(
            io.BytesIO(html_bytes),
            events=(
                "start",
                "end",
            ),
            recover=True,
        ):
            if first:
                logger.debug("first tag %s", element.tag)
                if element.tag in FEED_TAGS:
                    self.incr_stories("feed", self._log_url(story))
                    return ""
                first = False
            if event == "end":
                if element.tag == "head":
                    break
                if element.tag == "link":
                    if not canonical_url and element.get("rel") == "canonical":
                        canonical_url = element.get("href")
                elif element.tag == "meta":
                    if not og_url and element.get("property") == "og:url":
                        og_url = element.get("content")
        # end for event: prefer canonical_url
        canonical_url = canonical_url or og_url
        # XXX need html.unescape??  seems not?
        if not canonical_url:
            self.incr_stories("no-canonical-url", self._log_url(story))
            return ""

        logger.info("%s: canonical URL %s", story.rss_entry().link, canonical_url)

        # now that we FINALLY have a URL, make sure it isn't
        # from a source we filter out!!!
        if not self.check_story_url(canonical_url):
            return ""

        with story.http_metadata() as hmd:
            hmd.final_url = canonical_url
        return canonical_url

    def process_story(self, sender: StorySender, story: BaseStory) -> None:
        final_url = story.http_metadata().final_url
        if final_url is None:
            raise QuarantineException("no final_url")

        if final_url == NEED_CANONICAL_URL:
            if not (final_url := self._extract_canonical_url(story)):
                return  # logged and counted

        try:
            html = self._decode_content(story)
        except CannotDecode:
            return  # already counted

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
            pub_date = None
        mdd["publication_date"] = pub_date

        if not self._save_metadata(story, mdd, html):
            # here only when failed to get needed canonical_url
            # already counted and logged
            return

        # NOTE! after save_metadata, to get canonical_url if possible
        log_url = self._log_url(story)
        method = mdd["text_extraction_method"]
        logger.info("parsed %s with %s date %s", log_url, method, pub_date)
        self.incr_stories(f"OK-{method}", log_url)

        skip_items = {"total", "fetch"}  # stackable, no fetch done
        for item, sec in extract_stats.items():
            if item not in skip_items:
                # was tempted to replace 'content' with method,
                # but is really sum of all methods tried!!
                self.timing("extract", sec * 1000, labels=[("step", item)])

        # at very end: any unhandled exception would cause requeue
        sender.send_story(story)


if __name__ == "__main__":
    run(Parser, "parser", "metadata parser worker")

"""
metadata parser pipeline worker
"""

import datetime as dt
import logging
from collections import Counter
from typing import Any

# PyPI:
import mcmetadata
from bs4.dammit import UnicodeDammit

# local:
from indexer.app import run
from indexer.story import NEED_CANONICAL_URL, BaseStory
from indexer.storyapp import StorySender, StoryWorker
from indexer.worker import QuarantineException

QUARANTINE_DECODE_ERROR = True  # discard if False

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

        hmd = story.http_metadata()
        if hmd.final_url == NEED_CANONICAL_URL:
            # should only get here with S3 object id in rss.link
            # (historical data from S3 for Nov/Dec 2021 w/o CSV file)
            link = story.rss_entry().link or "SNH"  # for logging

            canonical_url = cmd.canonical_url
            if not canonical_url or canonical_url == NEED_CANONICAL_URL:
                # could have been a saved feed document, so try and
                # give observers and idea of what's going on!
                top = html[:2048].lower()
                first_gt = top.find("<")
                if first_gt > 0:
                    top = top[first_gt:]
                if (
                    top.startswith("<!doctype")
                    or top.startswith("<html")
                    or top.startswith("<head")
                    or top.startswith("<body")
                ):
                    counter = "no-cannonical-url"
                elif (
                    top.startswith("<?xml")
                    or top.startswith("<rss")
                    or top.startswith("<feed")
                    or top.startswith("<channel")
                ):
                    counter = "xml"
                else:
                    counter = "unknown"
                self.incr_stories(counter, link)
                return False  # discard

            # now that we FINALLY have a URL, make sure it isn't
            # from a source we filter out!!!
            if not self.check_story_url(canonical_url):
                return False  # already counted and logged

            with cmd:
                # importer calls mcmetadata.urls.unique_url_hash(cmd.url)
                # which calls normalize_url (cmd.normalized_url is never
                # used in story-indexer).
                cmd.url = canonical_url

            # story_archive_writer wants hmd.final_url
            with hmd:
                hmd.final_url = canonical_url

            # In this case rss.link is the downloads_id (S3 object id).
            logger.info("%s: using canonical_url %s", link, canonical_url)

            # NOTE! Cannot call "incr_stories": would cause double counting!

        return True

    def process_story(self, sender: StorySender, story: BaseStory) -> None:
        try:
            html = self._decode_content(story)
        except CannotDecode:
            return  # already counted

        extract_stats: Counter[str] = Counter()
        final_url = story.http_metadata().final_url
        if final_url is None:
            raise QuarantineException("no final_url")

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
            return

        sender.send_story(story)

        # send stats: not critical, so after passing story

        method = mdd["text_extraction_method"]
        self.incr_stories(f"OK-{method}", final_url)

        # NOTE! after save_metadata, to get canonical_url if possible
        logger.info("parsed %s with %s date %s", self._log_url(story), method, pub_date)

        skip_items = {"total", "fetch"}  # stackable, no fetch done
        for item, sec in extract_stats.items():
            if item not in skip_items:
                # was tempted to replace 'content' with method,
                # but is really sum of all methods tried!!
                self.timing("extract", sec * 1000, labels=[("step", item)])


if __name__ == "__main__":
    run(Parser, "parser", "metadata parser worker")

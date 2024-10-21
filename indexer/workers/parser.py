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
from indexer.story import BaseStory
from indexer.storyapp import StorySender, StoryWorker
from indexer.worker import QuarantineException

QUARANTINE_DECODE_ERROR = True  # discard if False

logger = logging.getLogger("parser")


class Discard(Exception):
    """Exception to signal story discard"""


class Parser(StoryWorker):
    def _decode_content(self, story: BaseStory) -> tuple[str, str, bool]:
        hmd = story.http_metadata()
        final_url = hmd.final_url
        need_canonical_url = False
        if not final_url:
            rss = story.rss_entry()

            if rss.link and rss.link.isdigit():
                # here with "historical" data without URL
                # for a one month window of 2021
                # (queued with --allow-empty-url)
                need_canonical_url = True
            else:
                # want notice level!
                # should NOT have gotten here
                logger.warning("rss link %s final_url %r", rss.link, final_url)
                self.incr_stories("no-url", final_url or "")
                raise QuarantineException("no url")
            final_url = ""

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
                    raise Discard("no-decode")

        # XXX also unicode_markup??
        html = ud.markup  # decoded HTML
        logger.info("parsing %s: %d characters", final_url, len(html))
        if not html:
            # can get here from batch fetcher, or if body was just a BOM
            self.incr_stories("no-html", final_url)  # want level=NOTICE
            raise Discard("no-html")

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

        return (final_url, html, need_canonical_url)

    def _save_metadata(
        self,
        story: BaseStory,
        final_url: str,
        mdd: dict[str, Any],
        need_canonical_url: bool,
    ) -> bool:
        # XXX check for empty text_content?
        # (will be discarded by importer)

        cmd = story.content_metadata()
        with cmd:
            for key, val in mdd.items():
                if hasattr(cmd, key):  # avoid hardwired exceptions
                    setattr(cmd, key, val)

            cmd.parsed_date = dt.datetime.utcnow().isoformat()

        if need_canonical_url:
            # should only get here with S3 object id in rss.link
            link = story.rss_entry().link or "SNH"

            if canonical_url := cmd.canonical_url:
                with cmd:
                    # importer calls mcmetadata.urls.unique_url_hash(cmd.url)
                    # which calls normalize_url (cmd.normalized_url is never
                    # used in story-indexer).  cmd.url is also
                    # story_archive_writer's second choice.
                    cmd.url = canonical_url

                # story_archive_writer wants hmd.final_url and response_code,
                # trying to centralize acts of forgery here, rather than
                # difusing bits of magic in various places.
                with story.http_metadata() as hmd:
                    hmd.final_url = canonical_url
                    hmd.response_code = 200

                # In this case rss.link is the downloads_id (S3 object id).
                logger.info("%s: using canonical_url %s", link, canonical_url)

                # NOTE! Cannot call "incr_stories": would cause double counting!
            else:
                self.incr_stories("no-canonical-url", link)
                return False  # discard

        return True

    def process_story(self, sender: StorySender, story: BaseStory) -> None:
        try:
            final_url, html, need_canonical_url = self._decode_content(story)
        except Discard:
            # already counted
            return

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

        method = mdd["text_extraction_method"]
        logger.info("parsed %s with %s date %s", final_url, method, pub_date)

        if self._save_metadata(story, final_url, mdd, need_canonical_url):
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

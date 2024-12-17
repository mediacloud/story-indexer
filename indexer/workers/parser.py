"""
metadata parser pipeline worker
"""

import datetime as dt
import logging
from collections import Counter
from typing import Any
from xml.etree.ElementTree import XMLPullParser

# PyPI:
import mcmetadata
from bs4.dammit import UnicodeDammit

# local:
from indexer.app import run
from indexer.story import NEED_CANONICAL_URL, BaseStory, ContentMetadata
from indexer.storyapp import StorySender, StoryWorker
from indexer.worker import QuarantineException

QUARANTINE_DECODE_ERROR = True  # discard if False

# must be tuple for endswith
# ordered (somewhat) by frequency
FEED_TAGS = ("rss", "feed", "rdf", "channel", "urlset", "sitemapindex")

# omit total so stackable in grafana, no fetching done by mcmetadata
SKIP_EXTRACT_STATS_ITEMS = {"total", "fetch"}

logger = logging.getLogger("parser")


class CannotDecode(Exception):
    """could not decode"""


class Parser(StoryWorker):
    def sentry_init(self) -> None:
        super().sentry_init()
        mcmetadata.sentry_ignore_loggers()
        mcmetadata.ignore_loggers()  # or at top of this file?

    def _log_url(self, story: BaseStory) -> str:
        """
        return URL for logging
        """
        url = story.http_metadata().final_url
        if url and url != NEED_CANONICAL_URL:
            return url
        # here with historical data w/o URL in csv
        # rss "link" will be S3 object ID
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
        """
        copy extracted metadata dict to Story object
        """
        # XXX check for empty text_content?
        # (will be quarantined by importer)

        cmd = story.content_metadata()
        with cmd:
            for key, val in mdd.items():
                if hasattr(cmd, key):  # avoid hardwired exceptions
                    setattr(cmd, key, val)

            # NOTE! Full timestamp: used for "indexed_date"
            # (the best choice for query result pagination)
            # so fine granularity is required!!
            cmd.parsed_date = dt.datetime.utcnow().isoformat()

        if (
            story.http_metadata().final_url == NEED_CANONICAL_URL
            and not self._check_canonical_url(story, html, cmd)
        ):
            return False  # logged and counted: discard

        # after _check_canonical_url
        if cmd.is_homepage:
            self.incr_stories("homepage", self._log_url(story))
            return False

        return True

    def _check_is_html(self, story: BaseStory, data: str) -> bool:
        """
        Here with stories from Nov/Dec 2021 and Mar/Apr 2022 pulled from
        S3 without URL.  Try to detect feed documents; some can cause
        loooong parse times (an hour!) causing RabbitMQ to close
        connection!

        Returns False after counting and logging errors.

        Tried extracting canonical URL here (only), but it was less
        effective so rather than have two places (here and
        _check_canonical_url), called before and after "extract"

        Looks ONLY at the first start tag for speed
        (quick tests shows between 0.4 and 41ms (most <1)

        Errs on the side of NOT discarding!!!
        """
        if "<!doctype" in data[0:128].lower():
            return True

        try:
            pp = XMLPullParser(["start"])
            pp.feed(data)  # pass substring? (first 8K?)
            event, element = next(pp.read_events())
            tag = element.tag.lower()
        except (SyntaxError, StopIteration):
            # xml.etree.ElementTree.ParseError is subclass of SyntaxError
            return True

        # use endswith to handle:
        # {http://www.w3.org/2005/atom}feed
        # {http://www.w3.org/1999/02/22-rdf-syntax-ns#}rdf
        # NOT searching for prefix, in case of variations!!!
        if tag.endswith(FEED_TAGS):
            self.incr_stories("feed", self._log_url(story))
            return False  # NOT HTML
        return True

    def _check_canonical_url(
        self, story: BaseStory, html: str, cmd: ContentMetadata
    ) -> bool:
        """
        should only get here with S3 object id in rss.link
        (historical data from S3 for Nov/Dec 2021 w/o CSV file)

        check if mcmetadata.extract found a canonical URL,
        validate it, and update Story object.
        """
        log_url = self._log_url(story)  # before setting final_url!!

        canonical_url = cmd.canonical_url
        if not canonical_url or canonical_url == NEED_CANONICAL_URL:
            self.incr_stories("no-canonical-url", log_url)
            return False  # logged and counted

        # now that we FINALLY have a URL, make sure it isn't
        # from a source we filter out!!!
        if not self.check_story_url(canonical_url):
            return False  # logged and counted

        with story.http_metadata() as hmd:
            # story_archive_writer wants hmd.final_url
            hmd.final_url = canonical_url

        with cmd:
            # importer calls mcmetadata.urls.unique_url_hash(cmd.url)
            # which calls normalize_url (cmd.normalized_url is never
            # used in story-indexer).
            cmd.url = canonical_url

            # NOTE! leaving original_url (which IS (currently) stored
            # in ES) alone!!!  story-indexer only ever uses
            # original_url for logging (with low preference)

            # Used to select sources/collections!!
            cmd.canonical_domain = mcmetadata.urls.canonical_domain(canonical_url)

            cmd.is_homepage = mcmetadata.urls.is_homepage_url(canonical_url)

        logger.info("%s: canonical URL %s", log_url, canonical_url)

        # NOTE! Cannot call "incr_stories": would cause double counting!
        return True

    def process_story(self, sender: StorySender, story: BaseStory) -> None:
        try:
            html = self._decode_content(story)
        except CannotDecode:
            return  # already counted

        final_url = story.http_metadata().final_url
        if final_url is None:
            raise QuarantineException("no final_url")

        if final_url == NEED_CANONICAL_URL and not self._check_is_html(story, html):
            return  # logged and counted

        extract_stats: Counter[str] = Counter()
        try:
            mdd = mcmetadata.extract(final_url, html, stats_accumulator=extract_stats)
        except mcmetadata.exceptions.BadContentError:
            self.incr_stories("too-short", self._log_url(story))
            # No quarantine error here, just discard
            return

        # change datetime object to JSON-safe string
        if mdd["publication_date"] is not None:
            pub_dt = mdd["publication_date"]
            pub_date = pub_dt.strftime("%Y-%m-%d")
            self.timing("extracted-pub-date", pub_dt.date())
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

        for item, sec in extract_stats.items():
            if item not in SKIP_EXTRACT_STATS_ITEMS:
                # was tempted to replace 'content' with method,
                # but is really sum of all methods tried!!
                self.timing("extract", sec * 1000, labels=[("step", item)])

        # at very end: any unhandled exception would cause requeue
        sender.send_story(story)


if __name__ == "__main__":
    run(Parser, "parser", "metadata parser worker")

"""
elasticsearch import pipeline worker
"""

import argparse
import logging
import sys
import unicodedata
from datetime import datetime
from typing import Literal, Optional, Union, cast

from elasticsearch.exceptions import ConflictError, RequestError
from mcmetadata.urls import unique_url_hash

from indexer.app import run
from indexer.elastic import ElasticConfMixin
from indexer.story import BaseStory
from indexer.storyapp import StorySender, StoryWorker
from indexer.worker import QuarantineException

logger = logging.getLogger("importer")

# Index name alias defined in the index_template.json
INDEX_NAME_ALIAS = "mc_search"

# Lucene has a term byte-length limit of 32766
MAX_TEXT_CONTENT_LENGTH = 32766

# story fields that should be truncated if longer than MAX_TEXT_CONTENT_LENGTH
TruncField = Literal["article_title", "text_content"]
TRUNC_FIELD_COUNTERS = {
    "article_title": "title_len",
    "text_content": "story_len",
}


def truncate_str(
    src: str | None,
    max_length: int = MAX_TEXT_CONTENT_LENGTH,
    normalize: bool = True,
) -> str | None:
    """
    Truncate a unicode string to fit within max_length when encoded in utf-8.

    src: str to truncate.
    max_length: maximum length of the truncated str. Must be non-negative integer.
    mormalize: whether src should be normalized to NFC before truncation.

    returns a utf-8 prefix of src guaranteed to fit within max_length when
    encoded as utf-8.
    """
    if src:
        src_bytes = src.encode(encoding="utf-8", errors="replace")
    if not src or len(src_bytes) <= max_length:
        return src
    if normalize:
        n_src = unicodedata.normalize("NFC", src)
        src_bytes = n_src.encode(encoding="utf-8", errors="replace")
    # any errors on decode are due to the truncation and should ignored
    return src_bytes[:max_length].decode(encoding="utf-8", errors="ignore")


class ElasticsearchImporter(ElasticConfMixin, StoryWorker):
    def define_options(self, ap: argparse.ArgumentParser) -> None:
        super().define_options(ap)
        ap.add_argument(
            "--no-output",
            action="store_false",
            dest="output_msgs",
            default=True,
            help="Disable output to archiver",
        )

        # While it's wasteful to run importer processes that don't do
        # importing, but it's a rare case (created for "offline"
        # processing of historical data) and this was less invasive
        # than altering the plumbing of the pipeline.
        ap.add_argument(
            "--no-import",
            action="store_true",
            default=False,
            help="Disable importing!",
        )

    def process_args(self) -> None:
        super().process_args()
        assert self.args
        logger.info(self.args)

        self.output_msgs = self.args.output_msgs
        self.no_import = self.args.no_import

        if not self.output_msgs and self.no_import:
            logger.error("no output AND no import?!")

        index_template = self.load_index_template()
        if not index_template:
            logger.error("Elasticsearch Index template not loaded")
            sys.exit(1)
        self.elasticsearch_fields = index_template["template"]["mappings"][
            "properties"
        ].keys()

    def incr_pub_date(self, status: str) -> None:
        """
        helper for reporting pub_date stats
        """
        self.incr("pub_date", labels=[("status", status)])

    def incr_trunc(
        self, field: TruncField, is_trunc: bool = False, status: Optional[str] = None
    ) -> None:
        """
        Increment the counter of one of the truncate-able story fields.
        """
        name = TRUNC_FIELD_COUNTERS[field]
        default_status = "intact"
        if is_trunc:
            default_status = "trunc"
        self.incr(name, labels=[("status", status or default_status)])

    def truncate_field(self, field: TruncField, value: str) -> str:
        """
        Truncates value if needed.
        """
        value_trunc = cast(str, truncate_str(value))  # mypy need cast for len
        is_trunc = len(value_trunc) < len(value)
        self.incr_trunc(field, is_trunc)
        return value_trunc

    def process_story(self, sender: StorySender, story: BaseStory) -> None:
        """
        Import story into Elasticsearch
        """
        content_metadata = story.content_metadata()
        data: dict[str, Optional[Union[str, bool]]] = {}
        # extract valid keys from index template (schema)

        fetch_ts = story.http_metadata().fetch_timestamp
        if fetch_ts:
            # Here with timestamp of when story was fetched, make
            # visible to see what's currently entering ES.
            # While it's tempting to subtract off the current time (to
            # get an idea of "how far behind" we are, with
            # historical data from S3 fetch_timestamp will be the
            # original fetch time (and historical data is processed
            # from newest to oldest, so send in as ms (displayable
            # in Grafana as "Date & Time")
            self.timing("fetch-ts", fetch_ts * 1000.0)

        self.incr("field_check.stories")  # total number of stories checked
        for key, value in content_metadata.as_dict().items():
            if key in self.elasticsearch_fields:
                if value is None or value == "":
                    # missing values are not uncommon (publication_date, and sometimes
                    # article_title) so lowering back to info, and counting instead.  NOTE!
                    # NOT using tags, because more than one field may be counted per story,
                    # and a sum of all "missing" fields isn't a count of the number stories
                    # with with a missing field.
                    self.incr(f"field_check.missing.{key}")
                    logger.info("Value for key: %s is not provided.", key)
                    continue
                assert isinstance(value, (str, bool))  # currently only strs
                data[key] = value

        # check if empty now, before any tampering (pub_date or indexed_date)
        if not data:
            self.incr_stories("no-data", "no-url")
            raise QuarantineException("no-data")

        # if publication date is None (from parser) or "None"(from archiver), fallback to rss_fetcher pub_date
        pub_date = data.get("publication_date")
        if pub_date and pub_date != "None":
            self.incr_pub_date("extracted")  # extracted from content by mcmetadata
        else:  # None, "", or "None"
            rss_pub_date = story.rss_entry().pub_date
            if rss_pub_date:
                pub_date = datetime.strptime(
                    rss_pub_date, "%a, %d %b %Y %H:%M:%S %z"
                ).strftime("%Y-%m-%d")
                self.incr_pub_date("rss")
            else:
                pub_date = None
                self.incr_pub_date("none")
            data["publication_date"] = pub_date

            # PB: NOTE! this means "metadata" will contain data NOT extracted by
            # mcmetadata, and if it turns out using the RSS date was a bad idea, future
            # generations reprocessing the archive without reparsing won't know whether
            # the the metadata publication_date is real (extracted) or not.  If they
            # REALLY want to know, they can reparse (and may have superior extraction
            # tools).
            with content_metadata:
                content_metadata.publication_date = pub_date

        # Use parsed_date (from parser or an archive file) as indexed_date, falling
        # back to UTC now (for everything parsed/queued before update applied to
        # parser) if missing or empty/null. API users use indexed_date to poll for
        # newly added stories, so a timestamp.  ES stores times in milliseconds.
        data["indexed_date"] = (
            content_metadata.parsed_date or datetime.utcnow().isoformat()
        )

        # Ensure that text_content (required) and article_title (optional)
        # do not exceed the underlying Lucene’s term byte-length limit
        url = data.get("url")  # for logging
        if not isinstance(url, str) or url == "":
            # exceedingly unlikely, but must check to keep
            # mypy quiet, so might as well do something rather
            # than pass an empty string, or turn None into "None"
            self.incr_stories("no-url", "none")
            raise QuarantineException("no-url")

        text_content = data.get("text_content")
        if not isinstance(text_content, str) or text_content == "":
            self.incr_stories("no-text", url)
            raise QuarantineException("no-text")

        data["text_content"] = self.truncate_field("text_content", text_content)

        article_title = data.get("article_title")
        if not isinstance(article_title, str) or article_title == "":
            # no need to reject a story without article_title; it has content
            logger.warn("missing article_title: %s", url)
            self.incr_trunc("article_title", status="empty")
        else:
            data["article_title"] = self.truncate_field("article_title", article_title)

        if self.import_story(data) and self.output_msgs:
            # pass story along to archiver, unless disabled or duplicate
            sender.send_story(story)

    def import_story(
        self,
        data: dict[str, Optional[Union[str, bool]]],
    ) -> Optional[str]:
        """
        Imports story to Elasticsearch

        Args:
            data (dict[str, Optional[Union[str, bool]]]): The story data to be imported,
            containing keys such as 'url' and 'text_content'.

        Returns:
            Optional[str]: The Elasticsearch document ID (url_hash) if the story is imported,
            None if it is a duplicate.
            else raises an exception.
        """
        # data can never be empty (has been checked in process_story, AND
        # "indexed_date", "url" & "text_content" will always be set, so no check here).

        # url is for hashing, logging, and testing
        url: str = cast(str, data["url"])  # switch to typing.assert_type in py3.11

        if self.no_import:
            self.incr_stories("noimp", url)
            return "success"  # so passed forward

        url_hash = unique_url_hash(url)

        try:
            # Check if the document already exists by ID in the alias.
            # We want to avoid indexing duplicates on ILM index rollover
            search_response = self.elasticsearch_client().search(
                index=INDEX_NAME_ALIAS,
                body={
                    "query": {"bool": {"filter": {"term": {"_id": url_hash}}}},
                    "size": 0,
                },
            )
            if search_response["hits"]["total"]["value"] > 0:
                self.incr_stories("ilm-dups", url)
                return None  # mypy explicit return
            # logs HTTP op with index name and ID str.
            # create: raises exception if a duplicate.
            response = self.elasticsearch_client().create(
                index=INDEX_NAME_ALIAS, id=url_hash, document=data
            )

            # ES is restful; python library turns HTTP errors into exceptions, and no
            # exception was thrown, so should only be here if HTTP returned 200.
            # PARANOIA! create() call always returns ObjectApiResponse.
            if not response:
                raise QuarantineException(f"response {response!r}")

            # Always count, and be explicit about what we saw.  Only documented result
            # values are "created" and "updated". The existing code was only counting
            # "success" on the expected ("created") result, but returning happy
            # regardless.  If there is ever an "indexer metadata" sub-object, save id
            # and response there?!  One could argue that the undesired "updated" result
            # (that should never be seen) should be returned as False (do not archive).
            result = response.get("result", "noresult") or "emptyres"
            self.incr_stories(result, url)  # count, logs result, URL
            return url_hash  # i.e. response.get("_id")
        except ConflictError:
            self.incr_stories("dups", url)
            return None
        except RequestError as e:
            # here with over-length content!
            self.incr_stories("reqerr", url)
            raise QuarantineException(repr(e))
        except Exception:
            # Capture all other exceptions here for counting, and re-raise for retry
            # (and eventual quarantine if the error is not transient).
            self.incr_stories("retry", url)
            raise

        # this should be unreachable (try contains return)
        # but in the spirit of total paranoia, count, log and quarantine.
        self.incr_stories("snh", url)
        raise QuarantineException("should not happen")


if __name__ == "__main__":
    run(ElasticsearchImporter, "importer", "elasticsearch import worker")

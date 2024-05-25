"""
Pull stories using rss-fetcher API (without a generated RSS file).

Allows arbitrary pull frequency.  Pulling too often could result in
clumps of stories from a single source, which isn't ideal for the
current queue-based fetcher.
"""

import argparse
import datetime as dt
import email.utils
import json
import logging
import os
import sys
import time
from typing import TypedDict, cast
from urllib.parse import urlparse

import requests

from indexer.app import AppException, run
from indexer.cookiejar import CookieJar
from indexer.story import RSSEntry, StoryFactory
from indexer.storyapp import StoryProducer

Story = StoryFactory()

logger = logging.getLogger("rss-puller")

# parameter names used to generate options, members, environment vars
# values are available as api_params[name]
RSS_FETCHER_PARAMS = ["pass", "url", "user"]


def rss_fetcher_name2opt(name: str) -> str:
    """
    convert short name to full option name
    """
    return "rss-fetcher-" + name


def rss_fetcher_name2var(name: str) -> str:
    """
    convert short name to name of args namespace property
    """
    return rss_fetcher_name2opt(name).replace("-", "_")


def rss_fetcher_name2env(name: str) -> str:
    """
    convert short name to environment variable name
    """
    return rss_fetcher_name2var(name).upper()


class StoryJSON(TypedDict):
    """
    JSON rows returned by rss-fetcher API /api/rss_entries
    """

    id: int
    url: str
    published_at: str | None  # UTC in DB/ISO format w/o TZ
    domain: str | None
    title: str | None
    sources_id: int | None
    feed_id: int | None
    feed_url: str | None
    # added in rss-fetcher 0.16.1
    fetched_at: str | None  # UTC in DB/ISO format w/o TZ


class RSSPuller(StoryProducer):
    def __init__(self, process_name: str, descr: str):
        super().__init__(process_name, descr)

        self.api_params = {}
        for param in RSS_FETCHER_PARAMS:
            self.api_params[param] = ""

    def define_options(self, ap: argparse.ArgumentParser) -> None:
        super().define_options(ap)

        # no defaults (all private)
        def add_rss_fetcher_arg(name: str) -> None:
            env = os.environ.get(rss_fetcher_name2env(name))
            ap.add_argument(
                "--" + rss_fetcher_name2opt(name),
                default=env,
                help=f"rss-fetcher API {name} (default: {env})",
            )

        for param in RSS_FETCHER_PARAMS:
            add_rss_fetcher_arg(param)

        # small batches more likely to have all stories from one source
        # (which thawrts the goal of the "shuffle" to make queue more varied).
        default_batch_size = int(os.environ.get("RSS_FETCHER_BATCH_SIZE", 2500))
        ap.add_argument(
            "--rss-fetcher-batch-size",
            type=int,
            default=default_batch_size,
            help=f"Use rss-fetcher API to fetch stories (default: {default_batch_size})",
        )

    def _get_rss_fetcher_value(self, name: str) -> None:
        """
        worker function to fetch value from command line
        or default (from environment) into self.api_params dict
        """
        member_name = rss_fetcher_name2var(name)
        val = getattr(self.args, member_name)
        if val == "" or val is None:
            logger.error(
                "need --%s or %s env var",
                rss_fetcher_name2opt(name),
                rss_fetcher_name2env(name),
            )
            sys.exit(1)
        assert isinstance(val, str)
        self.api_params[name] = val

    def process_args(self) -> None:
        super().process_args()

        args = self.args
        assert args

        self.dry_run = args.dry_run

        # fetch rss_fetcher parameters into self.api_params
        for name in RSS_FETCHER_PARAMS:
            self._get_rss_fetcher_value(name)

        u = urlparse(self.api_params["url"])
        self.rss_fetcher_netloc = u.netloc  # get host:port for "via"

    def api_pull_stories(self, first: int, count: int) -> list[StoryJSON]:
        """
        returns tuple with list of dicts, and "next" token
        """
        # There is an RSS API access class in the web-search repo, but it's
        # not a "public API", so there doesn't seem to be much point in putting
        # the code in a PyPI module of its own.
        base_url = self.api_params["url"]
        user = self.api_params["user"]
        password = self.api_params["pass"]
        url = f"{base_url}/api/rss_entries/{first}?_limit={count}"
        if user and password:
            auth = requests.auth.HTTPBasicAuth(user, password)
        else:
            auth = None
        hdrs = {"User-Agent": "story-indexer rss-queuer.py"}
        response = requests.get(url, auth=auth, headers=hdrs)

        if response.status_code != 200:
            raise AppException(f"status code {response.status_code}")

        j = response.json()
        status = j.get("status")
        if status != "OK":
            raise AppException(f"status {status}")

        results = j.get("results")
        # XXX validate (at least id and url)??
        rows = cast(list[StoryJSON], results)
        return rows

    def api_get_and_queue(
        self, last: StoryJSON | None, count: int
    ) -> tuple[int, StoryJSON | None]:
        """
        pull and queue stories;
        returns (count, new last record)
        """

        if last:
            # lets bad data cause fatal errors
            # try to keep this the ONLY place that adds one!
            next_id = last["id"] + 1
        else:
            next_id = 0

        stories = self.api_pull_stories(next_id, count)
        got = len(stories)

        new_last: StoryJSON | None
        if got > 0:
            new_last = stories[-1]  # before shuffle!!
        else:
            new_last = last
        logger.info("got %d new_last: %s", got, new_last)

        for s in stories:
            id_ = s.get("id")
            if not isinstance(id_, int):
                # don't muddy the stats if just a dry-run:
                if not self.dry_run:
                    self.incr_stories("bad-id", str(id_))  # log and count
                continue

            url = s.get("url")
            if not isinstance(url, str):
                # don't muddy the stats if just a dry-run:
                if not self.dry_run:
                    self.incr_stories("no-url", str(url))  # log and count
                continue

            if not self.check_story_url(url):
                continue  # logged and counted

            # reformat optional published_at (from original RSS file)
            # from isoformat (as read from rss-fetcher database)
            # to RFC2822, as in RSS files
            rfc2822_pub_date = None
            try:
                pub = s["published_at"]
                if pub is not None:
                    pub_dt = dt.datetime.fromisoformat(pub + "+00:00")
                    rfc2822_pub_date = email.utils.formatdate(pub_dt.timestamp())
            except (KeyError, TypeError, ValueError):
                pass

            story = Story()
            rss: RSSEntry = story.rss_entry()  # Temp typing?
            with rss:
                rss.link = url
                rss.title = s.get("title")
                rss.domain = s.get("domain")
                rss.pub_date = rfc2822_pub_date
                # tracking/debug aids (not used/indexed):
                rss.source_feed_id = s.get("feed_id")
                rss.source_source_id = s.get("sources_id")
                rss.source_url = s.get("feed_url")
                rss.via = f"{id_}@{self.rss_fetcher_netloc}"
                # added in rss-fetcher 0.16.1 (not in RSS files):
                rss.fetch_date = s.get("fetched_at")
            self.send_story(story)
        # end for s in stories:
        self.flush_shuffle_batch()
        return (got, new_last)

    def main_loop(self) -> None:
        assert self.args

        batch_size = self.args.rss_fetcher_batch_size
        cookie_jar = CookieJar(
            self.process_name,
            "last-rss-entry",
            force=self.args.force or self.args.dry_run,
        )
        while True:
            # may sleep (if looping) or exit if queues full enough
            self.check_output_queues()

            saved_cookie = cookie_jar.read()
            if saved_cookie:
                last = json.loads(saved_cookie)
            else:
                last = None
            got, new_last = self.api_get_and_queue(last, batch_size)
            if got > 0 and new_last:
                cookie_jar.write(json.dumps(new_last))

            if got < batch_size:
                # got nothing or short batch
                assert self.args
                if self.args.loop:
                    logger.debug("waiting for more...")
                    time.sleep(self.args.sleep)
                else:
                    # not saying "queued", since they aren't w/ --dry-run
                    logger.info("quitting: fetched %d stories", self.queued_stories)
                    return
            else:
                # give rss-fetcher API a quick break.
                # 0.5 sec/k adds ~4 minutes to daily fetch of 500K stories
                time.sleep(0.5)


if __name__ == "__main__":
    run(RSSPuller, "rss-puller", "pull stories using rss-fetcher API")

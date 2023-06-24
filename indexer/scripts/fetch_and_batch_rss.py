import argparse
import csv
import gzip
import logging
import random
import sys
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple, TypedDict

import requests
from lxml import etree

from indexer.app import App
from indexer.path import DATAPATH_BY_DATE
from indexer.story import BaseStory, DiskStory

"""
This eventually aught to subclass from the App class, for ease of integrating with logging etc-
But while I'm building up the functionality I'll just do it the more direct way.

Question: It might be preferable to put this in workers/fetcher/ rather than scripts/, since it is strictly associated with that process.
Additionally, perhaps the methods here should be in a separate file from the app class? tbd...
"""

logger = logging.getLogger(__name__)


class RSSEntry(TypedDict):
    """
    Distinct from indexer.story.RSSEntry in that we don't have any storage routines here- this is just for type clarity.
    """

    link: str
    title: str
    domain: str
    pub_date: str


def fetch_backup_rss(
    fetch_date: str, sample_size: Optional[int] = None
) -> List[RSSEntry]:
    """
    Fetch the content of the backup rss fetcher for a given day and return as a list of dicts.
    sample_size: 0 or
    """
    url = f"https://mediacloud-public.s3.amazonaws.com/backup-daily-rss/mc-{fetch_date}.rss.gz"
    rss = requests.get(url, timeout=60)
    if rss.status_code > 400:
        raise RuntimeError(f"No mediacloud rss available for {fetch_date}")

    # The mediacloud record is XML, so we just read it in directly and parse out our urls.
    data = gzip.decompress(rss.content)
    parser = etree.XMLParser(recover=True)
    root = etree.fromstring(data, parser=parser)

    found_items: List[Any] = []

    all_items = root.findall("./channel/item")

    # In testing contexts we might want to run on a small sample, rather than a whole day.
    if sample_size is not None:
        if len(all_items) < sample_size:
            raise RuntimeError(
                f"Only {len(all_items)} records found for {fetch_date} in backup rss ({sample_size} requested)"
            )
        all_items = random.sample(all_items, sample_size)

    found_items = []
    for item in all_items:
        entry: RSSEntry = {
            "link": item.findtext("link"),
            "title": item.findtext("title"),
            "domain": item.findtext("domain"),
            "pub_date": item.findtext("pubDate"),
        }
        found_items.append(entry)

    return found_items


def batch_rss(
    source_list: List[RSSEntry], num_batches: int = 20, max_domain_size: int = 10000
) -> tuple[List, Dict]:
    """
    Greedily pack source_list into num_batches batches, keeping each domain together.
    """

    # Get initial statistics
    agg: Dict[str, int] = {}
    for entry in source_list:
        name = entry["domain"]
        if name in agg:
            agg[name] += 1
        else:
            agg[name] = 1

    domains_sorted: List[Tuple[str, int]] = sorted(
        [(k, v) for k, v in agg.items()], key=lambda x: x[1]
    )

    batches: List = [[] for i in range(num_batches)]
    batch_map: Dict = {}

    while len(domains_sorted) > 0:
        domain = domains_sorted.pop()
        domain_name = domain[0]

        # comprehensions instead of filters because filters weren't working right.
        to_pack = [x for x in source_list if x["domain"] == domain_name]

        batch_index, target_batch = min(enumerate(batches), key=lambda b: len(b[1]))
        target_batch.extend(to_pack)
        batch_map[domain_name] = batch_index

        # Reduce the work of future iterations pls
        source_list = [x for x in source_list if x["domain"] != domain_name]

    return batches, batch_map


class RSS_Batcher(App):
    fetch_date: str
    sample_size: Optional[int]
    num_batches: int
    init_stories: bool = True

    def define_options(self, ap: argparse.ArgumentParser) -> None:
        super().define_options(ap)

        # fetch_date
        ap.add_argument(
            "--fetch-date",
            action="store_const",
            dest="fetch_date",
            help="Date (in YYYY-MM-DD) to fetch",
        )

        # sample_size
        ap.add_argument(
            "--sample-size",
            action="store_const",
            dest="sample_size",
            default=None,
            help="Number of stories to batch. Default (None) is 'all of them'",
        )

        # num_batches
        num_batches_default = 20
        ap.add_argument(
            "--num-batches",
            action="store_const",
            dest="num_batches",
            default=num_batches_default,
            help=f"Number of batches to break stories into (default {num_batches_default})",
        )

        # init_stories
        ap.add_argument(
            "--init-stories",
            dest="init_stories",
            action=argparse.BooleanOptionalAction,
            help="Toggle initialization of story objects- if on, this script initializes the data directory for each story and only passes serialized story content in the batchfile. If off, batchfiles contain the whole rss_entry content",
        )

    def process_args(self) -> None:
        super().process_args()

        assert self.args
        fetch_date = self.args.fetch_date
        if not fetch_date:
            logger.fatal("need fetch date")
            sys.exit(1)

        self.fetch_date = fetch_date

        self.sample_size = self.args.sample_size
        self.num_batches = self.args.num_batches
        self.init_stories = self.args.init_stories

    def main_loop(self) -> None:
        rss_records = fetch_backup_rss(self.fetch_date, self.sample_size)
        batches, batch_map = batch_rss(rss_records, num_batches=self.num_batches)

        data_path = DATAPATH_BY_DATE(self.fetch_date)

        for batch_index, batch in enumerate(batches):
            batch_path = data_path + f"batch_{batch_index}.csv"
            with open(batch_path, "w") as batch_file:
                if self.init_stories:
                    header = ["serialized_story"]
                else:
                    header = batch[0].keys()
                writer = csv.DictWriter(batch_file, fieldnames=header)
                writer.writeheader()
                for story in batch:
                    if self.init_stories:
                        new_story: DiskStory = DiskStory()
                        with new_story.rss_entry() as rss_entry:
                            rss_entry.link = story["link"]
                            rss_entry.title = story["title"]
                            rss_entry.domain = story["domain"]
                            rss_entry.pub_date = story["pub_date"]
                            rss_entry.fetch_date = self.fetch_date
                        writer.writerow({"serialized_story": new_story.dump()})
                    else:
                        writer.writerow(story)

        # This might not be neccesary, but keeping it around for now.
        batch_map_path = data_path + "batch_map.csv"
        with open(batch_map_path, "w") as map_file:
            header = ["domain", "batch_index"]
            writer = csv.DictWriter(map_file, fieldnames=header)
            writer.writeheader()
            for domain, index in batch_map.items():
                writer.writerow({"domain": domain, "batch_index": index})

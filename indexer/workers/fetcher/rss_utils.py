import gzip
import random
from typing import Any, Dict, List, Optional, Tuple, TypedDict

import requests
from lxml import etree

"""
Methods relating to grabbing and batching rss.
"""


class RSSEntry(TypedDict):
    """
    Distinct from indexer.story.RSSEntry in that we don't have any storage routines here- this is just for type clarity.
    """

    link: str
    title: str
    domain: str
    pub_date: str
    fetch_date: str  # from input file name
    # New fields (2/2024) from <source> tag:
    source_url: Optional[str]  # source tag url property
    source_feed_id: Optional[int]  # source tag mcFeedId property
    source_source_id: Optional[int]  # source tag mcSourceId property
    file_name: str  # full input file name


def fetch_daily_rss(
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
        source_tag = item.find("source")

        def src_attr(attr_name: str) -> Optional[str]:
            """
            return attribute (if set and non-empty) from <source> tag.
            return None rather than empty value
            """
            if source_tag is None:
                return None
            return source_tag.get(attr_name) or None

        def src_attr_int(attr_name: str) -> Optional[int]:
            val = src_attr(attr_name)
            if not val or not val.isdigit():
                return None
            return int(val)

        entry: RSSEntry = {
            "link": item.findtext("link"),
            "title": item.findtext("title"),
            "domain": item.findtext("domain"),
            "pub_date": item.findtext("pubDate"),
            "fetch_date": fetch_date,
            "source_url": src_attr("url"),
            "source_feed_id": src_attr_int("mcFeedId"),
            "source_source_id": src_attr_int("mcSourceId"),
            "file_name": url,  # source file
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

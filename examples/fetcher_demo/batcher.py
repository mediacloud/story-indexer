import requests
import xml.etree.ElementTree as ET
from lxml import etree
# import mediacloud.api
from datetime import datetime, timedelta
import gzip
import random
import html
from collections import Counter

import logging


# Get the current RSS file for a given date.
def sample_backup_rss(date, sample_size=100):
    datestring = date.strftime("%Y-%m-%d")
    url = f"https://mediacloud-public.s3.amazonaws.com/backup-daily-rss/mc-{datestring}.rss.gz"
    rss = requests.get(url, timeout=60)
    if rss.status_code > 400:
        # If the content doesn't exist for some reason, disregard.
        # print(f"no mediacloud rss yet for {datestring}")
        raise RuntimeError(f"no mediacloud rss yet for {datestring}")
        # return False

    # The mediacloud record is XML, so we just read it in directly and parse out our urls.
    data = gzip.decompress(rss.content)
    text = data.decode("utf-8")
    parser = etree.XMLParser(recover=True)
    root = etree.fromstring(data, parser=parser)

    found_items = []
    for item in root.findall('./channel/item'):

        found_items.append({
            "link": item.findtext("link"),
            "title": item.findtext("title"),
            "domain": item.findtext("domain"),
            "pub_date": item.findtext("pubDate"),
        })

    if sample_size == 0:
        return found_items

    elif len(found_items) < sample_size:
        raise RuntimeError(
            f"only {len(found_items)} records found for {datestring} in backup rss")

    else:
        sample = random.sample(found_items, sample_size)
        return sample


def rss_batcher(source_list, num_batches=20, max_domain_size=10000):

    # Get initial statistics
    total_count = len(source_list)

    agg = {}
    for entry in source_list:
        name = entry["domain"]
        if name in agg:
            agg[name] += 1
        else:
            agg[name] = 1

    domains_sorted = sorted([(k, v)
                            for k, v in agg.items()], key=lambda x: x[1])

    # this goes like a greedy packing algorithm.
    # We make our batches, and while there are domains still to put into batches,
    # we pack into the batch with the shortest current list
    # (picking at random if any are equal)

    batches = [[] for i in range(num_batches)]
    batch_map = {}

    i = 0
    while len(domains_sorted) > 0:
        domain = domains_sorted.pop()
        domain_name = domain[0]

        # comprehensions instead of filters because filters weren't working right.
        to_pack = [x for x in source_list if x["domain"] == domain_name]

        batch_index, target_batch = min(
            enumerate(batches), key=lambda b: len(b[1]))
        target_batch.extend(to_pack)
        batch_map[domain_name] = batch_index

        # Reduce the work of future iterations pls
        source_list = [x for x in source_list if x["domain"] != domain_name]

    return batches, batch_map

from datetime import datetime
import requests
import gzip
import random
from lxml import etree
from indexer.story import DiskStory, BaseStory, RSS_Entry
from index.path import DATAROOT
from typing import List, Optional

def fetch_backup_rss(fetch_date:str, sample_size: Optional(int) = None) -> List:
    """
    Fetch the content of the backup rss fetcher for a given day and return as a list of dicts. 
    sample_size: 0 or 
    """
    url = f"https://mediacloud-public.s3.amazonaws.com/backup-daily-rss/mc-{fetch_date_string}.rss.gz"
    rss = requests.get(url, timeout=60)
    if rss.status_code > 400:
        raise RuntimeError(f"No mediacloud rss available for {fetch_date_string}")

    #The mediacloud record is XML, so we just read it in directly and parse out our urls.
    data = gzip.decompress(rss.content)
    text = data.decode("utf-8")
    parser = etree.XMLParser(recover=True)
    root = etree.fromstring(data, parser=parser)
    
    found_items = []
    
    all_items = root.findall('./channel/item')
    
    #In testing contexts we might want to run on a small sample, rather than a whole day.
    if sample_size is not None:
        if len(all_items) < sample_size:
            raise RuntimeError(f"Only {len(all_items)} records found for {datestring} in backup rss ({sample_size} requested)")
        all_items = random.sample(all_items, sample_size)
        
    
    found_items = []
    for item in all_items:
        found_items.append({
            "link":item.findtext("link"),
            "title":item.findtext("title"),
            "domain":item.findtext("domain"),
            "pub_date":item.findtext("pubDate"),
        })
            
    return found_items
    

    
def batch_rss(source_list: list, num_batches:int=20, max_domain_size:int = 10000) -> List, Dict:

    #Get initial statistics
    total_count = len(source_list)
    
    agg = {}
    for entry in source_list:
        name = entry["domain"]
        if name in agg:
            agg[name] += 1
        else:
            agg[name] = 1

    domains_sorted = sorted( [(k, v)for k, v in agg.items()], key = lambda x: x[1])

    #this goes like a greedy packing algorithm.
    #We make our batches, and while there are domains still to put into batches, 
    #we pack into the batch with the shortest current list 
    #(picking at random if any are equal)

    batches = [[] for i in range(num_batches)]
    batch_map = {}

    i = 0
    while len(domains_sorted) > 0:
        domain = domains_sorted.pop()
        domain_name = domain[0]
        
        #comprehensions instead of filters because filters weren't working right. 
        to_pack = [x for x in source_list if x["domain"] == domain_name]
        
        batch_index, target_batch = min(enumerate(batches), key= lambda b: len(b[1]))
        target_batch.extend(to_pack)
        batch_map[domain_name] = batch_index
        
        #Reduce the work of future iterations pls
        source_list = [x for x in source_list if x["domain"] != domain_name]
              
              
    return batches, batch_map


def main(fetch_date:str, sample_size:str = 0,num_batches: int, max_domain_size: int= 10000, init_stories:bool=True)-> None
    rss_records = fetch_backup_rss(fetch_date, sample_size)
    
    if init_stories:
        for story in rss_records:
            new_story: DiskStory = DiskStory()
            with new_story.rss_entry() as rss_entry:
                rss_entry.link = item.findtext("link")
                rss_entry.title = item.findtext("title")
                rss_entry.domain = item.findtext("domain")
                rss_entry.pub_date = item.findtext("pubDate")
                rss_entry.fetch_date = fetch_date
                
    batches, batch_map = batch_rss(rss_records, num_batches=num_batches)
    #Then save the batchfiles somewhere, presumably. 
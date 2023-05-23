#Supervisor Entry script- prepare batches, launch scrapers, save results 
import argparse
import datetime
import json
import time
from pathlib import Path
from scrapy.crawler import CrawlerProcess

from common.filesystem_interface import pipeline_filesystem_interface, WorkState

from .batcher import sample_backup_rss, rss_batcher
from .scraper import BatchSpider

def main(date, num_batches, batch_index, sample_size):
    
    WORKDIR = "work/"
    SAVEDIR = "scraped_content/"
    READY = Path(f"{WORKDIR}/batchready")
    #Only the first job will actually generate the batches
    if batch_index == 0:
        #grab rss content, and generate batches
        fs = pipeline_filesystem_interface(date)
        
        source_rss = sample_backup_rss(date, sample_size=sample_size)
        
        fs.init_rss(source_rss)
        
        batches, batch_map = rss_batcher(source_rss, num_batches)

        fs.init_batches(batches, batch_map)
        
        
       
    else:
        #Give the batching process a head-start, so there's not a fs conflict
        time.sleep(1)
        fs = pipeline_filesystem_interface(date)

    #Then, using the little 'ready' file as a latch, wait.
    while not fs.get_status() == WorkState.BATCHES_READY:
        time.sleep(10)

    process = CrawlerProcess()
    process.crawl(BatchSpider, date=date, batch_index=batch_index)
    process.start()
    #Start with a smallish limit while we work out kinks 
    
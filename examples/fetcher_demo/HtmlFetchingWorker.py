#Supervisor Entry script- prepare batches, launch scrapers, save results 
import datetime
import json
import time
from pathlib import Path
from scrapy.crawler import CrawlerProcess

from common.filesystem_interface import pipeline_filesystem_interface
from common.state import WorkState

from pipeline.worker import Worker

from .batcher import sample_backup_rss, rss_batcher
from .BatchSpider import BatchSpider


class HtmlFetchingWorker(Worker):
    
    def __init__(self, process_name:str, descr:str, date:datetime, batch_index:int, num_batches:int, sample_size:int):
        super().__init__(process_name, descr)
        self.date = date
        self.batch_index = batch_index
        self.num_batches = num_batches
        self.sample_size = sample_size
    
    def main_loop(self, conn, chan):
        #Only the first job will actually generate the batches
        if self.batch_index == 0:
            #grab rss content, and generate batches
            fs = pipeline_filesystem_interface(self.date)

            source_rss = sample_backup_rss(self.date, sample_size=self.sample_size)

            fs.init_rss(source_rss)

            batches, batch_map = rss_batcher(source_rss, self.num_batches)

            fs.init_batches(batches, batch_map)


        #Otherwise give the batching process a head-start, so there's not a fs conflict
        else:
            time.sleep(1)
            fs = pipeline_filesystem_interface(self.date)

        #Then, using the little 'ready' file as a latch, wait.
        while not fs.get_status() >= WorkState.BATCHES_READY:
            time.sleep(10)

        process = CrawlerProcess()
        process.crawl(BatchSpider, date=self.date, batch_index=self.batch_index, send_items = self.send_items, chan=chan)
        process.start()
        #Start with a smallish limit while we work out kinks 
    
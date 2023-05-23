import scrapy
from scrapy.crawler import CrawlerProcess
import datetime

from pathlib import Path

import json
from common.filesystem_interface import pipeline_filesystem_interface, BatchState, WorkState

class BatchSpider(scrapy.Spider):
    '''This spider loads a batch of urls from file, then runs wild on it. 
    '''
    name = "main"
    
    custom_settings = {
        'COOKIES_ENABLED':False,
        'AUTOTHROTTLE_ENABLED': True,
        'AUTOTHROTTLE_DEBUG': True,
        'CONCURRENT_REQUESTS_PER_DOMAIN': 2,
        'AUTOTHROTTLE_TARGET_CONCURRENCY':10,
        'RETRY_HTTP_CODES':[502, 503, 504, 522, 524, 408, 429] #donut bother with retrying on 500s
    }
    
    def __init__(self, date, batch_index, limit=None, *args, **kwargs):
        super(BatchSpider, self).__init__(*args, **kwargs)
        self.fs = pipeline_filesystem_interface(date)
        self.batch_index = batch_index
        
        self.limit = limit
    
    def start_requests(self):    
        #Some kind of logging utility goes here
        #Path(f"work/batch_{self.batch_index}_start_tstamp").write_text(str(datetime.datetime.now().timestamp()))
        
        #Get the batch, and update all the neccesary flags. 
        batch = self.fs.get_batch(self.batch_index)
        
        print(f"Found a batch of size {len(batch)}")
        
        if self.limit == None:
            self.limit = len(batch)

        i = -1
        for entry in batch[:self.limit]:
            i += 1
            yield scrapy.Request(url = entry["link"], callback=self.parse, cb_kwargs={"entry":entry, "i":i})
            
    def parse(self, response, entry=None, i=0):
        if response.status < 400:
            raw_html = response.body
            
            original_link = entry["link"]
            
            meta = {
                "response_status":response.status,
                "fetch_timestamp":datetime.datetime.now().timestamp(),
                "rss_entry":entry, 
                "fetch_batch":self.batch_index
            }
            
            self.fs.put_fetched(original_link, raw_html, meta)
            
        #Will need to drop a message in a queue as well
        #not to mention logging of status and such
        

# A quick and dirty filesystem API, which will impliment all the basic filesystem interactions
# required by the various steps of the pipeline. The date is set at the API level, and all of the content for a given link are accessible
#

# The basic outline of the directory structure is:
"""
data/
- {date}/
-- source.rss
-- work-status.txt
-- batch_map.csv
-- batchfile_{i}.csv
-- batchfile_{i}_status.txt
    ...
-- content/
--- {link_hash}%-raw.html
--- {link_hash}%-http_meta.json
--- {link_hash}%-content_meta.json
     ...
- omitted/
-- {date}_omitted.csv

"""

import os
from pathlib import Path
from datetime import datetime
from enum import Enum
import csv
import json

from .state import WorkState, BatchState

DATAROOT = "data"
CONTENT = "content"
ERROR = "errors"
SOURCE_RSS = "source_rss.csv"
WORKSTATUS = "work_status.txt"
BATCHMAP = "batch_map.csv"


class pipeline_filesystem_interface(object):
    """
    An API which manages access to a filesystem data store, and also manages
    pipeline state as it pertains to the filesystem
    """

    def __init__(self, date):

        self.date = date
        self.year = date.year
        self.month = date.month
        self.day = date.day
        self.root_path_str = f"{DATAROOT}/{self.year}/{self.month}/{self.day}/"
        self.content_path_str = self.root_path_str+CONTENT+'/'
        self.error_path_str = self.root_path_str+ERROR+'/'

        self.status_file = Path(self.root_path_str+WORKSTATUS)
        self.batch_status = {}

        rootpath = Path(self.root_path_str)
        if not rootpath.exists():
            rootpath.mkdir(parents=True)

        content_path = Path(self.content_path_str)
        if not content_path.exists():
            content_path.mkdir(parents=True)

        error_path = Path(self.error_path_str)
        if not error_path.exists():
            error_path.mkdir(parents=True)

        # setup global work state manager
        if not self.status_file.exists():
            self.status = WorkState.INIT
            self.save_status()
        else:
            self.status = self.get_status()

    def get_status(self):
        return WorkState[self.status_file.read_text()]

    def save_status(self):
        self.status_file.write_text(self.status.name)

    def batchfile_path(self, batch_index):
        pathstr = self.root_path_str+f"batchfile_{batch_index}.csv"
        return Path(pathstr)

    def batchfile_status_path(self, batch_index):
        pathstr = self.root_path_str+f"batchfile_{batch_index}_status.txt"
        return Path(pathstr)

    def get_batch_status(self, batch_index):
        return BatchState[self.batchfile_status_path(batch_index).read_text()]

    def save_batch_status(self, batch_index):
        self.batchfile_status_path(batch_index).write_text(
            self.batch_status[batch_index].name)

    def init_rss(self, source_rss):
        """
        Save source rss to disk, and mark state
        """
        rss_path = Path(self.root_path_str+SOURCE_RSS)
        with rss_path.open("w") as f:
            header = source_rss[0].keys()
            writer = csv.DictWriter(f, fieldnames=header)
            writer.writeheader()
            for link in source_rss:
                writer.writerow(link)

        self.status = WorkState.RSS_READY
        self.save_status()

    def init_batches(self, batches, batchmap, omitted=None):
        """
        Save batches and batchmap to disk, and mark state.
        """
        # Save the batches, the batchmap, and set the status to ready
        for batch_index, batch in enumerate(batches):
            batchpath = self.batchfile_path(batch_index)
            with batchpath.open("w") as f:
                header = batch[0].keys()
                writer = csv.DictWriter(f, fieldnames=header)
                writer.writeheader()
                for link in batch:
                    writer.writerow(link)

            self.batch_status[batch_index] = BatchState.READY
            self.save_batch_status(batch_index)

        batchmap_path = Path(self.root_path_str+"/"+BATCHMAP)
        with batchmap_path.open("w") as f:
            header = ['domain', 'batch_index']
            writer = csv.DictWriter(f, fieldnames=header)
            writer.writeheader()
            for domain, index in batchmap.items():
                writer.writerow({"domain": domain, "batch_index": index})

        self.status = WorkState.BATCHES_READY
        self.save_status()

    def get_batch(self, batch_id):
        """
        Return a list of urls for a given batch id, and set the work state and batch state to FETCHING
        """
        self.status = WorkState.BATCHES_FETCHING
        self.save_status()

        self.batch_status[batch_id] = BatchState.FETCHING
        self.save_batch_status(batch_id)

        path = self.batchfile_path(batch_id)

        with path.open() as p:
            reader = csv.DictReader(p)
            batch = [i for i in reader]

        return batch

    def link_hash(self, link, reverse=False):
        """
        Return a file-URI safe string for a given url.
        Also implements the inverse operation.
        """
        if not reverse:
            return link.replace("/", "\\")
        else:
            return link.replace("\\", "/")

    def link_path_str(self, link, error=False):
        if error:
            return self.error_path_str+self.link_hash(link)
        else:
            return self.content_path_str+self.link_hash(link)

    def link_status(self, link):
        # We can test this on the fly by reading what exists in the content dir.
        pass

    def put_fetched(self, link, html_content, http_meta):
        path = self.link_path_str(link)

        html_out = Path(path+"%-raw.html")
        html_out.write_bytes(html_content)

        http_out = Path(path+"%-http_meta.json")
        with http_out.open("w") as out:
            json.dump(http_meta, out)

    def put_fetch_error(self, link, http_meta):
        path = self.link_path_str(link, error=True)
        http_out = Path(path+"%-fetch_error.json")
        with http_out.open("w") as out:
            json.dump(http_meta, out)

    def get_fetched(self, link):
        pass

    def get_http_meta(self, link):
        pass

    def put_content(self, link, content_json):
        pass

    def get_content(self, link):
        pass

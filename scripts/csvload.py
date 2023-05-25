"""
Create queue messages for files downloaded from S3 (w/ s3download)

CSV is expected to contain:
collect_date (YYYY-MM-DD HH:MM:SS[.uuuuuu])
downloads_id
url

May also contain: stories_id, media_id, feeds_id
"""

import argparse
import csv
import os
import sys
import time
from typing import Optional

# local:
from pipeline.worker import Worker, run

class CSVLoad(Worker):
    def define_options(self, ap: argparse.ArgumentParser):
        ap.add_argument('csv', help="csv file with downloads_id, url")
        ap.add_argument('dir', help="directory with DOWNLOADS_ID.gz files")
        super().define_options(ap)

    def main_loop(self, conn, chan):
        f = open(self.args.csv)
        reader = csv.DictReader(f, dialect='excel')

        dir = self.args.dir
        items = []
        for row in reader:
            dl_id = row['downloads_id']
            filename = os.path.join(dir, row['downloads_id'])
            if not os.path.exists(filename):
                if os.path.exists(filename + '.gz'):
                    filename += '.gz'
                else:
                    print(filename, "not found")
                    continue

            items.append({'csv': row, 'filename': filename, 'url': row['url']})
            if len(items) >= 5:
                self.send_items(chan, items)
                items = []
        if items:
            self.send_items(chan, items)

if __name__ == '__main__':
    run(CSVLoad, "csvload", "load stories from CSV")

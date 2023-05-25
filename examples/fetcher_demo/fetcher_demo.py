from HtmlFetchingWorker import HtmlFetchingWorker
from datetime import datetime
from pipeline.worker import run

import argparse

parser = argparse.ArgumentParser(description="test parser with supervisor")
parser.add_argument("--num_batches", type=int, help="The number of batches being executed", required=True)
parser.add_argument("--date", type=str, help="the date (as a string in %Y-%m-%d) being scraped for", required=True)
parser.add_argument("--batch_index", type=int, help="The batch index which this will run on", required=True)
parser.add_argument("--sample_size", type=int, help="For testing, how much of the rss to sample before batching. 0 means all", default=0)

args = parser.parse_args()

date = datetime.strptime(args.date, "%Y-%m-%d") 

if __name__ == "__main__":
    run(HtmlFetchingWorker, 
        "demo-fetcher",
        "Demo HTML-fetching pipeline worker"
        date = date, 
        batch_index=args.batch_index, 
        num_batches=args.num_batches, 
        sample_size=args.sample_size)
"""
metadata extractor worker
"""

import datetime as dt
import gzip
import json
import logging

# PyPI:
import mcmetadata

# local:
from pipeline.worker import ListConsumerWorker, run

logger = logging.getLogger(__name__)


def load_html(item):
    # Check for saved headers (from recently downloaded HTML)
    # to determine original Content-Type???

    # With old html downloaded from S3, any Content-Type headers have
    # been lost, and not everything on the web is UTF-8, so we have to
    # read the file as binary bytes and try different decoding schemes.

    filename = item['filename']
    if filename.endswith('.gz'):
        basename = filename[0:-3]
        with gzip.GzipFile(filename) as f:
            html_bytes = f.read()
    else:
        basename = filename
        with open(filename, 'rb') as f:
            html_bytes = f.read()

    # look for BOM first?
    try:
        html = html_bytes.decode()
    except:
        try:
            html = html_bytes.decode('utf-16')
        except:
            try:
                html = html_bytes.decode('utf-16')
            except:
                # XXX fall back to ISO-8859-1 (Latin-1)?
                # It would be wise to see if iso-8859-1 decode of
                # arbitrary binary EVER fails (if it doesn't,
                # it could return trash)!
                print("could not decode", filename)
                return None, None

    return basename, html


class Meta(ListConsumerWorker):

    def process_item(self, item):
        basename, html = load_html(item)
        if html is None:
            # XXX shunt message to quarantine?
            return None

        try:
            meta = mcmetadata.extract(item['url'], html)
            # make JSON safe:
            if isinstance(meta['publication_date'], dt.datetime):
                meta['publication_date'] = meta['publication_date'].isoformat()
        except mcmetadata.exceptions.BadContentError:
            # XXX shunt message to quarantine?
            meta = {}

        item['meta'] = meta

        json_fname = basename + '.json'
        with open(json_fname, 'w') as f:
            json.dump(item, f)
        logger.info(f"wrote {json_fname}")

        return None             # no output message generated for now


if __name__ == '__main__':
    run(Meta, "meta", "metadata extractor worker")

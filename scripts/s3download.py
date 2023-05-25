"""
Download raw HTML archived on in the AWS S3
mediacloud-downloads-backup bucket.

Database epochs B & D use overlapping download ids ("D" was created as
a stopgap after 32-bit story ids were exhausted, by restoring an old
backup, but without advancing the downloads_downloads_id_seq next
value to be clear of previously used values), BUT bucket versioning
was enabled, so it's possible to retrieve S3 objects based on their
LastModified time:

B: 2021-09-15 thru 2021-11-20
D: 2021-12-26 thru 2022-01-25

Daily CSV files extracted by C4A are in S3 buckets:
mediacloud-database-[b-e]-files

CSV is expected to contain:
collect_date (YYYY-MM-DD HH:MM:SS[.uuuuuu])
downloads_id
url

May also contain: stories_id, media_id, feeds_id

"""

import csv
import datetime as dt
import os
import sys
import time
from typing import Optional

# PyPI:
import boto3

BUCKET = 'mediacloud-downloads-backup'

def epoch_b(date: str) -> bool:
    """expects date in format 'YYYY-MM-DD .....'"""
    return date >= '2021-09-15' and date < '2021-11-21'

def epoch_d(date: str) -> bool:
    """expects date in format 'YYYY-MM-DD .....'"""
    return date >= '2021-12-26' and date < '2022-01-26'

def find_object_version(bucket, date, s3path) -> Optional[str]:
    """
    should only be called w/ date in epoch_b or epoch_d
    """
    # can only lookup by prefix!
    versions = bucket.object_versions.filter(Prefix=s3path, MaxKeys=1)

    # should only have two
    saved = []
    for version in versions:
        if version.key != s3path:
            break
        meta = version.get()    # fetch metadata
        # contains VersionId, ContentLength, LastModified
        # may need fuzzier test (S3 object created on day after)
        # could convert input date to datetime, and do subtraction
        last_mod = meta['LastModified'].strftime("%Y-%m-%d")
        if epoch_b(date) and epoch_b(last_mod):
            return meta['VersionId']
        if epoch_d(date) and epoch_d(last_mod):
            return meta['VersionId']
        saved.append(meta)
    found = " ".join([str(meta['LastModified']) for meta in saved])
    print(f"could not find version for {s3path} {date} found {found}")
    return None

def main() -> None:
    if len(sys.argv) < 3:
        sys.stderr.write(f"Usage: {sys.argv[0]} CSV_FILE DOWNLOAD_DIR\n")
        sys.exit(1)

    f = open(sys.argv[1])
    dest_dir = sys.argv[2]
    if not os.path.isdir(dest_dir):
        os.makedirs(dest_dir)

    reader = csv.DictReader(f, dialect='excel')

    s3 = boto3.resource('s3')
    bucket = s3.Bucket(BUCKET)

    t0 = time.time()
    found = downloaded = no_version = 0
    for row in reader:
        dl_id = row['downloads_id']
        output = os.path.join(dest_dir, dl_id + '.gz')
        if os.path.exists(output):
            found += 1
            continue

        s3path = f"downloads/{dl_id}"
        date = row['collect_date']
        if epoch_b(date) or epoch_d(date):
            version = find_object_version(bucket, date, s3path)
            if not version:
                no_version += 1
                continue
            print(s3path, version)
            extra = {'VersionId': version}
        else:
            extra = {}

        bucket.download_file(s3path, output, extra)
        downloaded += 1

    print(f"found {found} no_version {no_version} downloaded {downloaded} in {time.time() - t0}")

if __name__ == '__main__':
    main()

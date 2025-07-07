XXX means needs to be looked at

# Media Clould story-indexer metadata

## Background

This document was written to cover a number of bases:

* Document WARC files written by Media Cloud story-indexer `archiver` for backup/archive for future developers, both at Media Cloud and outside.
* Document Media Cloud story-indexer `Story` object metadata fields; including their sources, and uses.
* Document metadata required by story-indexer for anyone preparing data for incorporation into Media Cloud Elasticsearch index.
* Document additional data written by `qutil dump_archives` command.
* Document metadata fields stored and indexed in Elasticsearch.
* Give an overall description of story-indexer pipelines and 

NOTE! As with all documentation, this reflects (at best) the state of
the code (as read by a human) at a given instance in time.  If it's
important, check the code first (hopefully this give an indication of
where to look).

## Pipeline Overview

The Media Cloud story-indexer is a toolkit for constructing pipelines
of processing steps, with the mechanics of story representation and
messaging abstracted by classes in `story.py` and `storyapp.py`
respectively.

A full pipeline has the following steps
(specific pipeline flavors may omit some steps):

1. Queuer
2. Fetcher
3. Parser
4. Importer
5. Archiver

Pipelines are deployed using Docker by the `docker/deploy.sh` script
using Docker Swarm.  Pipelines are configured by `pipeline.py` (run in
a container), and individual programs have no knowledge of their
position in a pipeline.  Pipelines need not be linear: the output of a
step can be queued to multiple steps.

Multiple flavors of pipeline exist.  Queuers and Fetchers come in
multiple flavors, and are run in pairs.  (see Appendix: XXX link
here?)

## WARC file format

Media Cloud archival WARC files are written as WARC 1.0 archives using
`warcio` by `workers/archiver.py` using `story_archive_writer.py` and
consist of a header "warcinfo" record, followed by pairs of "response"
and "metadata" records.

### WARC metadata record

Media Cloud Archive WARC "metadata" records have Content-Type
`application/x.mediacloud-indexer+json` (see story_archive_writer.py
for COPIOUS discussion of how this was arrived at!) and the content is
a JSON encoded representation of the story-indexer `Story` object
metadata sub-objects (with VERY few exceptions), using the names of
the sub-object fields!!!

story-indexer is type hinted and checked with `mypy` so deviations
from the specified types should be uncommon.  Due to practical
considerations, just about all fields of a Story "in flight" may have
a null value.  Be liberal in what you accept.

In this section "MAY" means "depending on availability of
this or other data", not "I'm not sure"!

The top level is a JSON "object" (hash) with the following keys:

#### rss_entry

Data from the Story object RSSEntry subobject.

Data in this section is generally used for communication between Queuer
and Fetcher workers, and may vary depending on how the Story was acquired,
fetched, and processed.

"(forensic)" means the data is only for post-mortem examination,
and is not otherwise used.

##### link (str)

The Story URL to fetch, extracted from original RSS or Sitemap page,
used by Fetcher.

##### title (str)

The Story title (if any), extracted from original RSS or Sitemap page
(forensic).

##### domain (str)

Media Cloud "canonical domain" extracted using mcmetadata library
by rss-fetcher (forensiv).

##### pub_date (str)

The Story publishing data (if any) in RFC2822 format, extracted from
original RSS or Sitemap page, MAY be used by importer.

##### fetch_date (str)

Date (YYYY-MM-DD) the story was acquired: the batch fetcher put
the date of the synthetic RSS file here (forensic).

##### source_url (str)

The URL of the page the story URL was extracted from, if any
(forensic).

##### source_feed_id (int)

The Media Cloud id of the feed the story URL was extracted from, if any
(forensic).

##### source_source_id (int)

The Media Cloud source id associated with the feed the story URL was
extracted from, if any (forensic).

##### via (str)

Describes how/where the story was acquired from, for human consumption. May be the name of a file (forensic).

#### http_metadata

Data from the Story object HTTPMetadata subjobject.  Generally set by
Fetchers (or Queuers).

##### response_code (int)

HTTP Response code.  Should be 200.  Used by Archiver to write
WARC "response" record.

##### fetch_timestamp (float)

UNIX epoch time (seconds since 1970-01-01 UTC), with fractional microseconds.
If available, used by archiver (story_archive_writer) for WARC-Date.

##### final_url (str)

Final (redirected) URL (if known) of the fetched story, passed to mcmetadata
by `parser.py`.

##### encoding (str)

Page encoding from HTTP Content-Encoding header.
MAY be used by `parser.py`.

#### content_metadata

Data from the ContentMetadata subobject.

A dump of metadata extracted by `mcmetadata.extract` (all names
identical).  Only the fields used by story-indexer are included here.
See github.com/mediacloud/metadata-lib (XXX make into link?) for
details.

Elasticsearch field names are (XXX all?) based on these names.

##### url (str)

Final URL.  Required.

##### canonical_domain (str)

Media Cloud "canonical domain".  Required.

##### publication_date (str)

Publication date "YYYY-MM-DD" extracted by Parser.
If not present, Importer will parse and use rss_entry `pub_date`.
May be null if not available.

##### language (str)

Lower case two-letter ISO 3166-1 alpha-2 country code.

##### article_title (str)

##### text_content (str)

##### parsed_date (str)

Only in Elasticsearch (not present in Story object or WARC files),
Despite the name, is UTC date-time (YYYY-MM-DD hh:mm:ss.uuuuuu as
returned by Python datetime.utcnow().isoformat()) article was parsed).
If not available, Importer will use datetime.utcnow().isoformat() of
at time of import.

This field was in flux early on, and may be a bare YYYY-MM-DD in some
stories imported in prior to 

### WARC "response" record

`WARC-Target-URI` header is (in preferred order):
http_metadata.final_url, content_metadata.url, content_metadata.original_url or rss_entry.link

`WARC-Date` is formatted from http_metadata.fetch_timestamp or the current time.
In WARC version 1.0 date-times are limited to one second granularity.

If http.metadata.response_code is 200, the HTTP response will be "200
OK", otherwise it will be "recponse_code HUH?"

The HTTP `Content-Type` header will always be `text/html`; if http_metadata.encoding
(or raw_html.encoding) is available `; encoding=VALUE` will be appended.

## Appendices

### Pipeline types

#### queue-fetcher

(normal production)

1. `workers/fetcher/rss-puller.py` periodically to fetch new stories using the `rss-fetcher` API as a Queuer
2. `workers/fetcher/tqfetcher.py` (threaded queue fetcher) to continuously fetch queued stories using threads for parallelism
3. `workers/parser.py` to extract content, content metadata
4. `workers/importer.py` to import to Elasticsearch
5. `workers/archiver.py` to create WARC files and upload to cloud storage

Archived WARC file names are prefixed with `mc-`

rss_entry is populated from JSON encoded data recieved from the rss-fetcher API.

All rss_entry are used as intended, `via` contains `ID_NUMBER@SERVER`

All http_metdata fields are used as intended.

#### historical

1. `workers/fetcher/hist-queuer.py` reading CSV files from cloud storage
2. `workers/fetcher/hist-fetcher.py` fetching HTML from cloud storage
3. `workers/parser.py` to extract content, content metadata
4. `workers/importer.py` to import to Elasticsearch
5. `workers/archiver.py` to create WARC files and upload to cloud storage

Reads CSV files dumped from original Media Cloud system, with Story URL
and a `downloads_id` which is used to locate the HTML document
(as originally downloaded) from cloud storage.

NOTE!  The URL is *NOT* the final, redirected URL, and any HTTP
metadata was not retained, so page encoding must be inferred.

EXCEPT for periods where CSVs dumped from the old system were not
available, where documents pre-determined ranges of downloads_ids were
fetched "blind" (without any URL), and the URL was extracted by the
parser from "canonical url" HTML metdata tags (in which case the
extracted URL is the canonoical URL from the document producer).

Archived WARC file names are prefixed with `mchistYYYY-`

hist-queuer sets the following `rss_entry` fields:

* `link` contains a downloads_id (stringified int)
* `fetch_date` contains a date extracted from the CSV filename
* `source_feed_id` contains a feed id from the CSV, if available
* `source_source_id` contains a source id from the CSV, if available
* `via` contains the file name of the CSV file (S3 URL??)

hist-queuer sets the following `http_metadata` fields:

* `final_url` from url column in CSV file (not ACTUAL final URL). In "blind fetch" mode, url http://mediacloud.org/need_canonical_url will be supplied.
* `fetch_timestamp` is parsed from `collect_date` column of CSV (if available)

hist-queuer sets the following `content_metadata` fields:

* `language` and `full_language` set if CSV contains a language column, but this data is not used!

hist-fetcher sets `http_metadata.response_code` to 200,

#### csv

Reads CSVs of URLs to fetch.  The original CSVs were prepared by blind
fetching objects from the old system cloud storage archive, looking
for RSS files, and extracting story URLs.

1. `workers/fetcher/csv-queuer.py` reads CSV files from cloud storage
2. `workers/fetcher/tqfetcher.py`
3. `workers/parser.py`
4. `workers/importer.py`
5. `workers/archiver.py`

Archived WARC file names are prefixed with `mccsvYYYY-` (XXX??)  (some
written as `mc-`). Past archives should be ignored, as the stories were
removed (to avoid duplicates) and replaced with "blind fetched"
(extracting canonical URL) using the historical pipeline, to avoid
loss of stories due to "link rot".

No stories fetched this way are should currently be in the ES Index.

Description here is for completeness.  The CSV files were prepared from
RSS files.

* `rss_entry.link` and `http_metadata.final_url` are set from CSV url column
* `rss_entry.fetch_date` is set from date extracted from CSV file name

#### archive

Reads Media Cloud Archiver WARC files (from cloud storage) and
re-imports them for recovery (this was done for the first reindexing
in the early months of the new system).  No new archives are written.
Stories are imported using original "parsed date".

1. `workers/fetcher/arch-queuer.py` reads WARC files using `StoryArchiveReader`
2. `workers/importer.py`

`StoryArchiveReader` returns `Story` objects. Metadata is populated using the "metadata"
WARC record;  Cannot process arbitrary WARC files!!

#### batch-fetcher (obsolete)

Used whole-day synthetic RSS files from rss-fetcher, using "scrapy" to
fetch the HTML.  Ran until all stories fetched or (limited) retries exhausted.

Was replaced by `tqfetcher` which allows longer retries, and better
ability to recover from story-indexer downtime.

1. `workers/fetcher/fetch-worker.py`
2. `workers/parser.py`
3. `workers/importer.py`
4. `workers/archiver.py`

### qutil dump_archives output

The qutil program (run via `bin/run-qutil.sh dump_archives queue_name`
in a Docker container) writes WARC files from currently queued stories.

It attempts to add a `rabbitmq_headers` metadata object, BUT it will
fail if any RabbitMQ headers contain data types (eg; datetimes) that
are not JSON serializable!


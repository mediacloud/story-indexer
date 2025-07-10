# Media Cloud story-indexer metadata

The key job of the story-indexer is to extract, augment, and archive online news stories. To accomplish
these tasks the story-indexer creates a number of metadata fields, persisted in different ways and at 
different times. These include:
* `Story` objects used internally to manage data for workflow queues
* Elasticsearch storage fields
* `.warc` files files used as off-site long-term storage

This document describes that metadata.

## Background

This document was written to fill a number of needs:

* Document WARC files written by Media Cloud story-indexer `archiver` for backup/archive for use by future developers, both at Media Cloud and outside.
* Document Media Cloud story-indexer `Story` object metadata fields; including their sources, and uses.
* Document metadata required by story-indexer for anyone preparing data for incorporation into Media Cloud Elasticsearch index.
* Document additional data written by `qutil dump_archives` command.
* Document metadata fields stored and indexed in Elasticsearch.
* Give an overall description of story-indexer pipelines flavors and their metadata (ab)usage.

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
multiple flavors, and are run in pairs.  (see Appendix A).

## WARC file format

Media Cloud archival WARC files are written as
[WARC 1.0 archives](https://iipc.github.io/warc-specifications/specifications/warc-format/warc-1.0/)
using `warcio` by `workers/archiver.py` using
`story_archive_writer.py` and consist of a header `warcinfo` record,
followed by pairs of `response` and `metadata` records.  Media Cloud
WARC files should contain no more than 5000 stories.

### WARC metadata record

Media Cloud Archive WARC `metadata` records have Content-Type
`application/x.mediacloud-indexer+json` (see `story_archive_writer.py`
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

The top level is a JSON "object" (dict) with the following keys:

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
by rss-fetcher (forensic).

##### pub_date (str)

The Story publishing data (if any) in RFC2822 format, extracted from
original RSS or Sitemap page, MAY be used by importer if Parser
cannot extract a publication date.

##### fetch_date (str)

Date (YYYY-MM-DD) the story was acquired: the original (batch) fetcher put
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

Describes how/where the story was acquired from, for human
consumption. Generally added by the Queuer and may be the name of a
file (forensic).

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
See github.com/mediacloud/metadata-lib for details.

Elasticsearch field names map (almost) EXACTLY to these fields!

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

Extracted article title.

##### text_content (str)

Extracted text.

##### parsed_date (str)

Populated by Parser (not mcmetadata), 
despite the name, is UTC date-time (YYYY-MM-DD hh:mm:ss.uuuuuu

Importer stores in Elasticsearch as `indexed_date`; If not available,
Importer will use datetime.utcnow().isoformat() of at time of import.

This field was in flux early on, and may be a bare YYYY-MM-DD in some
stories imported in early 2024.

Some API clients use `indexed_date` to return only newly added
stories.

The latest Elasticsearch mapping for `indexed_date` has nanosecond
resolution to avoid truncation from microseconds (as retrieved from
the stored document) to milliseconds in the index.

### WARC "response" record

`WARC-Target-URI` header is (in preferred order) on of
`http_metadata.final_url`, `content_metadata.url`,
`content_metadata.original_url`, or `rss_entry.link`.

`WARC-Date` is formatted from `http_metadata.fetch_timestamp` or the
current time.  In WARC version 1.0 date-times are limited to one
second granularity.

If `http.metadata.response_code` is 200, the HTTP response will be "200
OK", otherwise it will be "response_code HUH?"

The HTTP `Content-Type` header will always be `text/html`; if
http_metadata.encoding (or raw_html.encoding) is available
`; encoding=VALUE` will be appended.

## Appendices

### Apendix A: Pipeline types

story-indexer pipelines come in various flavors, usually starting
with a Queuer to allow the next step to be run in parallel.

This appendix describes how the different flavors of Queuer
(and their associated Fetcher, if any) (ab)use metadata fields.

Queuers are generally simple: a subclass of the `Queuer` class
providing a `process_file` method, which takes a file i/o stream,
which may be backed by a gzip'ed file on cloud storage.  The hard(er)
part is figuring out what metadata fields to populate.

The `Queuer` class provides a `main_loop` method which takes any
number of inputs (local files, directories, HTTP URLs, or cloud
storage ("blobstore") URLs; suffix `*` wildcards are supported in
blobstore URLs).  Files are only queued when the output queue(s) is
below a low-water mark, and until the output queue(s) reach a
high-water mark.  After successful queuing, (base) file names are
recorded in an SQLite3 "tracker" database, and will be skipped
thereafter.

#### queue-fetcher

(normal production)

1. `workers/fetcher/rss-puller.py` periodically to fetch new stories using the `rss-fetcher` API as a Queuer
2. `workers/fetcher/tqfetcher.py` (threaded queue fetcher) to continuously fetch queued stories using threads for parallelism
3. `workers/parser.py` to extract content, content metadata
4. `workers/importer.py` to import to Elasticsearch
5. `workers/archiver.py` to create WARC files and upload to cloud storage

Archived WARC file names are prefixed with `mc-`

`rss_entry` is populated from JSON encoded data received from the
rss-fetcher API, all fields are used as intended, `via` contains
`ID_NUMBER@SERVER`

All `http_metadata` fields are used as intended.

#### historical

Used to read raw HTML from the cloud storage "downloads-backup" bucket
of the old Media Cloud system, using CSV files dumped from the old
PostgreSQL database to provide metadata.

1. `workers/fetcher/hist-queuer.py` reading CSV files from cloud storage
2. `workers/fetcher/hist-fetcher.py` fetching HTML from cloud storage
3. `workers/parser.py` to extract content, content metadata
4. `workers/importer.py` to import to Elasticsearch
5. `workers/archiver.py` to create WARC files and upload to cloud storage

NOTE!  The URL is *NOT* the final, redirected URL, and any HTTP
metadata was not retained, so page encoding must be inferred.

For periods where CSV files were not available (due to unrecoverable
databases), downloads_id ranges for the date ranges were identified,
and trivial CSV files were created (containing only a `downloads_id`),
and the HTML was fetched "blind" (without any metadata), and the URL
was extracted by the parser from "canonical url" HTML metadata tags
(in which case the extracted URL is the Canonical URL from the
producer of the document).

Archived WARC file names are prefixed with `mchistYYYY-`

hist-queuer sets the following `rss_entry` fields:

* `link` contains a `downloads_id` (stringified int)
* `fetch_date` contains a date extracted from the CSV filename
* `source_feed_id` contains a feed id from the CSV, if available
* `source_source_id` contains a source id from the CSV, if available
* `via` contains an S3 URL for the CSV file

hist-queuer sets the following `http_metadata` fields:

* `final_url` from url column in CSV file (not ACTUAL final URL). In "blind fetch" mode, url http://mediacloud.org/need_canonical_url will be supplied.
* `fetch_timestamp` is parsed from `collect_date` column of CSV (if available)

hist-queuer sets the following `content_metadata` fields:

* `language` and `full_language` set if CSV contains a language column, but this data is not used!

hist-fetcher sets `http_metadata.response_code` to 200, and if no
`fetch_timestamp` was supplied (as is the case for "blind" fetches),
the S3 `LastModified` date/time is fetched and used.

An added complication is that 32-bit database row ids were exhausted,
and to allow time to convert to 64-bit ids, an older backup was
restored, and two different time "epochs" used the same downloads_id
values.  S3 contained versioned objects (files) from both epochs, and
for certain ranges of downloads_id, hist-fetcher needed to retrieve
the S3 VersionId and LastModified date for each version, and select
the object matching the epoch of the `http_metadata` `fetch_timestamp`
(CSV `collect_date`).

#### csv

Reads CSVs of URLs to fetch.  The original CSVs were prepared by blind
fetching objects from the old system cloud storage archive, looking
for RSS files, and extracting story URLs.

1. `workers/fetcher/csv-queuer.py` reads CSV files from cloud storage
2. `workers/fetcher/tqfetcher.py`
3. `workers/parser.py`
4. `workers/importer.py`
5. `workers/archiver.py`

Most archived WARC file names are prefixed with `mccsv-` (some written
as `mc-`). Past archives should be ignored, as the stories were
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

`StoryArchiveReader` returns `Story` objects. Metadata is populated
using the `metadata` WARC record without
interpretation. `StoryArchiveReader` cannot process arbitrary WARC
files!!

If parsing (and archiving) data from WARC files is desired, a new
pipeline flavor should be created.

#### batch-fetcher (obsolete)

Used whole-day synthetic RSS files from rss-fetcher, and used "scrapy" to
fetch the HTML.  Ran until all stories fetched or (limited) retries exhausted.

Was replaced by `tqfetcher` which allows longer retries, and better
ability to catch up after story-indexer downtime.

1. `workers/fetcher/fetch-worker.py`
2. `workers/parser.py`
3. `workers/importer.py`
4. `workers/archiver.py`

### Appendix B: qutil dump_archives output

The qutil program (run via `bin/run-qutil.sh dump_archives queue_name`
in a Docker container) writes WARC files from currently queued stories.

It attempts to add a `rabbitmq_headers` metadata object, BUT it will
(currently) fail if any RabbitMQ headers contain data types (eg;
datetimes) that are not JSON serializable
[A github PR exists to solve this, by only dumping "x-mc-..." headers].

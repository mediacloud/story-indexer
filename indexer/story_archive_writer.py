"""
classes to write archives of Story objects (and read them too)
"""

# In a file of its own so it can be reused (however unlikely), and
# more easily tested, because it may require considerable tweaking,
# AND because of the VOLUMINUOUS comments about what the WARC spec
# does, and doesn't say, and about the choices.

import datetime as dt
import json
import os
import time
from io import BufferedWriter, BytesIO
from logging import getLogger
from typing import Any, BinaryIO, Dict, Iterator, Optional, cast

from warcio.archiveiterator import ArchiveIterator
from warcio.statusandheaders import StatusAndHeaders
from warcio.warcwriter import WARCWriter

from indexer.story import BaseStory, StoryFactory

ARCHIVE_WRITER_SOFTWARE = "mediacloud story-indexer ArchiveWriter"
ARCHIVE_EXTENSION = ".warc.gz"

Story = StoryFactory()

# WARC spec readings (November 2023):
# * http://bibnum.bnf.fr/WARC/
# * https://github.com/iipc/warc-specifications
# * http://iipc.github.io/warc-specifications/
# * http://iipc.github.io/warc-specifications/specifications/warc-format/warc-1.1-annotated/

# Writes mediacloud stort-indexer Story object metadata as a WARC 'metadata' record.

# This makes reading the file more of a chore: the reader has to check
# if subsequent records reference the ones already read.  It might be
# *FAR* simpler to simply write all our metadata as private format
# headers in the (forged) HTTP response, ie; "X-MC-Content-Lang: en"

# Initial note:
# Internet specs (published as RFCs) have a specification(!) for the
# use of words "MUST", "MUST NOT", "REQUIRED", "SHALL", "SHALL NOT",
# "SHOULD", "SHOULD NOT", "RECOMMENDED",  "MAY", and "OPTIONAL"
# defined in https://www.ietf.org/rfc/rfc2119.txt, which also
# defines a statement to be included at the top of any document
# referencing it.

# The WARC specification references MANY RFCs, but does not reference
# RFC2119.  This could be because it isn't an RFC, and published as an
# ISO standard, other standards apply to the meaning of words (and the
# ISO doesn't want to be held to another Standards Organization's
# standards definition standards) but they don't say ANYTHING about
# the meaning of their words!

#       "When I use a word," Humpty Dumpty said in rather a scornful
#       tone, "it means just what I choose it to mean -- neither more
#       nor less."  "The question is," said Alice, "whether you can
#       make words mean so many different things."
#
#               Lewis Carrol, "Through the Looking Glass"


# Confusingly, the WARC 1.1 spec section 6.6 on 'metadata' says:
#
#   The "application/warc-fields" format may be used.
#
# While secion 8 says:
#
#   The MIME type of warcinfo records, WARC metadata records,
#   and potentially other records types in the future, shall
#   be application/warc-fields.
#
# This contradiction is noted in warc-1.1-annotated (see above)
# and discussed (for warcinfo records) in an (open) issue:
#  https://github.com/iipc/warc-specifications/issues/50
# and
#  https://github.com/iipc/warc-specifications/issues/54
#   which documents tools that have different content-types
#   usages, including non "warc-fields" content-type for
#   "metadata" records, INCLUDING "application/json"
#
# AND since section 6.6 ALSO says:
#   Any number of metadata records may reference another specific record.
#   which implies one might have many types of metadata records that
#   I think we're PERFECTLY free to put JSON in a 'metadata' record,
#
# An alternative to this is WACZ format (Web Archive
# Collection Zipped), which is a ZIP file of (optionally gzip'ed)
# WARC files: https://specs.webrecorder.net/wacz/1.1.1/
# Which includes a "pages.jsonl" file (section 5.2.3):
#    Each entry in the [JSONL] file MAY contain additional
#    properties as long as they do not interfere with the required
#    properties.
#
# And however tempting it is to squeeze though a larger keyhole,
# WACZ is more complex, and it's better to avoid the additional
# complexity.

# Using Content-Type "application/json" for a JSON dump
# of metadata would only describe how the data is being
# presented, and nothing about the semantics.

# Looking at the offical list of registered content-types:
# https://www.iana.org/assignments/media-types/media-types.xhtml
# shows use of +json and +xml
#
# It appears https://www.rfc-editor.org/rfc/rfc3023 introduces +xml
# I cannot find an RFC documenting use of +json.
#
# https://www.rfc-editor.org/rfc/rfc4288 says:
#   For convenience and symmetry with this registration scheme, subtype
#   names with "x." as the first facet may be used for the same purposes
#   for which names starting in "x-" are used.  These types are
#   unregistered, experimental, and for use only with the active
#   agreement of the parties exchanging them.
#
#   However, with the simplified registration procedures described above
#   for vendor and personal trees, it should rarely, if ever, be
#   necessary to use unregistered experimental types.  Therefore, use of
#   both "x-" and "x." forms is discouraged.
#
# so we _could_ register vnd.mediacloud-indexer+json
# as if we're a "vendor"

METADATA_CONTENT_TYPE = "application/x.mediacloud-indexer+json"

logger = getLogger(__name__)


class ArchiveWriterError(RuntimeError):
    """
    base for all other ArchiveWriter run time errors
    """


class ArchiveStoryError(ArchiveWriterError):
    """
    error thrown by ArchiveWriter to indicate not saving a Story.
    First arg WILL be used as a counter name!!
    (so keep it short, and use hyphens, not spaces or underscores)
    """


class FileobjError(ArchiveWriterError):
    """
    error thrown by fileobj method if cannot return fileobj
    """


# WARC version to write.
# Using 1.0 for maximum acceptance.
# The only difference in Version 1.1 files
# can specify fractional seconds in timestamps.
WARC_VERSION = WARCWriter.WARC_1_0


# Replaces _massage_metadata, and handles arbitrarily arbitrarily
# deep/structured data. Regular metadata _should_ be only JSON-safe data, but
# since DiskStory not used in production, this this first place badness would be
# seen if someone adds a non-JSON datatype.  Changed to default handler to deal
# with RabbitMQ header data from qutil.py "dump_archives" command.
def _json_default_handler(value: Any) -> str:
    """
    default handler for json.dump[s] to handle date[time]
    (imported by qutil, for old JSON dumper)
    """
    if isinstance(value, (dt.datetime, dt.date)):
        return value.isoformat()
    return repr(value)  # same as old _massage_value


class StoryArchiveWriter:
    """
    Class to write Story object archives as WARC files.

    API hides the fact that files are WARCs,
    so it could be replaced with something else.
    """

    def __init__(
        self,
        *,
        prefix: str,
        hostname: str,
        fqdn: str,
        serial: int,
        work_dir: str,
        rw: bool = False,
    ):
        self.timestamp = time.time()  # time used to generate archive name
        # WARC 1.1 Annex C suggests naming:
        # Prefix-Timestamp-Serial-Crawlhost.warc.gz
        # where Timestamp is "a 14-digit GMT time-stamp"
        if serial >= 0:
            ts = time.strftime("%Y%m%d%H%M%S", time.gmtime(self.timestamp))
            self.filename = f"{prefix}-{ts}-{serial}-{hostname}{ARCHIVE_EXTENSION}"
        else:
            self.filename = f"{prefix}{ARCHIVE_EXTENSION}"
        if work_dir:
            self.full_path = os.path.join(work_dir, self.filename)
        else:  # allow prefix to be absolute path for testing
            self.full_path = self.filename
        self.temp_path = f"{self.full_path}.tmp"
        if rw:
            # open returns BufferedRandom (subclass of BufferedWriter + BufferedReader)
            mode = "w+b"
        else:
            mode = "wb"  # open returns BufferedWriter
        self._file = open(self.temp_path, mode)
        self._rw = rw
        self._finished = False
        self.size = -1

        self.writer = WARCWriter(
            cast(BufferedWriter, self._file), gzip=True, warc_version=WARC_VERSION
        )

        # write initial "warcinfo" record:
        info = {
            "hostname": hostname,  # likely internal or Docker container
            # ip is almost CERTAINLY an RFC1918 private addr
            "software": ARCHIVE_WRITER_SOFTWARE,
            "format": "WARC file version " + WARC_VERSION.split("/")[-1],
            # others:
            # description, isPartOf, operator
            # http-header-user-agent (use if passed by fetcher in http_metadata?)
        }
        self.writer.write_record(
            self.writer.create_warcinfo_record(self.filename, info)
        )

    def write_story(
        self,
        story: BaseStory,
        extra_metadata: Optional[Dict[str, Any]] = {},
        raise_errors: bool = True,
    ) -> bool:
        """
        Append a Story to the archive

        extra_metadata is for qutil dump_archives command to save rabbitmq headers
        """
        # started from https://pypi.org/project/warcio/#description
        # "Manual/Advanced WARC Writing"
        # which shows a 'response' record without a 'request'

        # XXX check here if current WARC file over 1GB, and return False??
        # (WARC 1.1 Annex C suggest 10^9 as max file size)

        re = story.rss_entry()
        hmd = story.http_metadata()
        cmd = story.content_metadata()
        rhtml = story.raw_html()

        # NOTE! logging original_url (as elsewhere) for log tracing.
        original_url = cmd.original_url or re.link

        # qutil "dump_archives" can dump queue contents at ANY point in pipeline
        # (ie; before fetching or parsing), so must handle null values in http
        # metadata, raw html, and content metdata!
        url = hmd.final_url or cmd.url or original_url or ""
        html = rhtml.html or b""

        logger.debug("write_story %s %s %d bytes", original_url, url, len(html))

        if raise_errors:
            if not url:
                raise ArchiveStoryError("no-url")  # NOTE! used as counter!

            if html is None or not html:  # explicit None check for mypy
                raise ArchiveStoryError("no-html")  # NOTE! used as counter!

        ################ create a WARC "response" with the HTML (no request)

        # COULD forge a response record with a redirect
        # if original_url != url, but all the information
        # is present in our metadata, so not jumping
        # though any hoops here. Caveat Emptor.

        rcode = hmd.response_code
        if rcode == 200:
            http_response = "200 OK"
        else:
            http_response = f"{rcode} HUH?"

        content_type = "text/html"
        encoding = hmd.encoding or rhtml.encoding
        if encoding:
            content_type = f"{content_type}; encoding={encoding}"

        # NOTE! a peek inside warcio looks like passing an empty
        # list causes create_warc_record to do different stuff,
        # so oddness may occur if an empty http_headers_list is passed!
        http_headers_list = [
            ("Content-Type", content_type),
            ("Content-Length", str(len(html))),
        ]

        hmd_fetch = hmd.fetch_timestamp or time.time()

        # no fractional seconds in WARC/1.0:
        fetch_date = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(hmd_fetch))

        # any headers here appear to appear first in the WARC records.
        response_whd = {"WARC-Date": fetch_date}  # back date to when fetched

        response_record = self.writer.create_warc_record(
            url,
            "response",
            warc_headers_dict=response_whd,
            http_headers=StatusAndHeaders(
                http_response, http_headers_list, protocol="HTTP/1.0"
            ),
            payload=BytesIO(html),
        )
        self.writer.write_record(response_record)

        ################ WARC metadata record

        # Originally tempted to make the top/only entry in the dict
        # be "x-mediacloud", but the content-type makes the origin
        # clear.

        # dumping data as-is.  used to call _massage_metadata, but non-JSON
        # datatypes now handled by _json_default_handler
        metadata_dict = {
            "rss_entry": re.as_dict(),
            "http_metadata": hmd.as_dict(),
            "content_metadata": cmd.as_dict(),
        }

        # Used by qutil dump_archives, to dump ALL RabbitMQ headers,
        # which may include datetime data for messages that have been
        # retried, or were dumped from -delay or -fast queues:
        if extra_metadata:
            metadata_dict.update(extra_metadata)

        metadata_bytes = json.dumps(
            metadata_dict, indent=2, default=_json_default_handler
        ).encode()
        metadata_length = len(metadata_bytes)
        metadata_file = BytesIO(metadata_bytes)

        # additional/changed WARC headers
        # NOTE! references response record
        metadata_whd = {
            "Content-Type": METADATA_CONTENT_TYPE,
            "WARC-Refers-To": response_record.rec_headers["WARC-Record-ID"],
            "WARC-Date": fetch_date,  # XXX back date to when parsed??
        }

        metadata_record = self.writer.create_warc_record(
            url,
            "metadata",
            warc_headers_dict=metadata_whd,
            payload=metadata_file,
            length=metadata_length,
        )
        self.writer.write_record(metadata_record)

        # make what archive a story ended up in visible
        logger.info("%s -> %s", url, self.filename)

        return True  # written

    def finish(self) -> None:
        if not self._finished:
            self.size = self._file.tell()
            if self._rw:
                self._file.flush()
            else:
                self.close()

            if os.path.exists(self.temp_path):
                os.rename(self.temp_path, self.full_path)
                logger.info("renamed %s", self.full_path)
            self._finished = True

        # useful data now available:
        # self.filename: archive file name
        # self.full_path: full local path of output file
        # self.size: size of (compressed) output file
        # self.timestamp: timestamp used to create filename

    def fileobj(self) -> BinaryIO:
        """
        for use with blobstore.upload_fileobj
        (caller must rewind!)
        """
        if not self._file:
            raise FileobjError("no file")
        if self._file.closed:
            raise FileobjError("closed")
        if not self._rw:
            raise FileobjError("not r/w")
        if not self._finished:
            raise FileobjError("not finished")
        return cast(BinaryIO, self._file)

    def close(self) -> None:
        if self._file and not self._file.closed:
            self._file.close()


class StoryArchiveReader:
    """
    read an archive written by StoryArchiveWriter
    """

    def __init__(self, fileobj: BinaryIO):
        self.iterator = ArchiveIterator(fileobj)

    def read_stories(self) -> Iterator[BaseStory]:
        # read WARC file:
        expect = "warcinfo"
        html = b""
        for record in self.iterator:
            if record.rec_type != expect:
                # XXX log warning?
                continue
            elif expect == "warcinfo":
                # XXX log warning if software != ARCHIVE_WRITER_SOFTWARE?
                expect = "response"
            elif expect == "response":
                html = record.raw_stream.read()
                # strip leading HTTP response and headers
                if html.startswith(b"HTTP/") and (crcr := html.find(b"\r\n\r\n")) > 0:
                    html = html[crcr + 4 :]
                expect = "metadata"
            elif expect == "metadata":
                j = json.load(record.raw_stream)
                story = Story()
                # NOTE! flies under type-checking radar:
                # unsafe to load arbitrary/alien WARC files!!!
                with story.rss_entry() as rss:
                    for key, value in j["rss_entry"].items():
                        setattr(rss, key, value)
                with story.http_metadata() as hmd:
                    for key, value in j["http_metadata"].items():
                        setattr(hmd, key, value)
                with story.content_metadata() as cmd:
                    for key, value in j["content_metadata"].items():
                        setattr(cmd, key, value)
                    if not cmd.parsed_date:
                        # For archives written before "parsed_date" added;
                        # Use WARC metadata record WARC-Date header.
                        date = record.rec_headers["WARC-Date"]
                        if date.endswith("Z"):  # should always be the case
                            date = date[:-1]  # remove Z
                        cmd.parsed_date = date
                with story.raw_html() as rh:
                    rh.html = html
                    rh.encoding = j["http_metadata"]["encoding"]

                yield story
                expect = "response"

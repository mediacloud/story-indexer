# just what's used!

import io
from typing import Any, Dict, Optional, Union

from warcio.statusandheaders import StatusAndHeaders

class _WARCRecord:
    rec_headers: Dict[str, str]
    ...

class WARCWriter:
    WARC_1_0: str

    def __init__(self, file: io.BufferedWriter, gzip: bool, warc_version: str): ...
    def create_warcinfo_record(
        self, filename: str, info: Dict[str, Any]
    ) -> _WARCRecord: ...
    def create_warc_record(
        self,
        uri: str,
        record_type: str,
        payload: Optional[io.BytesIO] = None,
        length: Optional[int] = None,
        warc_headers_dict: Optional[Dict[str, str]] = None,
        http_headers: Optional[StatusAndHeaders] = None,
    ) -> _WARCRecord: ...
    def write_record(self, record: _WARCRecord) -> None: ...

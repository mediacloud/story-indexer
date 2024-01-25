from typing import Any, BinaryIO, Dict

class ArcWarcRecord:
    rec_headers: Dict[str, Any]
    rec_type: str
    raw_stream: BinaryIO

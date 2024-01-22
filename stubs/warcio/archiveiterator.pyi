# from https://skeptric.com/python-type-stubs/index.html

import io
from typing import BinaryIO, Generator

from warcio.recordloader import ArcWarcRecord

class ArchiveIterator:
    def __init__(self, stream: BinaryIO): ...
    def __iter__(self) -> Generator[ArcWarcRecord, None, None]: ...
    def __next__(self) -> ArcWarcRecord: ...

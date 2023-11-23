# just what's used

from typing import List, Tuple

class StatusAndHeaders:
    def __init__(
        self,
        statusline: str,
        headers: List[Tuple[str, str]],
        protocol: str = "",
        total_len: int = 0,
        is_http_request: bool = False,
    ): ...

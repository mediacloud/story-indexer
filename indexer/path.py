import os
from pathlib import Path


def DATAROOT() -> str:
    return os.getenv("DATAROOT", "data/")


def DATAPATH_BY_DATE(date: str, init_path: bool = True) -> str:
    """
    putting this in the common date path since some scripts depend on this as well
    """
    year, month, day = date.split("-")
    datapath = f"{DATAROOT()}{year}/{month}/{day}/"
    if init_path:
        Path(datapath).mkdir(parents=True, exist_ok=True)
    return datapath


STORIES = "stories"

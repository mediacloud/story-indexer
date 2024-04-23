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


def app_data_dir(app_name: str) -> str:
    """
    return path for per-application data directory
    (creating it first, if needed)
    """
    dataroot = DATAROOT()
    if not os.path.isdir(dataroot):
        dataroot = "."
    work_dir = os.path.join(dataroot, app_name)
    if not os.path.isdir(work_dir):
        os.mkdir(work_dir)
    return work_dir

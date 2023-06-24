import os


def DATAROOT() -> str:
    return os.getenv("DATAROOT", "data/")


def DATAPATH_BY_DATE(date: str) -> str:
    """
    putting this in the common date path since some scripts depend on this as well
    """
    year, month, day = date.split("-")
    return f"{DATAROOT}{year}/{month}/{day}/"

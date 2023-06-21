import os


def DATAROOT() -> str:
    return os.getenv("DATAROOT", "data/")

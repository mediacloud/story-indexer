import os


def get_env_or_default(key: str, default: str) -> str:
    if key in os.environ:
        return os.environ[key]
    else:
        return default


DATAROOT = get_env_or_default("DATAROOT", "data/")

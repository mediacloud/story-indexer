import logging
import os

# PyPI:
import sentry_sdk

logger = logging.getLogger(__name__)


def init() -> bool:
    """
    Centralized logging to Sentry.
    """

    sentry_dsn = os.environ.get("SENTRY_DSN")

    if sentry_dsn:
        sentry_sdk.init(
            dsn=sentry_dsn,
            # By default the SDK will try to use the SENTRY_RELEASE
            # environment variable, or infer a git commit
            # SHA as release, however you may want to set
            # something more human-readable.
            # release="indexer@1.0.0",
        )
        return True
    else:
        logger.info("SENTRY_DSN not found. Not logging errors to Sentry")
        return False

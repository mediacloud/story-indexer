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
    sentry_environment = os.environ.get("SENTRY_ENVIRONMENT")

    if sentry_dsn:
        sentry_sdk.init(
            dsn=sentry_dsn,
            environment=sentry_environment,
            # By default the SDK will try to use the SENTRY_RELEASE
            # environment variable, or infer a git commit
            # SHA as release, however you may want to set
            # something more human-readable.
            # release="indexer@1.0.0",
        )
        return True
    else:
        logger.warning("SENTRY_DSN not found. Not logging errors to Sentry")
        return False

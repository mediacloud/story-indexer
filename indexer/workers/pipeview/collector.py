"""
Collect breadcrumbs from Story processing and sum in a PG database
"""

import argparse
import json
import logging
import os

# PyPI
from sqlalchemy import create_engine
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import sessionmaker

# story-indexer/indexer
from indexer.app import AppException, run
from indexer.storyapp import StoryMixin
from indexer.worker import InputMessage, Worker

# local dir:
from indexer.workers.pipeview.models import CRUMB_UNIQUE_KEYS, Base, Crumb

logger = logging.getLogger(__name__)


class Collector(Worker):  # NOT a StoryWorker!
    def define_options(self, ap: argparse.ArgumentParser) -> None:
        super().define_options(ap)
        ap.add_argument(
            "--database_url",
            default=os.environ.get("DATABASE_URL"),
        )

    def process_args(self) -> None:
        super().process_args()
        assert self.args

        database_url = self.args.database_url
        if not database_url:
            raise AppException("need DATABASE_URL")

        # can pass echo (echo sql statements for debug)
        self.engine = create_engine(database_url, pool_pre_ping=True)

        # create tables (if not previously existing)
        Base.metadata.create_all(self.engine)

        # factory for Session with presupplied parameters:
        self.session_factory = sessionmaker(bind=self.engine)

    def incr_msg(self, status: str) -> None:
        self.incr("msgs", labels=[("status", status)])

    def process_message(self, im: InputMessage) -> None:
        """
        decode as newline separated JSON
        (easier to read and to recover from one bad entry)
        """

        lines = im.body.decode("utf-8").split("\n")
        if not lines:
            self.incr_msg("empty")
            return

        rows: list[dict] = []
        first = True
        version_dict = {}
        version_app = None
        version = []
        app = "unknown"
        for line in lines:
            try:
                j = json.loads(line)
                assert isinstance(j, dict)
            except Exception as e:
                self.incr("lines.bad")
                logger.error("exception parsing %s: %.50r", line, e)
                continue

            if first:
                first = False
                if "version" in j:
                    ll = len(lines)
                    version_dict = j
                    version = version_dict["version"]
                    if version[0] > StoryMixin.BREADCRUMB_VERSION[0]:
                        logger.info("%d lines(s); too new: %r", ll, version_dict)
                        self.incr_msg("too-new")
                        return
                    self.incr_msg("vers-ok")
                    version_app = version_dict.get("app")
                    logger.info("%d line(s): %r", ll, version_dict)
                    # XXX discard if "sent_at" (timestamp) too long ago?
                    continue
                else:
                    self.incr_msg("no-vers")
            # XXX handle old version crumbs?
            if "count" not in j:
                j["count"] = 1
            app = j.get("app", version_app)
            rows.append(j)

            # showing counts rcvd (not committed)
            self.incr("crumbs", labels=[("app", app)])

        with self.session_factory() as session:
            session.begin()

            # can't pass all the rows at once to .values(); (it
            # accepts a list of dicts) because more than one may have
            # same set of keys, _COULD_ coalesce them up front (via a
            # Counter indexed by a tuple (or frozendict) of the crumb
            # columns), but mindful of Knuth's warning, not doing that
            # for now:

            for row in rows:
                insert_stmt = insert(Crumb).values(row)

                # an "upsert" that increments!
                incsert_stmt = insert_stmt.on_conflict_do_update(
                    index_elements=CRUMB_UNIQUE_KEYS,
                    # get increment value from values
                    # (in case dups consolidatated)
                    set_={"count": Crumb.count + insert_stmt.excluded.count},
                )
                # not in a try (let message handling retry/quarantine)
                result = session.execute(incsert_stmt)
                result.close()
            session.commit()


if __name__ == "__main__":
    # second argument controls logging and input queue name!
    run(Collector, "pipeview", "pipeview collector")

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

        # pool_size? echo??
        self.engine = create_engine(database_url)

        # create tables (if not previously existing)
        Base.metadata.create_all(self.engine)

        # factory for Sesion with presupplied parameters:
        self.session_factory = sessionmaker(bind=self.engine)

    def process_message(self, im: InputMessage) -> None:
        """
        decode as newline separated JSON
        (easier to read and to recover from one bad entry)
        """

        lines = im.body.decode("utf-8").split("\n")
        logger.info("process_message: %d line(s)", len(lines))
        if not lines:
            # count? log??
            return

        rows: list[dict] = []
        first = True
        version = []
        app = "unknown"
        for line in lines:
            try:
                j = json.loads(line)
                assert isinstance(j, dict)
            except Exception as e:
                # XXX count??
                logger.error("exception parsing %s: %.50r", line, e)
                continue

            if first:
                first = False
                if "version" in j:
                    # also: "sent_at" (time.time), "app"
                    version = j["version"]
                    if version[0] > StoryMixin.BREADCRUMB_VERSION[0]:
                        logger.info("breadcrumbs too new: %r", version)
                        return
                    if "app" in j:
                        app = j["app"]
                    continue
            # XXX handle old version crumbs?
            # XXX discard if "date" too old!!!!
            if "count" not in j:
                j["count"] = 1
            rows.append(j)
            self.incr("crumbs", labels=[("app", app)])

        with self.session_factory() as session:
            session.begin()

            # can't pass all the rows at once to insert/increment (it
            # accepts a list of dicts) because more than one may have
            # same set of keys, _COULD_ coalesce them up front (via a
            # Counter indexed by a tuple (or frozendict) of the crumb
            # columns), but mindful of Knuth's warning, not doing
            # that for now:

            for row in rows:
                insert_stmt = insert(Crumb).values(row)

                # an "upsert" that increments!
                incsert_stmt = insert_stmt.on_conflict_do_update(
                    index_elements=CRUMB_UNIQUE_KEYS,
                    # could replace 1 with insert_stmt.excluded.count
                    # if passed list of deduped rows with count:
                    set_={"count": Crumb.count + 1},
                )
                # no need to wrap in a try??
                result = session.execute(incsert_stmt)
                result.close()
            session.commit()


if __name__ == "__main__":
    # second argument controls logging and input queue name!
    run(Collector, "pipeview", "pipeview collector")

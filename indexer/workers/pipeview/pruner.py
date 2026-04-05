"""
Collect breadcrumbs from Story processing and sum in a PG database
"""

import argparse
import logging
import os
import sys
from typing import cast

from sqlalchemy import create_engine, delete, func, select

# PyPI
from sqlalchemy.engine import CursorResult
from sqlalchemy.orm import sessionmaker

# story-indexer/indexer
from indexer.app import App, AppException, run

# local dir:
from indexer.workers.pipeview.models import Crumb

logger = logging.getLogger("pipeview-pruner")


class Pruner(App):
    def define_options(self, ap: argparse.ArgumentParser) -> None:
        super().define_options(ap)

        ap.add_argument(
            "--database_url",
            default=os.environ.get("DATABASE_URL"),
            help="SQLAlchemy URL (default from DATABASE_URL)",
        )
        days_str = os.environ.get("PIPEVIEW_DAYS")
        if days_str:
            days = int(days_str)
        else:
            days = None
        ap.add_argument(
            "--days",
            type=int,
            default=days,
            help=f"number of days to keep for each feed (default: {days})",
        )
        ap.add_argument(
            "--delete", action="store_true", help="actually delete rows (else dry run)"
        )

    def process_args(self) -> None:
        super().process_args()
        assert self.args

    def main_loop(self) -> None:
        assert self.args
        days = self.args.days
        if days is None:
            print("MUST have --days or PIPEVIEW_DAYS")
            sys.exit(1)

        database_url = self.args.database_url
        if not database_url:
            raise AppException("need DATABASE_URL")

        # pool_size? echo??
        self.engine = create_engine(database_url)

        # factory for Sesion with presupplied parameters:
        self.session_factory = sessionmaker(bind=self.engine)

        really_delete = self.args.delete
        if not really_delete:
            print("Dry run!!! use --delete to remove!")

        with self.session_factory() as session:
            # Goal: keep last "days" days of results for EACH FEED_ID.

            # get distinct date, feed_id pairings
            date_feed = select(Crumb.date, Crumb.feed_id).group_by(
                Crumb.date, Crumb.feed_id
            )

            # rank the date, feed_id pairings
            dfc = date_feed.subquery(name="date_feed").c  # subquery output columns
            ranked = select(
                dfc.date,
                dfc.feed_id,
                func.rank()
                .over(
                    partition_by=dfc.feed_id,  # rank dates within each feed
                    order_by=dfc.date.desc(),  # highest rank for oldest
                )
                .label("rank"),
            )
            rc = ranked.subquery(name="ranked").c  # ranked subquery columns

            # filter by rank (cannot be done inside the ranked query!)
            old_date_feed = select(rc.date, rc.feed_id).where(rc.rank > days)
            odfc = old_date_feed.subquery(
                name="old_date_feed"
            ).c  # columns from subquery

            if really_delete:
                result = cast(
                    CursorResult,  # subclass of Result[_T]
                    session.execute(  # returns Result[_T]
                        delete(Crumb).where(
                            Crumb.date == odfc.date, Crumb.feed_id == odfc.feed_id
                        )
                    ),
                )
                session.commit()
                logger.info("%d rows deleted", result.rowcount)
            else:
                logger.debug("SQL: %s", old_date_feed)
                old_date_feed_count = 0
                dates = set()
                feeds = set()
                df = set()  # (date,feed) tuples
                for row in session.execute(old_date_feed):
                    logger.debug("date %s feed %d", row.date, row.feed_id)
                    old_date_feed_count += 1
                    dates.add(row.date)
                    feeds.add(row.feed_id)
                    df.add((row.date, row.feed_id))
                print(
                    old_date_feed_count,
                    "feed/date pairs,",
                    len(dates),
                    "dates,",
                    len(feeds),
                    "feeds",
                )
                s = select(func.count(1)).where(
                    Crumb.date == odfc.date, Crumb.feed_id == odfc.feed_id
                )
                print(session.execute(s).one()[0], "rows")
                assert old_date_feed_count == len(df)
                # test days == len(dates)??


if __name__ == "__main__":
    # second argument controls App logging
    run(Pruner, "pipeview-pruner", "pipeview database pruner")

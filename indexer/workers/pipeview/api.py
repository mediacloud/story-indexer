"""
API server for pipeview database
"""

import argparse
import logging
import os

# PyPI:
import uvicorn
from fastapi import FastAPI, Query
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.ext.asyncio.session import async_sessionmaker

from indexer.app import App

# local directory:
from indexer.workers.pipeview.models import Crumb, CrumbKey

logger = logging.getLogger("pipeview-api")

DATABASE_URL = os.environ.get("DATABASE_URL", "")

# pool_size, echo??
async_engine = create_async_engine(DATABASE_URL, pool_pre_ping=True)

# https://docs.sqlalchemy.org/en/20/orm/extensions/asyncio.html#preventing-implicit-io-when-using-asyncsession
AsyncSession = async_sessionmaker(async_engine, expire_on_commit=False)

app = FastAPI(
    title="PipeView",
    description="Look inside story-indexer pipeline",
)


class PipeViewAPI(App):
    """
    Indexer app: initializes logging, stats with command line options
    """

    def define_options(self, ap: argparse.ArgumentParser) -> None:
        super().define_options(ap)
        ap.add_argument(
            "--api-port", type=int, default=os.environ.get("PIPEVIEW_API_PORT", 8000)
        )

    def main_loop(self) -> None:
        assert self.args
        uvicorn.run(app, host="0.0.0.0", port=self.args.api_port, log_config=None)


# instantiate early for access to stats calls (pvapp.{incr,gauge,timing})
pvapp = PipeViewAPI("pipeview-api", "PipeView API Server")


# XXX avoid having all rows in memory by creating new endpoint w/:
#        async def generator(stream):
#            async for row in stream:
#                yield json.dumps(row._asdict()) + "\n"
#        results = await session.stream(query)
#        return StreamingReponse(generator(results), media_type="application/x-ndjson")


@app.get("/sum/")
async def sum(
    col: list[CrumbKey] = Query(default=[]),
    # filters
    source_id: int | None = Query(None),
    feed_id: int | None = Query(None),
    domain: str | None = Query(None),
    app: str | None = Query(None),
    status: str | None = Query(None),
    # pagination (requires ordered results!!)
    # skip: int = Query(0),
    # limit: int = Query(1000),
) -> list[dict[str, int | str | None]]:
    columns = [getattr(Crumb, c) for c in col]
    query = select(*columns, func.sum(Crumb.count), func.count(Crumb.id))

    # apply filters
    if source_id is not None:
        query = query.where(Crumb.source_id == source_id)
    if feed_id is not None:
        query = query.where(Crumb.feed_id == feed_id)
    if domain is not None:
        # treat empty string as NULL
        query = query.where(Crumb.domain == (domain or None))
    if app is not None:
        query = query.where(Crumb.app == app)
    if status is not None:
        query = query.where(Crumb.status == status)

    query = query.group_by(*columns)
    # need order_by if paginating

    async with AsyncSession() as session:
        # row is sqlalchemy.engine.row.Row
        return [row._asdict() for row in await session.execute(query)]


if __name__ == "__main__":
    pvapp.main()

"""
SQLAlchemy table definitions

Using full ORM/Declarative declarations
"""

from typing import Literal, TypeAlias, get_args

# PyPI:
import sqlalchemy as sa
from sqlalchemy.orm import DeclarativeBase, mapped_column


# declarative base class
class Base(DeclarativeBase):
    pass


# MUST have composite unique index
# for UPDATE ... ON CONFLICT "incsert" to function properly!!

CrumbKey: TypeAlias = Literal["date", "feed_id", "source_id", "domain", "app", "status"]

# can't define Literal in terms of dynamic data, so extract list from Literal:
CRUMB_UNIQUE_KEYS = get_args(CrumbKey)


class Crumb(Base):
    """
    NOTE! column names currently match items sent in JSON breadcrumb messages!
    """

    __tablename__ = "crumb"

    id = mapped_column(sa.BigInteger, primary_key=True, autoincrement=True)

    # YYYY-MM-DD
    date = mapped_column(sa.String, nullable=False)

    feed_id = mapped_column(sa.BigInteger)

    source_id = mapped_column(sa.BigInteger)

    # null before parser:
    domain = mapped_column(sa.String)

    app = mapped_column(sa.String, nullable=False)

    status = mapped_column(sa.String, nullable=False)

    # incremented for each matching breadcrumb:
    count = mapped_column(sa.BigInteger, nullable=False)

    __table_args__ = (
        sa.Index(
            "ix_crumb_unique",
            *CRUMB_UNIQUE_KEYS,  # index columns
            unique=True,
            # necessary to treat rows with NULL keys as identical
            postgresql_nulls_not_distinct=True
        ),
    )

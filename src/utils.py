import logging
from datetime import date
from enum import Enum, auto
from pathlib import Path

import pandera.polars as pa

ALLOWED_COUNTRIES = ["Australia", "India", "USA", "UK", "Canada", "New Zealand"]
ROOT_DIR = Path(__file__).parent.parent
SQLITE_DB = ROOT_DIR / "db/pipeline.db"


def setup_logging(level=logging.INFO):
    logging.basicConfig(
        level=level, format="%(asctime)s %(levelname)s %(message)s", force=True
    )


class Source(Enum):
    sftp = auto()
    path = auto()


class DataType(Enum):
    csv = auto()
    json = auto()


class CSVOutput(pa.DataFrameModel):
    sales_person: str = pa.Field(nullable=False)
    country: str = pa.Field(nullable=False, isin=ALLOWED_COUNTRIES)
    product: str
    sale_date: date = pa.Field(ge=date(year=2022, month=1, day=1))
    amount: float
    boxes_shipped: int = pa.Field(le=1000)
    first_name: str = pa.Field(nullable=False)
    last_name: str = pa.Field(nullable=False)


class CSVInput(pa.DataFrameModel):
    sales_person: str = pa.Field(nullable=False)
    country: str = pa.Field(nullable=False, isin=ALLOWED_COUNTRIES)
    product: str
    date: str = pa.Field(str_matches=r"\d{2}-[A-Z][a-z]{2}-\d{2}")
    amount: str
    boxes_shipped: int = pa.Field(le=1000)

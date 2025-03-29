"""This module is for the pipeline. It fectches processes and validates the data"""

import logging
import re
import sqlite3
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import polars as pl
import polars.selectors as cs
from sqlalchemy import create_engine

from utils import SQLITE_DB, CSVInput, CSVOutput, DataType, Source, setup_logging

setup_logging()

logger = logging.getLogger(__name__)


@dataclass
class Pipeline:
    url: str
    source: Source
    datatype: DataType

    def fetch_data(self) -> pl.LazyFrame:
        if self.source == Source.path and self.datatype == DataType.csv:
            data = pl.scan_csv(url)

        # clean the column names
        data = data.rename(
            {
                c: re.sub(pattern=r"\s+", repl="_", string=c).lower().strip()
                for c in data.collect_schema().names()
            }
        )

        # validate input schema
        if self.datatype == DataType.csv:
            try:
                CSVInput.validate(data.collect(), lazy=True)
            except Exception as e:
                logger.exception(e)
                raise

        return data

    @staticmethod
    def process_data(data: pl.LazyFrame) -> pl.LazyFrame:

        # split first & last names
        data = data.with_columns(
            pl.col("sales_person")
            .str.split_exact(by=" ", n=2)
            .struct.rename_fields(["first_name", "last_name"])
            .alias("names")
        ).unnest("names")

        # convert date to iso format
        data = data.with_columns(
            pl.col("date").str.to_date(format="%d-%b-%y").alias("sale_date")
        ).drop(["date"])

        # convert price to float
        data = data.with_columns(
            pl.col("amount").str.replace_all(pattern=r"\D", value="").cast(pl.Float64())
        )

        return data

    @staticmethod
    def validate_data(data: pl.LazyFrame) -> tuple[bool, dict]:
        # validate schema
        try:
            CSVOutput.validate(data.collect(), lazy=True)
            logger.info("data validation was successful")
            validation_result = True
        except Exception as e:
            logger.exception(e)
            validation_result = False

        ## dq metrics
        # summary stats on categorical cols
        cat_cols = data.select(pl.col(pl.String())).collect_schema().names()
        logger.info(f"categorical columns: {cat_cols}")

        categorical_stats = {}
        for c in cat_cols:
            summary = data.select(pl.col(c).value_counts(sort=True))
            categorical_stats[c] = summary.collect()

        # summary stats on numeric data
        numeric_cols = data.select(cs.numeric()).collect_schema().names()
        logger.info(f"numeric columns: {numeric_cols}")

        numeric_stats = {}
        for c in numeric_cols:
            summary = data.select(
                pl.col(c).max().alias(f"{c}_max"),
                pl.col(c).min().alias(f"{c}_min"),
            )

            numeric_stats[c] = summary.collect()

        # null count
        null_count = (
            data.null_count()
            .collect()
            .transpose(include_header=True, column_names=["null_count"])
        )

        return (
            validation_result,
            {
                "categorical_stats": categorical_stats,
                "numeric_stats": numeric_stats,
                "null_count": null_count,
            },
        )

    def save_data(self, data: pl.LazyFrame):
        tz = ZoneInfo("UTC")
        ts = datetime.now(tz)
        file_name = Path(self.url).stem

        data = data.with_columns(
            pl.lit(value=file_name).alias("file_name"),
            pl.lit(value=str(self.url)).alias("source"),
            pl.lit(value=ts).alias("created_at"),
            pl.lit(value=ts).alias("modified_at"),
        )

        if not SQLITE_DB.exists():
            SQLITE_DB.parent.mkdir(parents=True, exist_ok=True)

            with sqlite3.connect(SQLITE_DB) as conn:
                logger.info(f"database file created at {SQLITE_DB}")

        logger.info(f"saving info to {SQLITE_DB}...")
        engine = create_engine(f"sqlite:///{SQLITE_DB}")

        data.collect().write_database(
            table_name="processed_data", connection=engine, if_table_exists="append"
        )


if __name__ == "__main__":

    url = Path("../data/Chocolate Sales.csv").resolve()

    pipeline = Pipeline(url=url, source=Source.path, datatype=DataType.csv)

    data = pipeline.fetch_data()
    logger.info("input data")
    logger.info(data.collect())

    proc_data = pipeline.process_data(data=data)
    logger.info("processed data")
    logger.info(proc_data.collect())

    dq_result, _ = pipeline.validate_data(data=proc_data)
    logger.info(f"{dq_result=}")

    if dq_result:
        pipeline.save_data(proc_data)

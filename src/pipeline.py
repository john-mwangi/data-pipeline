"""This module is for the pipeline. It fectches processes and validates the data"""

import logging
import re
from dataclasses import dataclass

import polars as pl
import polars.selectors as cs

from utils import CSVInput, CSVOutput, DataType, Source, setup_logging

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
        )

        # convert price to float
        data = data.with_columns(
            pl.col("amount").str.replace_all(pattern=r"\D", value="").cast(pl.Float64())
        )

        return data

    @staticmethod
    def validate_data(data: pl.LazyFrame) -> dict:
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

        # validate schema
        try:
            CSVOutput.validate(data.collect(), lazy=True)
            logger.info("schema validation was successful")
        except Exception as e:
            logger.exception(e)

        # dq metrics
        null_count = (
            data.null_count()
            .collect()
            .transpose(include_header=True, column_names=["null_count"])
        )

        return {
            "categorical_stats": categorical_stats,
            "numeric_stats": numeric_stats,
            "null_count": null_count,
        }

    def save_data(data: pl.LazyFrame):
        pass


if __name__ == "__main__":
    from pathlib import Path

    url = Path("../data/Chocolate Sales.csv").resolve()

    pipeline = Pipeline(url=url, source=Source.path, datatype=DataType.csv)

    data = pipeline.fetch_data()
    logger.info("input data")
    logger.info(data.collect())

    proc_data = pipeline.process_data(data=data)
    logger.info("processed data")
    logger.info(proc_data.collect())

    dq_results = pipeline.validate_data(data=proc_data)
    logger.info("dq results")
    logger.info(dq_results)

"""This module is for the pipeline. It fectches processes and validates the data"""

import json
import logging
import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from uuid import uuid4
from zoneinfo import ZoneInfo

import pandas as pd
import polars as pl
import polars.selectors as cs
import requests
import yaml
from pandera.errors import SchemaErrors
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError

from data_pipeline.src.utils import (
    SQLITE_DB,
    DataInput,
    DataOutput,
    FileType,
    config_path,
    setup_logging,
)

setup_logging()
logger = logging.getLogger(__name__)


@dataclass
class Pipeline:
    url: str

    def fetch_data(self) -> pl.LazyFrame:
        tz = ZoneInfo("UTC")
        start_time = datetime.now(tz)
        self.start_time = start_time
        self.run_id = uuid4().hex

        print("\n")
        logger.info(f"reading data from {self.url}")

        file_type = self.url.split(".")[-1]
        if self.url.__contains__("token="):
            file_type = self.url.split("?")[0].split(".")[-1]

        if not file_type in FileType._member_names_:
            msg = f"data type not supported. supported data types are {FileType._member_names_}"
            logger.error(msg)
            raise ValueError(msg)

        self.file_type = file_type

        if file_type == FileType.csv.name:
            data = pl.scan_csv(self.url)

        if file_type == FileType.json.name:
            df = pd.read_json(
                self.url, convert_axes=False, convert_dates=False
            )
            data = pl.from_pandas(df).lazy()

        # clean the column names
        data = data.rename(
            {
                c: re.sub(pattern=r"\s+", repl="_", string=c).lower().strip()
                for c in data.collect_schema().names()
            }
        )

        # validate input schema
        try:
            DataInput.validate(data.collect(), lazy=True)
            self.file_name = Path(self.url).stem
        except SchemaErrors as e:
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
            pl.col("amount")
            .str.replace_all(pattern=r"\D", value="")
            .cast(pl.Float64())
        )

        return data

    @staticmethod
    def validate_data(data: pl.LazyFrame) -> tuple[bool, pl.LazyFrame]:
        # validate output schema
        validation_result = False

        try:
            DataOutput.validate(data.collect(), lazy=True)
            logger.info("data validation was successful")
            validation_result = True
        except SchemaErrors as e:
            logger.exception(e)

        ## dq metrics
        # summary stats on categorical cols
        cat_cols = data.select(pl.col(pl.String())).collect_schema().names()
        logger.info(f"categorical columns: {cat_cols}")

        categorical_stats = {}
        for c in cat_cols:
            summary = data.select(pl.col(c).value_counts(sort=True))
            categorical_stats[c] = summary.collect().to_dicts()

        # summary stats on numeric data
        numeric_cols = data.select(cs.numeric()).collect_schema().names()
        logger.info(f"numeric columns: {numeric_cols}")

        numeric_stats = {}
        for c in numeric_cols:
            summary = data.select(
                pl.col(c).max().alias(f"{c}_max"),
                pl.col(c).min().alias(f"{c}_min"),
            )

            numeric_stats[c] = summary.collect().to_dicts()

        # null count
        null_count = (
            data.null_count()
            .collect()
            .transpose(include_header=True, column_names=["null_count"])
        ).to_dicts()

        dq_metrics = {
            "categorical_stats": json.dumps(categorical_stats),
            "numeric_stats": json.dumps(numeric_stats),
            "null_count": json.dumps(null_count),
        }

        dq_results = pl.LazyFrame(dq_metrics)

        return validation_result, dq_results

    def save_data(self, data: pl.LazyFrame, table_name: str):
        if not SQLITE_DB.exists():
            SQLITE_DB.parent.mkdir(parents=True, exist_ok=True)
            engine = create_engine(f"sqlite:///{SQLITE_DB}")
            logger.info(f"database file created at {SQLITE_DB}")
        else:
            engine = create_engine(f"sqlite:///{SQLITE_DB}")

        try:
            id_query = f"SELECT COUNT(*) as last_id FROM {table_name};"
            with engine.connect() as conn:
                last_id = conn.execute(text(id_query)).fetchone()[0]
        except Exception as e:
            last_id = 0

        data = data.with_columns(
            pl.lit(value=self.run_id).alias("run_id"),
            pl.lit(value=self.file_name).alias("file_name"),
            pl.lit(value=self.file_type).alias("file_type"),
            pl.lit(value=self.url).alias("source"),
            pl.lit(value=self.start_time).alias("created_at"),
            pl.lit(value=self.start_time).alias("modified_at"),
        ).with_row_index(name="id", offset=last_id + 1)

        logger.info(f"saving info to {table_name} table...")
        try:
            data.collect().write_database(
                table_name=table_name,
                connection=engine,
                if_table_exists="append",
            )

        except OperationalError as e:
            logger.exception(e)


def main(url: str = None, use_local: bool = False, file_path: Path = None):
    with open(config_path, mode="r") as f:
        config = yaml.safe_load(f)

    table_name = config["pipeline"]["destination_table"]
    dq_table = config["pipeline"]["data_quality"]

    if url is not None:
        file_exists = requests.head(url).status_code == requests.codes.ok

    if use_local:
        url = str(file_path)
        file_exists = file_path.exists()

    if file_exists == False:
        msg = f"the file does not exist in the specified {url}"
        logger.error(msg)
        raise ValueError(msg)

    pipeline = Pipeline(url=url)

    data = pipeline.fetch_data()
    logger.info("input data")
    logger.info(data.collect())

    proc_data = pipeline.process_data(data=data)
    logger.info("processed data")
    logger.info(proc_data.collect())

    dq_result, dq_metrics = pipeline.validate_data(data=proc_data)
    logger.info(f"{dq_result=}")

    if dq_result:
        pipeline.save_data(data=proc_data, table_name=table_name)
        pipeline.save_data(data=dq_metrics, table_name=dq_table)

    logger.info(f"pipeline with id {pipeline.run_id} completed")

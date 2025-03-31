from pathlib import Path

import polars as pl
import pytest
import yaml

from data_pipeline.src.pipeline import Pipeline, main
from data_pipeline.src.utils import ROOT_DIR, DataOutput


@pytest.fixture
def local_files():
    data_dir = ROOT_DIR / "data"
    return {
        "csv": str(data_dir / "Chocolate Sales.csv"),
        "json": str(data_dir / "Chocolate Sales.json"),
    }


@pytest.fixture
def remote_urls():
    with open(ROOT_DIR / "src/config.yaml") as f:
        config = yaml.safe_load(f)
    return config["pipeline"]["urls"]


def test_pipeline_with_local_files(local_files):
    for file_type, path in local_files.items():
        pipeline = Pipeline(path)
        data = pipeline.fetch_data()
        assert isinstance(data, pl.LazyFrame)
        assert data.collect().height > 0


def test_pipeline_with_remote_files(remote_urls):
    for name, url in remote_urls.items():
        pipeline = Pipeline(url)
        data = pipeline.fetch_data()
        assert isinstance(data, pl.LazyFrame)
        assert data.collect().height > 0


def test_pipeline_process_data(local_files):
    pipeline = Pipeline(local_files["csv"])
    raw_data = pipeline.fetch_data()
    processed_data = pipeline.process_data(raw_data)
    DataOutput.validate(processed_data.collect(), lazy=True)


def test_main(local_files):
    main(file_path=Path(local_files["csv"]), use_local=True)

from pathlib import Path

import polars as pl
import pytest
import yaml

from data_pipeline.src.pipeline import Pipeline, main
from data_pipeline.src.utils import ROOT_DIR, DataOutput, FileType


@pytest.fixture
def local_files():
    data_dir = ROOT_DIR / "data"
    paths = list(data_dir.glob("*"))
    file_types = [p.suffix.replace(".", "") for p in paths]
    return {k: str(v) for k, v in zip(file_types, paths)}


@pytest.fixture
def remote_urls():
    with open(ROOT_DIR / "src/config.yaml") as f:
        config = yaml.safe_load(f)
    return config["pipeline"]["urls"]


def test_pipeline_with_local_files(local_files):
    for file_type, path in local_files.items():
        if file_type in FileType._member_names_:
            pipeline = Pipeline(path)
            data = pipeline.fetch_data()
            assert isinstance(data, pl.LazyFrame)
            assert data.collect().height > 0
        else:
            with pytest.raises(ValueError):
                pipeline = Pipeline(path)
                data = pipeline.fetch_data()


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
    filtered = {k: v for k, v in local_files.items() if k in FileType._member_names_}
    for _, v in filtered.items():
        main(file_path=Path(v), use_local=True)

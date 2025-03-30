"""This module is for the API. It serves the API endpoints"""

import logging
from typing import Optional

import fastapi
import yaml
from fastapi import HTTPException, status
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from sqlalchemy import create_engine, text

from utils import ROOT_DIR, SQLITE_DB, responses, setup_logging

setup_logging()

logger = logging.getLogger(__name__)

service_desc = "API for querying the data pipeline"
app = fastapi.FastAPI(description=service_desc)


with open(ROOT_DIR / "src/config.yaml", mode="r") as f:
    config = yaml.safe_load(f)

table_name = config["pipeline"]["destination_table"]


class DataRequest(BaseModel):
    source: str = Field(default=table_name, description="Source database table")
    start_index: int = Field(ge=0, description="Starting index for pagination")
    end_index: int = Field(ge=0, description="Ending index for pagination")
    limit: Optional[int] = Field(
        default=5, le=1000, description="Number of records to fetch"
    )
    start_date: str = Field(default=None, description="Start of the date range")
    end_date: str = Field(default=None, description="End of the date range")


@app.post("/get_data")
def get_data(request: DataRequest):
    """Queries the database table"""

    engine = create_engine(f"sqlite:///{SQLITE_DB}")
    query = f"SELECT * FROM {request.source} LIMIT {request.limit}"

    try:
        logger.info("fetching records from the database...")
        with engine.connect() as conn:
            res = conn.execute(text(query)).fetchall()

        if len(res) == 0:
            logger.warning("no records returned")

        data = [dict(row._mapping) for row in res]
        payload = {"data": data, **responses.get("SUCCESS")}
        status_code = status.HTTP_200_OK

    except HTTPException as e:
        logger.exception(e)
        payload = {"data": None, **responses.get("ERROR")}
        status_code = status.HTTP_400_BAD_REQUEST

    return JSONResponse(content=payload, status_code=status_code)

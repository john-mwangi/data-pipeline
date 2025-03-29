"""This module is for the API. It serves the API endpoints"""

import logging
from typing import Optional

import fastapi
from fastapi import status
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from sqlalchemy import create_engine, text

from utils import SQLITE_DB, responses, setup_logging

setup_logging()

logger = logging.getLogger(__name__)

service_desc = "API for querying the data pipeline"
app = fastapi.FastAPI(description=service_desc)


class DataRequest(BaseModel):
    source: str = Field(default="processed_data", description="Source database table")
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

    except Exception as e:
        logger.exception(e)
        payload = {"data": None, **responses.get("ERROR")}
        status_code = status.HTTP_400_BAD_REQUEST

    return JSONResponse(content=payload, status_code=status_code)

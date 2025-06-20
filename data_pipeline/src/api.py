"""This module is for the API. It serves the API endpoints"""

import logging
import secrets
from datetime import date, timedelta
from typing import Annotated, Optional

import fastapi
import yaml
from fastapi import APIRouter, Depends, HTTPException, Request, status
from fastapi.responses import JSONResponse
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from pydantic import BaseModel, Field
from slowapi import Limiter
from slowapi.util import get_remote_address
from sqlalchemy import create_engine, text

from data_pipeline.src.utils import (
    SQLITE_DB,
    config_path,
    responses,
    setup_logging,
)

setup_logging()
logger = logging.getLogger(__name__)

with open(config_path, mode="r") as f:
    config = yaml.safe_load(f)

table_name = config["pipeline"]["destination_table"]
users = config["api"]["admin"]
rate_per_minute = config["api"]["rate_limits"]["per_minute"]
rate_per_second = config["api"]["rate_limits"]["per_second"]
start_time = str(date.today()) + " 00:00:00"
end_time = str(date.today() + timedelta(days=1)) + " 00:00:00"
api_version = config["api"]["version"]
service_desc = "API for querying the data pipeline"

app = fastapi.FastAPI(description=service_desc)
security = HTTPBasic()
limiter = Limiter(
    key_func=get_remote_address,
    strategy="fixed-window",
    storage_uri="memory://",
)

v1_router = APIRouter(prefix=api_version)


def get_current_username(
    credentials: Annotated[HTTPBasicCredentials, Depends(security)],
):
    current_username_bytes = credentials.username.encode("utf8")
    correct_username_bytes = users.get("username").encode("utf8")
    is_correct_username = secrets.compare_digest(
        current_username_bytes, correct_username_bytes
    )
    current_password_bytes = credentials.password.encode("utf8")
    correct_password_bytes = users.get("password").encode("utf8")
    is_correct_password = secrets.compare_digest(
        current_password_bytes, correct_password_bytes
    )
    if not (is_correct_username and is_correct_password):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Basic"},
        )
    return credentials.username


class RequestParams(BaseModel):
    source_table: str = Field(
        default=table_name, description="Source database table"
    )
    limit: Optional[int] = Field(
        default=5, le=1000, description="Number of records to fetch"
    )
    start_date: str = Field(
        default=start_time, description="Start of the date range"
    )
    end_date: str = Field(
        default=end_time, description="End of the date range"
    )
    cursor: Optional[int] = Field(
        default=None, description="The last seen record id"
    )


@v1_router.post("/get_data", dependencies=[Depends(get_current_username)])
@limiter.limit(f"{rate_per_second}/second", per_method=True)
@limiter.limit(f"{rate_per_minute}/minute", per_method=True)
def get_data(params: RequestParams, request: Request):
    """Queries the database table"""

    engine = create_engine(f"sqlite:///{SQLITE_DB}")
    base_query = f"SELECT * FROM {params.source_table}"

    filters = []
    if params.start_date:
        filters.append(f"created_at >= '{params.start_date}'")
    if params.end_date:
        filters.append(f"created_at <= '{params.end_date}'")
    if params.cursor:
        filters.append(f"id > {params.cursor}")

    where_clause = f" WHERE {' AND '.join(filters)}" if filters else ""
    query = f"{base_query}{where_clause} ORDER BY id, created_at LIMIT {params.limit};"

    logger.info(f"executing query: {query}")

    try:
        logger.info("fetching records from the database...")
        with engine.connect() as conn:
            res = conn.execute(text(query)).fetchall()

        if len(res) == 0:
            logger.warning("no records returned")

        data = [dict(row._mapping) for row in res]
        next_cursor = data[-1]["id"] if data else None
        payload = {
            "data": data,
            "next_cursor": next_cursor,
            **responses.get("SUCCESS"),
        }
        status_code = status.HTTP_200_OK

    except HTTPException as e:
        logger.exception(e)
        payload = {"data": None, **responses.get("ERROR")}
        status_code = status.HTTP_400_BAD_REQUEST

    return JSONResponse(content=payload, status_code=status_code)


@v1_router.get("/users/me")
def read_current_user(username: Annotated[str, Depends(get_current_username)]):
    return {"username": username}


app.include_router(v1_router)

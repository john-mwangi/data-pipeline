"""This module is for the API. It serves the API endpoints"""

import logging
import secrets
from typing import Annotated, Optional

import fastapi
import yaml
from fastapi import Depends, HTTPException, status
from fastapi.responses import JSONResponse
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from pydantic import BaseModel, Field
from sqlalchemy import create_engine, text

from .utils import ROOT_DIR, SQLITE_DB, responses, setup_logging

setup_logging()

logger = logging.getLogger(__name__)

service_desc = "API for querying the data pipeline"
app = fastapi.FastAPI(description=service_desc)
security = HTTPBasic()

with open(ROOT_DIR / "src/config.yaml", mode="r") as f:
    config = yaml.safe_load(f)

table_name = config["pipeline"]["destination_table"]
users = config["api"]["admin"]


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


class DataRequest(BaseModel):
    source_table: str = Field(default=table_name, description="Source database table")
    start_index: int = Field(ge=0, description="Starting index for pagination")
    end_index: int = Field(ge=0, description="Ending index for pagination")
    limit: Optional[int] = Field(
        default=5, le=1000, description="Number of records to fetch"
    )
    start_date: str = Field(default=None, description="Start of the date range")
    end_date: str = Field(default=None, description="End of the date range")


@app.post("/get_data", dependencies=[Depends(get_current_username)])
def get_data(request: DataRequest):
    """Queries the database table"""

    engine = create_engine(f"sqlite:///{SQLITE_DB}")
    query = f"SELECT * FROM {request.source_table} LIMIT {request.limit}"

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


@app.get("/users/me")
def read_current_user(username: Annotated[str, Depends(get_current_username)]):
    return {"username": username}

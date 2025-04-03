import base64
import time

import pytest
import yaml
from fastapi import status
from fastapi.testclient import TestClient

from data_pipeline.src.api import app
from data_pipeline.src.utils import ROOT_DIR


@pytest.fixture
def client():
    return TestClient(app)


@pytest.fixture
def auth_header():
    with open(ROOT_DIR / "src/config.yaml") as f:
        config = yaml.safe_load(f)
    username = config["api"]["admin"]["username"]
    password = config["api"]["admin"]["password"]
    credentials = f"{username}:{password}"
    encoded = base64.b64encode(credentials.encode()).decode()
    return {"Authorization": f"Basic {encoded}"}


def test_api_authentication(client):
    response = client.post("/get_data")
    assert response.status_code == status.HTTP_401_UNAUTHORIZED


def test_get_data_endpoint(client, auth_header):
    response = client.post("/get_data", headers=auth_header, json={"limit": 5})
    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert "data" in data
    assert len(data["data"]) == 5
    assert "next_cursor" in data


@pytest.fixture
def rate_limits():
    with open(ROOT_DIR / "src/config.yaml") as f:
        config = yaml.safe_load(f)
    return {
        "per_second": config["api"]["rate_limits"]["per_second"],
        "per_minute": config["api"]["rate_limits"]["per_minute"],
    }


def test_pagination(client, auth_header, rate_limits):
    time.sleep(rate_limits["per_second"] + 0.1)

    response1 = client.post("/get_data", headers=auth_header, json={"limit": 1})
    assert response1.status_code == 200
    cursor = response1.json()["next_cursor"]

    time.sleep(rate_limits["per_second"] + 0.1)

    response2 = client.post(
        "/get_data", headers=auth_header, json={"limit": 1, "cursor": cursor}
    )
    assert response2.status_code == status.HTTP_200_OK
    assert response2.json()["data"][0]["id"] == cursor + 1


def test_rate_limiting(client, auth_header, rate_limits):
    time.sleep(rate_limits["per_second"] + 0.1)

    responses = [
        client.post("/get_data", headers=auth_header, json={"limit": 1})
        for _ in range(2)
    ]

    assert responses[0].status_code == status.HTTP_200_OK
    assert responses[-1].status_code == status.HTTP_429_TOO_MANY_REQUESTS

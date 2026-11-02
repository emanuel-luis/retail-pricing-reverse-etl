"""Fixtures for tests against the compose ClickHouse and RustFS (marker `clickhouse`).

Start them first: `docker compose up -d --wait clickhouse s3` with a `.env` copied from
`.env.example`, then `uv run retail-pricing data all --sf 0.01`. Each module marks itself with
`pytestmark = pytest.mark.clickhouse`. The tests truncate the raw tables, and the session ends
with the SF0.01 seed loaded as cycle 1, which is the state `make ch-load SF=0.01 CYCLE=1` leaves.
"""

from collections.abc import Iterator
from pathlib import Path

import pytest
from ch_helpers import reset_raw

from retail_pricing.clickhouse.client import Client, connect
from retail_pricing.settings import ClickHouseSettings, DataSettings, S3Settings


@pytest.fixture(scope="session")
def ch_settings() -> ClickHouseSettings:
    return ClickHouseSettings()


@pytest.fixture(scope="session")
def s3_settings() -> S3Settings:
    return S3Settings()


@pytest.fixture(scope="session")
def data_dir() -> Path:
    path = DataSettings().dir
    assert (path / "oltp" / "sf=0.01" / "manifest.json").exists(), (
        "run `uv run retail-pricing data all --sf 0.01` first"
    )
    return path


@pytest.fixture(scope="session", autouse=True)
def _leave_cycle_one(ch_settings: ClickHouseSettings, data_dir: Path) -> Iterator[None]:
    yield
    reset_raw(ch_settings, data_dir, 1)


@pytest.fixture
def loaded(ch_settings: ClickHouseSettings, data_dir: Path) -> None:
    """Raw tables truncated, then the seed loaded as cycle 1."""
    reset_raw(ch_settings, data_dir, 1)


@pytest.fixture
def admin(ch_settings: ClickHouseSettings) -> Iterator[Client]:
    with connect(ch_settings) as ch:
        yield ch

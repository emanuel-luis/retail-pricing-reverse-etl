"""Fixtures for tests against the compose Postgres services (marker `sql`).

Start them first: `docker compose up -d --wait oltp control-db` with a `.env` copied from
`.env.example`. Each test module marks itself with `pytestmark = pytest.mark.sql`.
"""

from collections.abc import Iterator
from typing import Any

import psycopg
import pytest

from retail_pricing.control.init import init
from retail_pricing.settings import ControlSettings, OltpSettings

# Control tests run in their own database on the control Postgres, so a running demo's audit
# data in `control` is never touched.
CONTROL_TEST_DB = "control_test"


@pytest.fixture(scope="session")
def oltp_settings() -> OltpSettings:
    return OltpSettings()


@pytest.fixture(scope="session")
def control_settings() -> ControlSettings:
    return ControlSettings()


@pytest.fixture
def oltp_owner(oltp_settings: OltpSettings) -> Iterator[psycopg.Connection]:
    """An autocommit connection as the OLTP owner."""
    s = oltp_settings
    with psycopg.connect(s.conninfo(s.owner_user, s.owner_password), autocommit=True) as conn:
        yield conn


@pytest.fixture
def control_owner(control_settings: ControlSettings) -> Iterator[psycopg.Connection]:
    """An autocommit connection as the control DB owner."""
    s = control_settings
    with psycopg.connect(s.conninfo(s.owner_user, s.owner_password), autocommit=True) as conn:
        yield conn


@pytest.fixture(scope="session")
def ctl_settings(control_settings: ControlSettings) -> ControlSettings:
    s = control_settings
    with psycopg.connect(s.conninfo(s.owner_user, s.owner_password), autocommit=True) as conn:
        if not conn.execute(
            "SELECT 1 FROM pg_database WHERE datname = %s", (CONTROL_TEST_DB,)
        ).fetchone():
            conn.execute(f"CREATE DATABASE {CONTROL_TEST_DB}")
    test = s.model_copy(update={"db": CONTROL_TEST_DB})
    with psycopg.connect(test.conninfo(s.owner_user, s.owner_password), autocommit=True) as conn:
        conn.execute("DROP SCHEMA IF EXISTS audit CASCADE; DROP SCHEMA IF EXISTS registry CASCADE")
    init(test)
    return test


@pytest.fixture
def db(ctl_settings: ControlSettings) -> Iterator[psycopg.Connection[Any]]:
    """An autocommit owner connection on a freshly reset database."""
    s = ctl_settings
    with psycopg.connect(s.conninfo(s.owner_user, s.owner_password), autocommit=True) as conn:
        conn.execute(
            "TRUNCATE audit.cycle, audit.alert RESTART IDENTITY CASCADE;"
            "DELETE FROM registry.watermark;"
            "UPDATE registry.job SET enabled = true,"
            " schedule_interval = CASE WHEN job_id = 'cycle' THEN interval '10 minutes' END;"
            "UPDATE registry.extract_table SET enabled = true, page_rows = 250000"
        )
        yield conn

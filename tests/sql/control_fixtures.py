"""Shared fixtures of the control tests. They run in their own database, `control_test`, on the
compose control Postgres, so a running demo's audit data in `control` is never touched."""

from collections.abc import Iterator
from typing import Any

import psycopg
import pytest

from retail_pricing.control.init import init
from retail_pricing.settings import ControlSettings

TEST_DB = "control_test"


@pytest.fixture(scope="session")
def ctl_settings(control_settings: ControlSettings) -> ControlSettings:
    s = control_settings
    with psycopg.connect(s.conninfo(s.owner_user, s.owner_password), autocommit=True) as conn:
        if not conn.execute("SELECT 1 FROM pg_database WHERE datname = %s", (TEST_DB,)).fetchone():
            conn.execute(f"CREATE DATABASE {TEST_DB}")
    test = s.model_copy(update={"db": TEST_DB})
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


def one(conn: psycopg.Connection[Any], query: str, *args: object) -> tuple[Any, ...]:
    row = conn.execute(query, args).fetchone()  # type: ignore[call-overload]
    assert row is not None
    return row

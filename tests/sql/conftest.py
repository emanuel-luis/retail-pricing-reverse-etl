"""Fixtures for tests against the compose Postgres services (marker `sql`).

Start them first: `docker compose up -d --wait oltp control-db` with a `.env` copied from
`.env.example`. Each test module marks itself with `pytestmark = pytest.mark.sql`.
"""

from collections.abc import Iterator

import psycopg
import pytest

from retail_pricing.settings import ControlSettings, OltpSettings


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

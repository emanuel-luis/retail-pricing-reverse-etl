"""Helpers of the control tests (the fixtures live in conftest.py)."""

from typing import Any

import psycopg


def one(conn: psycopg.Connection[Any], query: str, *args: object) -> tuple[Any, ...]:
    row = conn.execute(query, args).fetchone()  # type: ignore[call-overload]
    assert row is not None
    return row

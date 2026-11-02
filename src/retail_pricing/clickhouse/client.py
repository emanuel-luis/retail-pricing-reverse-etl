"""A tiny ClickHouse HTTP client on httpx. Errors carry ClickHouse's message, never a password."""

import json
import logging
from typing import Any, Self

import httpx
from pydantic import SecretStr

from retail_pricing.settings import ClickHouseSettings

logging.getLogger("httpx").setLevel(logging.WARNING)  # its INFO lines echo every request


def _text(v: Any) -> str | None:
    if v is None:
        return None
    return str(v).lower() if isinstance(v, bool) else str(v)


class ClickHouseError(RuntimeError):
    pass


class Client:
    def __init__(self, base_url: str, user: str, password: SecretStr, timeout: float = 600.0):
        # Basic auth goes in a header, never in the URL, so no exception text can echo it.
        self._http = httpx.Client(
            base_url=base_url,
            auth=httpx.BasicAuth(user, password.get_secret_value()),
            timeout=timeout,
        )

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *exc: object) -> None:
        self._http.close()

    def command(self, sql: str, **settings: Any) -> str:
        """Run one statement and return the response body (`wait_end_of_query=1`)."""
        params = {
            "wait_end_of_query": 1,
            "default_format": "JSONCompact",
            "output_format_json_quote_decimals": 1,  # exact decimals as text
            **settings,
        }
        try:
            resp = self._http.post("/", content=sql.encode("utf-8"), params=params)
        except httpx.HTTPError as e:
            raise ClickHouseError(f"{type(e).__name__} talking to ClickHouse: {e}") from e
        if resp.status_code != 200:
            raise ClickHouseError(f"HTTP {resp.status_code}: {resp.text.strip()[:2000]}")
        return resp.text

    def rows(self, sql: str, **settings: Any) -> list[list[str | None]]:
        """Run a SELECT; every value comes back as text (NULL as None)."""
        body = self.command(sql, **settings)
        data = json.loads(body)["data"] if body.strip() else []
        # JSONCompactStrings would print a NULL LowCardinality(Nullable) as 'NULL' in small caps.
        return [[_text(v) for v in row] for row in data]

    def scalar(self, sql: str, **settings: Any) -> str | None:
        return self.rows(sql, **settings)[0][0]


def connect(settings: ClickHouseSettings, user: str | None = None) -> Client:
    """A client for `user` (default: the admin); its password comes from the settings."""
    name = user or settings.user
    return Client(settings.url, name, settings.password_of(name))

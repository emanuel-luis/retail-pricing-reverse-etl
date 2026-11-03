"""NiFi REST client on httpx: single-user token login, retries while NiFi boots.

The password and the token never reach a log line or an exception message: the token lives only
in the Authorization header, and errors carry the status, the path and NiFi's message.
"""

import logging
import time
import uuid
from collections.abc import Callable
from typing import Any, Self
from urllib.parse import urlsplit

import httpx
from pydantic import SecretStr

from retail_pricing.settings import NifiSettings

logging.getLogger("httpx").setLevel(logging.WARNING)  # its INFO lines echo every request
log = logging.getLogger(__name__)

# NiFi's certificate is self-signed, so verification is off only for a NiFi on this machine.
_LOCAL_HOSTS = {"127.0.0.1", "localhost", "::1"}
# Status codes NiFi answers while it is still starting (or the port is not open yet).
_BOOTING = {409, 502, 503, 504}


class NifiError(RuntimeError):
    pass


def verify_tls(base_url: str) -> bool:
    """False only for an https URL on the loopback interface (the local self-signed NiFi)."""
    return urlsplit(base_url).hostname not in _LOCAL_HOSTS


class Client:
    def __init__(
        self,
        base_url: str,
        username: str,
        password: SecretStr,
        timeout: float = 60.0,
        transport: httpx.BaseTransport | None = None,
    ):
        self._username = username
        self._password = password
        self.client_id = str(uuid.uuid4())
        self._http = httpx.Client(
            base_url=base_url.rstrip("/"),
            verify=verify_tls(base_url),
            timeout=timeout,
            transport=transport,
        )

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *exc: object) -> None:
        self._http.close()

    # -- auth ----------------------------------------------------------------------------------

    def fetch_token(self, wait_s: float = 300.0, poll_s: float = 5.0) -> str:
        """POST /access/token until NiFi answers 201; bad credentials fail at once."""
        deadline = time.monotonic() + wait_s
        while True:
            try:
                resp = self._http.post(
                    "/access/token",
                    data={
                        "username": self._username,
                        "password": self._password.get_secret_value(),
                    },
                )
            except httpx.TransportError as e:
                reason = type(e).__name__
            else:
                if resp.status_code in (200, 201):
                    return resp.text.strip()
                if resp.status_code not in _BOOTING:
                    # The body is NiFi's message; it never contains the submitted password.
                    raise NifiError(
                        f"login as {self._username} refused: HTTP {resp.status_code} "
                        f"{resp.text.strip()[:300]}"
                    )
                reason = f"HTTP {resp.status_code}"
            if time.monotonic() >= deadline:
                raise NifiError(f"NiFi not ready after {wait_s:.0f} s ({reason})")
            log.info("waiting for NiFi", extra={"reason": reason})
            time.sleep(poll_s)

    def login(self, wait_s: float = 300.0, poll_s: float = 5.0) -> None:
        token = self.fetch_token(wait_s, poll_s)
        self._http.headers["Authorization"] = f"Bearer {token}"

    # -- requests ------------------------------------------------------------------------------

    def request(self, method: str, path: str, **kw: Any) -> Any:
        """One API call; returns the JSON body (None when empty). Raises NifiError otherwise."""
        try:
            resp = self._http.request(method, path, **kw)
        except httpx.HTTPError as e:
            raise NifiError(f"{type(e).__name__} on {method} {path}") from e
        if resp.status_code >= 400:
            raise NifiError(f"{method} {path}: HTTP {resp.status_code} {resp.text.strip()[:2000]}")
        if not resp.content:
            return None
        if resp.headers.get("content-type", "").startswith("application/json"):
            return resp.json()
        return resp.text

    def get(self, path: str, **kw: Any) -> Any:
        return self.request("GET", path, **kw)

    def post(self, path: str, **kw: Any) -> Any:
        return self.request("POST", path, **kw)

    def put(self, path: str, **kw: Any) -> Any:
        return self.request("PUT", path, **kw)

    def delete(self, path: str, **kw: Any) -> Any:
        return self.request("DELETE", path, **kw)

    def poll(
        self,
        fetch: Callable[[], Any],
        done: Callable[[Any], bool],
        what: str,
        timeout_s: float = 120.0,
        every_s: float = 1.0,
    ) -> Any:
        """Call `fetch` until `done(result)`; raise NifiError after `timeout_s`."""
        deadline = time.monotonic() + timeout_s
        while True:
            result = fetch()
            if done(result):
                return result
            if time.monotonic() >= deadline:
                raise NifiError(f"timed out after {timeout_s:.0f} s waiting for {what}")
            time.sleep(every_s)


def connect(settings: NifiSettings, timeout: float = 60.0) -> Client:
    """A client for the configured NiFi; call `login()` before the first API call."""
    return Client(settings.url, settings.username, settings.password, timeout)

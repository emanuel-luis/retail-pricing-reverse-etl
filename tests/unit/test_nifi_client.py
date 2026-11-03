import httpx
import pytest
from pydantic import SecretStr

from retail_pricing.nifi.client import Client, NifiError, verify_tls

PASSWORD = "very-secret-password"
TOKEN = "eyJ.header.signature"


def _client(handler: httpx.MockTransport, url: str = "https://127.0.0.1:28443/nifi-api") -> Client:
    return Client(url, "admin", SecretStr(PASSWORD), transport=handler)


def test_tls_verification_is_off_only_on_loopback() -> None:
    assert verify_tls("https://127.0.0.1:28443/nifi-api") is False
    assert verify_tls("https://localhost:28443/nifi-api") is False
    assert verify_tls("https://nifi.example.org:8443/nifi-api") is True
    assert verify_tls("https://127.0.0.1.example.org/nifi-api") is True


def test_login_retries_while_booting_then_sends_only_the_bearer() -> None:
    answers = iter(["connect", 503, 409, 201])
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        if request.url.path.endswith("/access/token"):
            a = next(answers)
            if a == "connect":
                raise httpx.ConnectError("refused", request=request)
            headers = {"set-cookie": f"__Secure-Authorization-Bearer={TOKEN}; Path=/"}
            return httpx.Response(int(a), text=TOKEN if a == 201 else "", headers=headers)
        return httpx.Response(200, json={"ok": True})

    with _client(httpx.MockTransport(handler)) as c:
        c.login(wait_s=5, poll_s=0)
        assert c.post("/process-groups/x") == {"ok": True}
    last = seen[-1]
    assert last.headers["Authorization"] == f"Bearer {TOKEN}"
    # A cookie would subject the call to NiFi's browser CSRF check (403).
    assert "cookie" not in last.headers
    assert b"password=very-secret-password" in seen[0].content


def test_bad_credentials_fail_at_once_without_echoing_the_password() -> None:
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(400, text="The supplied username and password are not valid.")

    with _client(httpx.MockTransport(handler)) as c, pytest.raises(NifiError) as e:
        c.login(wait_s=60, poll_s=0)
    assert calls == 1
    assert PASSWORD not in str(e.value)


def test_login_gives_up_after_the_wait() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(503)

    with _client(httpx.MockTransport(handler)) as c, pytest.raises(NifiError, match="not ready"):
        c.login(wait_s=0, poll_s=0)


def test_api_errors_carry_status_and_path_but_never_the_token() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/access/token"):
            return httpx.Response(201, text=TOKEN)
        return httpx.Response(409, text="Port 'in' is invalid")

    with _client(httpx.MockTransport(handler)) as c:
        c.login(wait_s=0)
        with pytest.raises(NifiError) as e:
            c.put("/flow/process-groups/g1", json={})
    assert "409" in str(e.value) and "/flow/process-groups/g1" in str(e.value)
    assert TOKEN not in str(e.value)

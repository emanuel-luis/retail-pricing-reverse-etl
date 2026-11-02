import psycopg.conninfo
import pytest
from pydantic import SecretStr

from retail_pricing.settings import ControlSettings, OltpSettings


def test_ports_follow_the_compose_variables(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("OLTP_PG_PORT", "35432")
    monkeypatch.setenv("CONTROL_PG_PORT", "35433")
    assert OltpSettings(_env_file=None).port == 35432  # type: ignore[call-arg]
    assert ControlSettings(_env_file=None).port == 35433  # type: ignore[call-arg]


def test_conninfo_quotes_awkward_passwords() -> None:
    awkward = r"pa ss'w\ord=x"
    info = OltpSettings(_env_file=None).conninfo("u", SecretStr(awkward))  # type: ignore[call-arg]
    assert psycopg.conninfo.conninfo_to_dict(info)["password"] == awkward

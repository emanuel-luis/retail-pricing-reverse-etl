from collections.abc import Iterator

import pytest

from retail_pricing.clickhouse.client import ClickHouseError, Client, connect
from retail_pricing.settings import ClickHouseSettings

pytestmark = pytest.mark.clickhouse

PROBE_DBS = ("reverse", "pricing", "marts", "stg")


@pytest.fixture
def probes(admin: Client) -> Iterator[None]:
    """One row in a probe table of every dbt database, so SELECT grants can be tested."""
    for db in PROBE_DBS:
        admin.command(
            f"CREATE TABLE IF NOT EXISTS {db}.probe (a Int8) ENGINE = MergeTree ORDER BY a"
        )
    yield
    for db in PROBE_DBS:
        admin.command(f"DROP TABLE IF EXISTS {db}.probe")
    admin.command("DROP TABLE IF EXISTS stg.made_by_dbt")


ALLOWED = [
    ("nifi", "SELECT count() FROM raw.master__brand"),
    ("nifi", "INSERT INTO raw.master__brand SELECT * FROM raw.master__brand WHERE 0"),
    ("nifi", "SELECT count() FROM reverse.probe"),
    ("dbt", "SELECT count() FROM raw.master__brand"),
    ("dbt", "CREATE TABLE stg.made_by_dbt (a Int8) ENGINE = MergeTree ORDER BY a"),
    ("dbt", "SELECT count() FROM marts.probe"),
    ("dbt", "ALTER TABLE reverse.probe DELETE WHERE a = 1"),
    ("grafana", "SELECT count() FROM pricing.probe"),
    ("grafana", "SELECT count() FROM reverse.probe"),
    ("grafana", "SELECT count() FROM marts.probe"),
]
DENIED = [
    ("nifi", "SELECT count() FROM marts.probe"),
    ("nifi", "CREATE TABLE stg.nope (a Int8) ENGINE = Memory"),
    ("nifi", "INSERT INTO reverse.probe VALUES (1)"),
    ("nifi", "SELECT * FROM url('http://127.0.0.1:1/x', 'CSV')"),
    ("dbt", "INSERT INTO raw.master__brand SELECT * FROM raw.master__brand WHERE 0"),
    ("dbt", "CREATE DATABASE not_allowed"),
    ("dbt", "SELECT * FROM s3(raw_s3, filename = 'x.parquet', format = 'Parquet')"),
    ("dbt", "CREATE USER nobody"),
    ("grafana", "SELECT count() FROM raw.master__brand"),
    ("grafana", "SELECT count() FROM stg.probe"),
    ("grafana", "INSERT INTO marts.probe VALUES (1)"),
    ("grafana", "CREATE TABLE marts.nope (a Int8) ENGINE = Memory"),
]


@pytest.mark.parametrize(("user", "sql"), ALLOWED)
def test_allowed(ch_settings: ClickHouseSettings, probes: None, user: str, sql: str) -> None:
    with connect(ch_settings, user) as ch:
        ch.command(sql)


@pytest.mark.parametrize(("user", "sql"), DENIED)
def test_denied(ch_settings: ClickHouseSettings, probes: None, user: str, sql: str) -> None:
    with connect(ch_settings, user) as ch, pytest.raises(ClickHouseError) as e:
        ch.command(sql)
    assert "ACCESS_DENIED" in str(e.value) or "READONLY" in str(e.value)


def test_profiles(ch_settings: ClickHouseSettings) -> None:
    def settings(user: str) -> list[str | None]:
        with connect(ch_settings, user) as ch:
            return ch.rows(
                "SELECT getSetting('max_memory_usage'), getSetting('max_threads'), "
                "getSetting('readonly')"
            )[0]

    assert settings("nifi") == settings("dbt") == ["1288490188", "4", "0"]
    assert settings("grafana")[2] == "2"


def test_admin_is_not_default(ch_settings: ClickHouseSettings, admin: Client) -> None:
    assert ch_settings.user != "default"
    assert admin.scalar("SELECT currentUser()") == ch_settings.user
    no_default = ch_settings.model_copy(update={"user": "default"})
    with pytest.raises(ClickHouseError), connect(no_default) as ch:
        ch.command("SELECT 1")


def test_errors_never_carry_the_password(ch_settings: ClickHouseSettings) -> None:
    bad = ch_settings.model_copy(
        update={"dbt_password": ch_settings.dbt_password.__class__("wrong-pw-xyz")}
    )
    with pytest.raises(ClickHouseError) as e, connect(bad, "dbt") as ch:
        ch.command("SELECT 1")
    assert "wrong-pw-xyz" not in str(e.value)

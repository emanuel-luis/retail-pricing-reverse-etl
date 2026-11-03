"""Environment-driven settings, one class per variable prefix. Defaults are host-side."""

from pathlib import Path

from psycopg.conninfo import make_conninfo
from pydantic import AliasChoices, Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class _Base(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")


def _conninfo(host: str, port: int, db: str, user: str, password: SecretStr) -> str:
    # make_conninfo quotes every value, so a password with spaces, quotes or backslashes can
    # never break the string (a libpq parse error would echo part of it).
    return make_conninfo(
        host=host,
        port=port,
        dbname=db,
        user=user,
        password=password.get_secret_value(),
        application_name="retail-pricing",
    )


class DataSettings(_Base):
    """Where generated data lives and how DuckDB may use the machine (`DATA_*`)."""

    model_config = SettingsConfigDict(env_prefix="DATA_")

    dir: Path = Path("data")
    duckdb_memory_limit: str = "4GB"
    duckdb_threads: int = 8


class OltpSettings(_Base):
    """The OLTP Postgres and the passwords of the roles `oltp load` creates (`OLTP_*`)."""

    model_config = SettingsConfigDict(env_prefix="OLTP_")

    host: str = "127.0.0.1"
    # The same variable compose publishes the port with, so moving it in .env moves both.
    port: int = Field(default=25432, validation_alias=AliasChoices("OLTP_PG_PORT", "OLTP_PORT"))
    db: str = "retail"
    owner_user: str = "oltp_owner"
    owner_password: SecretStr = SecretStr("")
    workload_user: str = "workload"
    workload_password: SecretStr = SecretStr("")
    nifi_user: str = "nifi"
    nifi_password: SecretStr = SecretStr("")
    exporter_user: str = "exporter"
    exporter_password: SecretStr = SecretStr("")

    def conninfo(self, user: str, password: SecretStr) -> str:
        """A libpq keyword/value string, quoted by psycopg; never log it (it holds the password)."""
        return _conninfo(self.host, self.port, self.db, user, password)


class ControlSettings(_Base):
    """The control/audit Postgres and the passwords of its roles (`CONTROL_*`)."""

    model_config = SettingsConfigDict(env_prefix="CONTROL_")

    host: str = "127.0.0.1"
    # The same variable compose publishes the port with, so moving it in .env moves both.
    port: int = Field(
        default=25433, validation_alias=AliasChoices("CONTROL_PG_PORT", "CONTROL_PORT")
    )
    db: str = "control"
    owner_user: str = "control_owner"
    owner_password: SecretStr = SecretStr("")
    nifi_user: str = "nifi"
    nifi_password: SecretStr = SecretStr("")
    grafana_user: str = "grafana_ro"
    grafana_password: SecretStr = SecretStr("")
    exporter_user: str = "exporter"
    exporter_password: SecretStr = SecretStr("")

    def conninfo(self, user: str, password: SecretStr) -> str:
        """A libpq keyword/value string, quoted by psycopg; never log it (it holds the password)."""
        return _conninfo(self.host, self.port, self.db, user, password)


class WorkloadSettings(_Base):
    """Rates and seed of the OLTP workload generator (`WORKLOAD_*`)."""

    model_config = SettingsConfigDict(env_prefix="WORKLOAD_")

    seed: int = 7
    workers: int = 8
    sales_per_second: float = 20.0
    price_edits_per_second: float = 0.5
    override_edits_per_second: float = 0.1
    cost_rows_per_minute: float = 1.0
    label_batches_per_second: float = 1.0
    return_share: float = 0.02
    day_seconds: int = 600
    metrics_port: int = 9108


class ClickHouseSettings(_Base):
    """ClickHouse HTTP endpoint and user passwords (`CLICKHOUSE_*`).

    `user` and `password` are the image admin account (CLICKHOUSE_USER, CLICKHOUSE_PASSWORD).
    The users nifi, dbt and grafana come from config/clickhouse/users.d with their own passwords.
    """

    model_config = SettingsConfigDict(env_prefix="CLICKHOUSE_")

    host: str = "127.0.0.1"
    # The same variable compose publishes the port with, so moving it in .env moves both.
    port: int = Field(default=28123, validation_alias="CLICKHOUSE_HTTP_PORT")
    user: str = "ch_admin"
    password: SecretStr = SecretStr("")
    nifi_password: SecretStr = SecretStr("")
    dbt_password: SecretStr = SecretStr("")
    grafana_password: SecretStr = SecretStr("")

    @property
    def url(self) -> str:
        return f"http://{self.host}:{self.port}"

    def password_of(self, user: str) -> SecretStr:
        """Password of the admin or of one of the three XML users."""
        return {
            self.user: self.password,
            "nifi": self.nifi_password,
            "dbt": self.dbt_password,
            "grafana": self.grafana_password,
        }[user]


class S3Settings(_Base):
    """The S3-compatible raw zone (`S3_*`); credentials are local-only."""

    model_config = SettingsConfigDict(env_prefix="S3_")

    host: str = "127.0.0.1"
    api_port: int = 29000
    access_key: str = "retail_s3_admin"
    secret_key: SecretStr = SecretStr("")
    bucket: str = "retail-raw"

    @property
    def endpoint(self) -> str:
        return f"http://{self.host}:{self.api_port}"


class DbtRunnerSettings(_Base):
    """The dbt runner HTTP service (`DBT_RUNNER_*`, plus `DBT_THREADS`)."""

    model_config = SettingsConfigDict(env_prefix="DBT_RUNNER_")

    host: str = "127.0.0.1"
    # Host-side value; the compose service overrides it with the in-container port.
    port: int = 28580
    project_dir: Path = Path("dbt")
    # Job files, dbt targets and dbt logs; a named volume in compose.
    state_dir: Path = Path("runs/dbt-runner")
    dbt_bin: str = "dbt"
    threads: int = Field(default=2, validation_alias="DBT_THREADS")


class NifiSettings(_Base):
    """The NiFi HTTPS API and its single-user login (`NIFI_*`)."""

    model_config = SettingsConfigDict(env_prefix="NIFI_")

    host: str = "127.0.0.1"
    # The same variables compose publishes the ports with, so moving them in .env moves both.
    https_port: int = 28443
    trigger_port: int = 28081
    username: str = "nifi_admin"
    password: SecretStr = SecretStr("")

    @property
    def url(self) -> str:
        return f"https://{self.host}:{self.https_port}/nifi-api"

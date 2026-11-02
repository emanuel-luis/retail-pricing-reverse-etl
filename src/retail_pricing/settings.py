"""Environment-driven settings, one class per variable prefix. Defaults are host-side."""

from pathlib import Path

from pydantic import SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class _Base(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")


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
    port: int = 25432
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
        """A libpq keyword/value string; the password never reaches a log line."""
        return (
            f"host={self.host} port={self.port} dbname={self.db} user={user} "
            f"password={password.get_secret_value()} application_name=retail-pricing"
        )


class ControlSettings(_Base):
    """The control/audit Postgres and the passwords of its roles (`CONTROL_*`)."""

    model_config = SettingsConfigDict(env_prefix="CONTROL_")

    host: str = "127.0.0.1"
    port: int = 25433
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
        """A libpq keyword/value string; the password never reaches a log line."""
        return (
            f"host={self.host} port={self.port} dbname={self.db} user={user} "
            f"password={password.get_secret_value()} application_name=retail-pricing"
        )


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

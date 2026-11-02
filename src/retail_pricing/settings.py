"""Environment-driven settings, one class per variable prefix. Defaults are host-side."""

from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict


class _Base(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")


class DataSettings(_Base):
    """Where generated data lives and how DuckDB may use the machine (`DATA_*`)."""

    model_config = SettingsConfigDict(env_prefix="DATA_")

    dir: Path = Path("data")
    duckdb_memory_limit: str = "4GB"
    duckdb_threads: int = 8

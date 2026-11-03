from pathlib import Path

import boto3  # type: ignore[import-untyped]
import pytest
from botocore.config import Config  # type: ignore[import-untyped]
from typer.testing import CliRunner

from retail_pricing.cli import app
from retail_pricing.clickhouse.client import connect
from retail_pricing.settings import ClickHouseSettings, S3Settings

pytestmark = pytest.mark.clickhouse


def _s3(s: S3Settings):  # type: ignore[no-untyped-def]
    return boto3.client(
        "s3",
        endpoint_url=s.endpoint,
        aws_access_key_id=s.access_key,
        aws_secret_access_key=s.secret_key.get_secret_value(),
        region_name="us-east-1",
        config=Config(s3={"addressing_style": "path"}),
    )


def test_s3_init_is_idempotent(s3_settings: S3Settings) -> None:
    runner = CliRunner()
    for _ in range(2):
        result = runner.invoke(app, ["s3", "init"])
        assert result.exit_code == 0, result.output
    assert result.output.strip().endswith("exists")
    names = [b["Name"] for b in _s3(s3_settings).list_buckets()["Buckets"]]
    assert names.count(s3_settings.bucket) == 1


def test_nifi_reads_the_bucket_through_the_named_collection(
    s3_settings: S3Settings, ch_settings: ClickHouseSettings, data_dir: Path
) -> None:
    CliRunner().invoke(app, ["s3", "init"])
    src = data_dir / "oltp" / "sf=0.01" / "master__brand.parquet"
    client, key = _s3(s3_settings), "probe/master__brand.parquet"
    client.put_object(Bucket=s3_settings.bucket, Key=key, Body=src.read_bytes())
    try:
        with connect(ch_settings, "nifi") as ch:
            n = ch.scalar(f"SELECT count() FROM s3(raw_s3, filename = '{key}', format = 'Parquet')")
        assert int(n or 0) > 0
    finally:
        client.delete_object(Bucket=s3_settings.bucket, Key=key)

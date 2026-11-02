"""`retail-pricing s3 ...` commands (boto3 against RustFS)."""

import typer

app = typer.Typer(no_args_is_help=True, help="S3-compatible raw zone (RustFS).")


@app.command("init")
def init_command() -> None:
    """Create the raw bucket when it is missing (safe to run twice)."""
    import boto3  # type: ignore[import-untyped]
    from botocore.config import Config  # type: ignore[import-untyped]
    from botocore.exceptions import ClientError  # type: ignore[import-untyped]

    from retail_pricing.settings import S3Settings

    s = S3Settings()
    client = boto3.client(
        "s3",
        endpoint_url=s.endpoint,
        aws_access_key_id=s.access_key,
        aws_secret_access_key=s.secret_key.get_secret_value(),
        region_name="us-east-1",
        config=Config(
            s3={"addressing_style": "path"},
            request_checksum_calculation="when_required",
            response_checksum_validation="when_required",
        ),
    )
    try:
        client.head_bucket(Bucket=s.bucket)
        typer.echo(f"bucket {s.bucket} exists")
    except ClientError as e:
        if e.response["Error"]["Code"] not in ("404", "NoSuchBucket", "NotFound"):
            raise
        client.create_bucket(Bucket=s.bucket)
        typer.echo(f"bucket {s.bucket} created")

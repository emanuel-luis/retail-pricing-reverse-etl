"""`retail-pricing nifi ...` commands. Implementations are imported inside each command."""

from enum import StrEnum
from pathlib import Path
from typing import Annotated

import typer

app = typer.Typer(no_args_is_help=True, help="Apache NiFi: token, definitions and deploy.")
definitions_app = typer.Typer(no_args_is_help=True, help="Processor and service definitions.")
app.add_typer(definitions_app, name="definitions")


@app.command("token")
def token_command(
    out: Annotated[Path | None, typer.Option(help="File to write the JWT to (mode 600).")] = None,
    stdout: Annotated[bool, typer.Option("--stdout", help="Print the JWT instead.")] = False,
    wait: Annotated[float, typer.Option(help="Seconds to wait for NiFi to boot.")] = 300.0,
) -> None:
    """Fetch a NiFi JWT into a file (Prometheus credentials_file); never printed unless asked."""
    import os

    from retail_pricing.nifi.client import connect
    from retail_pricing.settings import NifiSettings

    if (out is None) == (not stdout):
        raise typer.BadParameter("give exactly one of --out FILE or --stdout")
    with connect(NifiSettings()) as client:
        token = client.fetch_token(wait_s=wait)
    if out is None:
        typer.echo(token)
        return
    # Write next to the target and rename, so a reader never sees a half-written token.
    tmp = out.with_name(out.name + ".tmp")
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w", encoding="utf-8") as f:
        f.write(token)
    os.replace(tmp, out)
    typer.echo(f"token written to {out}")


@definitions_app.command("pull")
def definitions_pull(
    out_dir: Annotated[Path, typer.Option(help="Where the JSON files go.")] = Path(
        "nifi/definitions"
    ),
) -> None:
    """Write one definition JSON per processor and controller-service type the flows use."""
    from retail_pricing.nifi.client import connect
    from retail_pricing.nifi.definitions import pull
    from retail_pricing.settings import NifiSettings

    with connect(NifiSettings()) as client:
        client.login()
        for path in pull(client, out_dir):
            typer.echo(path.as_posix())


class Env(StrEnum):
    local = "local"
    ci = "ci"


@app.command("deploy")
def deploy_command(
    env: Annotated[Env, typer.Option(help="Parameter set: nifi/params/<env>.json.")],
    flow: Annotated[Path, typer.Option(help="Flow-definition JSON to upload.")] = Path(
        "nifi/flows/retail-pricing.json"
    ),
    start: Annotated[bool, typer.Option(help="Start the group after it validates.")] = True,
    wait: Annotated[float, typer.Option(help="Seconds to wait for NiFi to boot.")] = 300.0,
) -> None:
    """Replace the retail-pricing group: parameters, upload, services, validation, start."""
    from retail_pricing.nifi.client import connect
    from retail_pricing.nifi.deploy import deploy
    from retail_pricing.settings import ControlSettings, NifiSettings

    settings = NifiSettings()
    with connect(settings) as client:
        gid = deploy(
            client,
            flow,
            env.value,
            Path(f"nifi/params/{env.value}.json"),
            start=start,
            trigger=(settings.host, settings.trigger_port),
            control=ControlSettings(),
            wait_s=wait,
        )
    typer.echo(f"deployed group {gid}")

"""`retail-pricing control ...` commands. Implementations are imported inside each command."""

import typer

app = typer.Typer(no_args_is_help=True, help="Control/audit Postgres: registry and audit.")

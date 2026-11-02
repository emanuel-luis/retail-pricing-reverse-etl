"""`retail-pricing oltp ...` commands. Implementations are imported inside each command."""

import typer

app = typer.Typer(no_args_is_help=True, help="OLTP Postgres: create, load and inspect.")

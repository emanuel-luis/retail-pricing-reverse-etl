"""`retail-pricing workload ...` commands. Implementations are imported inside each command."""

import typer

app = typer.Typer(no_args_is_help=True, help="Concurrent OLTP workload and scripted edits.")

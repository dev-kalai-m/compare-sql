from __future__ import annotations

import json
import os
from pathlib import Path

import click

from .runner import run_all
from .types import Config


@click.group()
def main() -> None:
    """cmp-sql — compare Oracle DDL files at AST level."""


@main.command()
@click.option("--source", "source_dir", default="assets/code_sql",
              type=click.Path(file_okay=False, path_type=Path))
@click.option("--target", "target_dir", default="assets/db_sql",
              type=click.Path(file_okay=False, path_type=Path))
@click.option("--out", "out_dir", default="assets/cmp_results",
              type=click.Path(file_okay=False, path_type=Path))
@click.option("--mode",
              type=click.Choice(["strict", "normalized", "semantic"], case_sensitive=False),
              default="semantic", show_default=True)
@click.option("--ignore-storage/--strict-storage", default=True, show_default=True,
              help="Drop Oracle physical/storage clauses before comparing.")
@click.option("--ignore-column-order/--strict-column-order", default=True, show_default=True,
              help="Sort CREATE TABLE columns before comparing (Oracle treats "
                   "column order as semantic for SELECT *; disable if that matters).")
@click.option("--workers", type=int, default=0,
              help="Worker processes; 0 means os.cpu_count().")
@click.option("--html-for", type=click.Choice(["all", "non-identical"]),
              default="non-identical", show_default=True)
@click.option("--timeout", "timeout_seconds", type=float, default=5.0, show_default=True)
def run(
    source_dir: Path,
    target_dir: Path,
    out_dir: Path,
    mode: str,
    ignore_storage: bool,
    ignore_column_order: bool,
    workers: int,
    html_for: str,
    timeout_seconds: float,
) -> None:
    """Compare all file pairs and write reports to OUT_DIR."""
    cfg = Config(
        source_dir=source_dir,
        target_dir=target_dir,
        out_dir=out_dir,
        mode=mode.lower(),
        ignore_storage=ignore_storage,
        ignore_column_order=ignore_column_order,
        workers=workers,
        html_for=html_for,
        timeout_seconds=timeout_seconds,
    )
    summary = run_all(cfg)
    _print_totals(summary)


@main.command()
@click.argument("summary_path", type=click.Path(exists=True, dir_okay=False, path_type=Path),
                default="assets/cmp_results/summary.json")
def stats(summary_path: Path) -> None:
    """Print totals from a summary.json file."""
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    _print_totals(summary)


def _print_totals(summary: dict) -> None:
    totals = summary.get("totals", {})
    click.echo(f"Generated at: {summary.get('generated_at')}")
    click.echo("Totals:")
    for k in sorted(totals):
        click.echo(f"  {k:<22} {totals[k]}")
    click.echo(f"Files total: {sum(totals.values())}")


if __name__ == "__main__":
    main()

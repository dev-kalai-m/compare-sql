from __future__ import annotations

import difflib
from pathlib import Path

from .types import PairResult, Severity


def write_text_report(
    out_path: Path,
    result: PairResult,
    src_rendered: str,
    tgt_rendered: str,
) -> None:
    lines: list[str] = []
    lines.append("# cmp-sql report")
    lines.append(f"# file:    {result.name}")
    lines.append(f"# status:  {result.status.value}")
    lines.append(
        f"# edits:   {result.edits.major} major · {result.edits.minor} minor · "
        f"{result.edits.cosmetic} cosmetic"
    )
    if result.text_fallback:
        lines.append("# note:    text-fallback used (sqlglot could not fully parse)")
    if result.parse_error_src:
        lines.append(f"# src parse error: {result.parse_error_src}")
    if result.parse_error_tgt:
        lines.append(f"# tgt parse error: {result.parse_error_tgt}")
    lines.append("#" + "-" * 72)

    unified = difflib.unified_diff(
        src_rendered.splitlines(keepends=True),
        tgt_rendered.splitlines(keepends=True),
        fromfile=f"assets/code_sql/{result.name}",
        tofile=f"assets/db_sql/{result.name}",
        n=3,
    )
    lines.append("".join(unified).rstrip() or "(no textual diff)")

    if result.classified:
        lines.append("")
        lines.append("Structured edits:")
        for e in sorted(result.classified, key=_severity_sort):
            lines.append(f"  [{e.severity.value:>8}] {e.kind:<6} {e.path}  {e.summary}")

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _severity_sort(e):
    order = {Severity.MAJOR: 0, Severity.MINOR: 1, Severity.COSMETIC: 2}
    return (order.get(e.severity, 3), e.path)

from __future__ import annotations

import difflib
import html
from functools import lru_cache
from pathlib import Path

from jinja2 import Environment, FileSystemLoader, select_autoescape
from pygments import highlight
from pygments.formatters import HtmlFormatter
from pygments.lexers import get_lexer_by_name

from .types import PairResult, Severity

_TEMPLATE_DIR = Path(__file__).resolve().parent.parent.parent / "templates"


@lru_cache(maxsize=1)
def _env() -> Environment:
    return Environment(
        loader=FileSystemLoader(str(_TEMPLATE_DIR)),
        autoescape=select_autoescape(["html"]),
    )


@lru_cache(maxsize=1)
def _lexer():
    return get_lexer_by_name("sql", stripall=False)


@lru_cache(maxsize=1)
def _formatter():
    return HtmlFormatter(nowrap=True, noclasses=True, style="monokai")


def _pygmentize(text: str) -> str:
    if not text:
        return ""
    return highlight(text, _lexer(), _formatter())


def write_html_report(
    out_path: Path,
    result: PairResult,
    src_rendered: str,
    tgt_rendered: str,
) -> None:
    tmpl = _env().get_template("side_by_side.html.j2")

    unified_lines = difflib.unified_diff(
        src_rendered.splitlines(keepends=False),
        tgt_rendered.splitlines(keepends=False),
        fromfile=f"assets/code_sql/{result.name}",
        tofile=f"assets/db_sql/{result.name}",
        n=3,
        lineterm="",
    )
    unified_html_parts: list[str] = []
    for ln in unified_lines:
        esc = html.escape(ln)
        if ln.startswith("+") and not ln.startswith("+++"):
            unified_html_parts.append(f'<span class="add">{esc}</span>')
        elif ln.startswith("-") and not ln.startswith("---"):
            unified_html_parts.append(f'<span class="del">{esc}</span>')
        else:
            unified_html_parts.append(f'<span class="ctx">{esc}</span>')

    rendered = tmpl.render(
        name=result.name,
        status=result.status.value,
        edits=result.edits.as_dict(),
        text_fallback=result.text_fallback,
        parse_error_src=result.parse_error_src,
        parse_error_tgt=result.parse_error_tgt,
        src_html=_pygmentize(src_rendered),
        tgt_html=_pygmentize(tgt_rendered),
        unified_html="\n".join(unified_html_parts) or "(no textual diff)",
        classified=[
            {
                "severity": e.severity.value,
                "kind": e.kind,
                "path": e.path,
                "summary": e.summary,
            }
            for e in sorted(result.classified, key=_severity_sort)
        ],
    )
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(rendered, encoding="utf-8")


def _severity_sort(e):
    order = {Severity.MAJOR: 0, Severity.MINOR: 1, Severity.COSMETIC: 2}
    return (order.get(e.severity, 3), e.path)

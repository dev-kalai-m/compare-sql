from __future__ import annotations

import logging
import re
from dataclasses import dataclass

import sqlglot
from sqlglot import exp

# Silence sqlglot's "Falling back to parsing as a 'Command'" messages; we handle
# that case explicitly by retrying with stripped physical clauses.
logging.getLogger("sqlglot").setLevel(logging.ERROR)

ORACLE = "oracle"

_OPAQUE_NODES = (exp.Command,)

# Oracle physical/storage keywords that appear after the closing schema paren
# of CREATE TABLE / INDEX and tend to trip sqlglot's Oracle parser. Keywords
# only — the scanner below reads any balanced (...) group that follows.
_PHYSICAL_KEYWORDS = {
    "TABLESPACE", "PCTFREE", "PCTUSED", "INITRANS", "MAXTRANS",
    "STORAGE", "LOGGING", "NOLOGGING", "CACHE", "NOCACHE",
    "MONITORING", "NOMONITORING", "PARALLEL", "NOPARALLEL",
    "COMPRESS", "NOCOMPRESS", "RESULT_CACHE",
    "LOB", "SEGMENT", "ENABLE", "DISABLE",
}

# Multi-word physical clauses that begin with these keywords and consume
# everything through the end of the statement (or until the next top-level
# keyword boundary). Used for partitioning etc., which can contain multiple
# balanced parens and are too complex for the single-clause consumer.
_PHYSICAL_TAIL_STARTERS = {
    "PARTITION", "SUBPARTITION",         # PARTITION BY RANGE/LIST/HASH/REFERENCE (...)
    "ORGANIZATION",                      # ORGANIZATION EXTERNAL/INDEX/HEAP
    "CLUSTER",                           # IN CLUSTER cluster_name (col, ...)
    "XMLTYPE",                           # XMLTYPE <col> STORE AS ...
}


@dataclass
class ParseResult:
    expressions: list[exp.Expression]   # empty when fallback_text is set
    fallback_text: str | None = None    # populated when parse failed or opaque
    error: str | None = None
    physical_stripped: bool = False     # true if we pre-stripped to get a parse

    @property
    def ok(self) -> bool:
        return self.fallback_text is None


def parse_sql(text: str) -> ParseResult:
    """Parse Oracle SQL. On failure, retry with physical-storage clauses stripped;
    on second failure, return a text-fallback result instead of raising.
    """
    if not text.strip():
        return ParseResult(expressions=[])

    first = _try_parse(text)
    if first is not None and not _all_opaque(first):
        return ParseResult(expressions=first)

    stripped = _strip_physical_clauses(text)
    if stripped != text:
        second = _try_parse(stripped)
        if second is not None and not _all_opaque(second):
            return ParseResult(expressions=second, physical_stripped=True)

    # Both attempts failed or produced only Command nodes → text fallback.
    return ParseResult(
        expressions=[],
        fallback_text=_normalize_text(text),
        error=(None if first is not None else "parse error on raw; fallback engaged"),
    )


def _try_parse(text: str) -> list[exp.Expression] | None:
    try:
        trees = sqlglot.parse(text, read=ORACLE)
    except Exception:  # noqa: BLE001 — sqlglot raises ParseError, TokenError, …
        return None
    return [t for t in trees if t is not None]


def _all_opaque(trees: list[exp.Expression]) -> bool:
    return bool(trees) and all(isinstance(t, _OPAQUE_NODES) for t in trees)


_WORD_RE = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")


def _strip_physical_clauses(text: str) -> str:
    """Remove Oracle physical/storage keywords (and any balanced parenthetical that
    follows them) from the text. Balanced-paren aware, so it handles things like
    STORAGE(INITIAL 64K NEXT 1M) and LOB(DATA) STORE AS BASICFILE.
    """
    out: list[str] = []
    i, n = 0, len(text)
    while i < n:
        ch = text[i]
        if ch in ("'", '"'):  # walk past string/quoted identifier verbatim
            j = _skip_quoted(text, i, ch)
            out.append(text[i:j])
            i = j
            continue
        if ch == "-" and i + 1 < n and text[i + 1] == "-":
            j = text.find("\n", i)
            j = n if j == -1 else j
            out.append(text[i:j])
            i = j
            continue
        if ch == "/" and i + 1 < n and text[i + 1] == "*":
            j = text.find("*/", i + 2)
            j = n if j == -1 else j + 2
            out.append(text[i:j])
            i = j
            continue
        if ch.isalpha() or ch == "_":
            m = _WORD_RE.match(text, i)
            assert m is not None
            word = m.group(0)
            upper = word.upper()
            if upper in _PHYSICAL_TAIL_STARTERS:
                i = _consume_through_terminator(text, m.end())
                continue
            if upper in _PHYSICAL_KEYWORDS:
                i = _consume_physical(text, m.end())
                continue
            out.append(word)
            i = m.end()
            continue
        out.append(ch)
        i += 1
    return "".join(out)


def _consume_through_terminator(text: str, pos: int) -> int:
    """Consume text from `pos` through the end of the current statement.
    Skips balanced parens, string and quoted-identifier literals, and comments;
    stops at a top-level `;` (inclusive-exclusive — returns index of `;` itself,
    leaving it for the main loop).
    """
    n = len(text)
    depth = 0
    while pos < n:
        c = text[pos]
        if c in ("'", '"'):
            pos = _skip_quoted(text, pos, c)
            continue
        if c == "-" and pos + 1 < n and text[pos + 1] == "-":
            j = text.find("\n", pos)
            pos = n if j == -1 else j
            continue
        if c == "/" and pos + 1 < n and text[pos + 1] == "*":
            j = text.find("*/", pos + 2)
            pos = n if j == -1 else j + 2
            continue
        if c == "(":
            depth += 1
        elif c == ")":
            if depth == 0:
                return pos  # unmatched close — belongs to outer context
            depth -= 1
        elif c == ";" and depth == 0:
            return pos
        pos += 1
    return pos


def _consume_physical(text: str, pos: int) -> int:
    """After seeing a physical keyword at [start..pos], consume its argument:
    either the next balanced `(...)` or the next token (identifier/number)."""
    n = len(text)
    # Skip whitespace.
    while pos < n and text[pos] in " \t\r\n":
        pos += 1
    if pos >= n:
        return pos
    if text[pos] == "(":
        depth = 0
        while pos < n:
            c = text[pos]
            if c == "(":
                depth += 1
            elif c == ")":
                depth -= 1
                pos += 1
                if depth == 0:
                    return pos
                continue
            elif c in ("'", '"'):
                pos = _skip_quoted(text, pos, c)
                continue
            pos += 1
        return pos
    # Single-token argument (e.g. TABLESPACE USERS, PCTFREE 10).
    m = re.match(r"[A-Za-z_][A-Za-z0-9_$#]*|\d+[KMG]?", text[pos:])
    if m:
        return pos + m.end()
    return pos


def _skip_quoted(text: str, pos: int, quote: str) -> int:
    n = len(text)
    pos += 1
    while pos < n:
        if text[pos] == quote:
            # Oracle doubles quote to escape.
            if pos + 1 < n and text[pos + 1] == quote:
                pos += 2
                continue
            return pos + 1
        pos += 1
    return pos


_COMMENT_BLOCK = re.compile(r"/\*.*?\*/", re.DOTALL)
_COMMENT_LINE = re.compile(r"--[^\n]*")
_WS_RUNS = re.compile(r"[ \t]+")
_BLANK_LINES = re.compile(r"\n\s*\n+")


def _normalize_text(text: str) -> str:
    """Lossy normalizer for text-fallback diffs: strip comments, collapse whitespace."""
    t = _COMMENT_BLOCK.sub(" ", text)
    t = _COMMENT_LINE.sub("", t)
    lines = [_WS_RUNS.sub(" ", ln).rstrip() for ln in t.splitlines()]
    t = "\n".join(ln for ln in lines if ln.strip())
    t = _BLANK_LINES.sub("\n", t)
    return t.strip() + "\n"

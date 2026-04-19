# cmp-sql

Oracle DDL AST comparator. Pairs files in `assets/code_sql/` (source) with
same-name files in `assets/db_sql/` (target), parses both with `sqlglot`
(Oracle dialect), and writes per-pair text diffs + HTML side-by-side views to
`assets/cmp_results/`.

Designed for ~5 000 file corpora. Runs multi-process; full design is in
[PLAN.md](PLAN.md).

## Install

Requires Python ≥ 3.11 and [uv](https://docs.astral.sh/uv/).

```bash
uv sync
```

## Layout

```text
assets/
  code_sql/         # source DDL (one statement or more per .sql file)
  db_sql/           # target DDL, same filename as its code_sql counterpart
  cmp_results/
    text/<name>.diff           always for non-identical pairs
    html/<name>.html           only for pairs that actually differ
    summary.json               aggregate status + per-file counts
    missing.log                pairs where one side was absent
```

**Note:** The `assets/` folder structure is tracked in git (via `.gitkeep` files),
but all user data files (`.sql`, `.diff`, `.html`, `.json`, `.log`) are ignored.

## Usage

```bash
# Default: semantic mode, all CPU cores, storage + column-order ignored.
uv run cmp-sql run

# Explicit flags:
uv run cmp-sql run \
  --source assets/code_sql --target assets/db_sql --out assets/cmp_results \
  --mode semantic \
  --ignore-storage \
  --ignore-column-order \
  --workers 8 \
  --html-for non-identical

# Print totals from a previous run:
uv run cmp-sql stats assets/cmp_results/summary.json
```

### Comparison modes

| `--mode` | What gets equalized |
| --- | --- |
| `strict` | Raw text equality only (post-whitespace-trim). |
| `normalized` | Adds: case folding, storage clauses ignored, `SYS_Cnnn` names dropped, constraint order sorted, numeric literals folded. |
| `semantic` *(default)* | Adds: inline PK/UQ promoted to out-of-line, view bodies passed through `sqlglot.optimizer` (qualifies identifiers, reorders `AND`, simplifies booleans). |

### Important flags

| Flag | Default | Notes |
| --- | --- | --- |
| `--ignore-storage / --strict-storage` | ignore | Drops `TABLESPACE`, `PCTFREE`, `STORAGE(...)`, `LOGGING`, `PARTITION BY`, `ORGANIZATION`, etc. before comparing. |
| `--ignore-column-order / --strict-column-order` | ignore | Sorts `CREATE TABLE` columns alphabetically. Oracle treats column order as semantic for `SELECT *` and `INSERT ... VALUES (...)` — turn off if that matters for you. |
| `--html-for all\|non-identical` | non-identical | HTML reports are heavy; skip them for `identical` by default. |
| `--workers N` | `cpu_count()` | 1 forces single-process (useful for debugging). |

### NLS (BYTE vs CHAR)

**No auto-conversion.** `VARCHAR2(100 BYTE)` vs `VARCHAR2(100 CHAR)` (or
`VARCHAR2(100)`) are treated as distinct. Source and target must match
as-written.

## Per-pair status

Written to `summary.json` and each report header:

| Status | Meaning |
| --- | --- |
| `identical` | Byte-identical post-trim. HTML is skipped. |
| `normalized_match` | Equal after structural normalization. |
| `semantic_match` | Equal after semantic rewrites (optimizer / inline-to-outline). |
| `different` | At least one non-cosmetic AST edit remains. |
| `parse_error_src` · `parse_error_tgt` · `parse_error_both` | sqlglot failed; **text-fallback** diff used (comments stripped, whitespace collapsed). |
| `missing_src` · `missing_tgt` | Only one side exists. Logged to `missing.log`. |

### Parse fallback

PL/SQL bodies (`PACKAGE BODY`, complex `TRIGGER`, …) that sqlglot can't fully
parse fall back to a **normalized text diff** rather than being silently
skipped. `text_fallback: true` is recorded in `summary.json` for every pair
that took this path.

Before giving up, the parser makes a second attempt with Oracle physical
clauses stripped (`TABLESPACE`, `PCTFREE`, `STORAGE(...)`, `PARTITION BY`,
`ORGANIZATION`, …). This rescues most production DDL that sqlglot's Oracle
dialect doesn't model in full.

## Output examples

`assets/cmp_results/text/<name>.diff`:

```diff
# cmp-sql report
# file:    orders.sql
# status:  different
# edits:   2 major · 1 minor · 0 cosmetic
#------------------------------------------------------------------------
--- assets/code_sql/orders.sql
+++ assets/db_sql/orders.sql
@@ -2,4 +2,5 @@
   ID NUMBER(10) NOT NULL,
-  AMOUNT NUMBER(12, 2)
+  AMOUNT NUMBER(14, 4),
+  STATUS VARCHAR2(20)
 );

Structured edits:
  [   major] Insert stmt[0].ColumnDef    Insert ColumnDef: STATUS VARCHAR2(20)
  [   major] Update stmt[0].DataType     Update DataType: NUMBER(12, 2) → NUMBER(14, 4)
  [   minor] Update stmt[0].Literal      Update Literal: 2 → 4
```

`assets/cmp_results/summary.json`:

```json
{
  "generated_at": "2026-04-18T17:06:20+00:00",
  "totals": {
    "identical": 4120,
    "normalized_match": 430,
    "semantic_match": 220,
    "different": 180,
    "parse_error_both": 35,
    "missing_src": 10,
    "missing_tgt": 5
  },
  "files": [ { "name": "orders.sql", "status": "different", "edits": {...} } ]
}
```

## Development

```bash
uv run pytest             # full test suite
uv run pytest -k pairing  # one module
```

Modules:

- [src/cmp_sql/pairing.py](src/cmp_sql/pairing.py) — filename pairing + missing detection
- [src/cmp_sql/parser.py](src/cmp_sql/parser.py) — sqlglot Oracle parse + physical-clause pre-strip + text fallback
- [src/cmp_sql/normalizer.py](src/cmp_sql/normalizer.py) — structural AST canonicalization
- [src/cmp_sql/semantic.py](src/cmp_sql/semantic.py) — optimizer-based semantic rewrites
- [src/cmp_sql/differ.py](src/cmp_sql/differ.py) — `sqlglot.diff` + severity classification
- [src/cmp_sql/reporter_text.py](src/cmp_sql/reporter_text.py) · [reporter_html.py](src/cmp_sql/reporter_html.py) — outputs
- [src/cmp_sql/runner.py](src/cmp_sql/runner.py) — multiprocessing orchestrator
- [src/cmp_sql/cli.py](src/cmp_sql/cli.py) — click CLI

See [PLAN.md](PLAN.md) for the full design and the phased-delivery history.

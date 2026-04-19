# cmp-sql — Oracle DDL AST Comparator

Compare source DDL files (`assets/code_sql/`) against target DDL files
(`assets/db_sql/`) at the AST level, pairing them by filename. Emit a text diff
and an HTML side-by-side view for every non-identical pair; write an aggregate
`summary.json`.

## 1. Fixed decisions

| Area | Decision |
| --- | --- |
| Dialect | Oracle (both sides) |
| Statement type | DDL only |
| Comparison modes | Normalized equality + semantic equivalence |
| Outputs | Text diff (always for non-identical) + HTML side-by-side (diffs only) |
| Missing pair | Log to `cmp_results/missing.log`, continue |
| Scale target | ~5 000 file pairs, < 60 s on 8 cores |
| Python env | uv |
| NLS | Source and target must match as-is; **no auto-conversion** (BYTE vs CHAR treated as diff) |
| HTML rendering | Only for `different`, `parse_error_*`, `semantic_match`, `normalized_match` (skip `identical`) |
| Schema defaults | None — no implicit qualification |
| PL/SQL bodies | Text fallback when sqlglot cannot parse |

## 2. Status taxonomy (per pair)

| Status | Meaning |
| --- | --- |
| `identical` | Source and target strings are byte-identical (post-strip) |
| `normalized_match` | Equal after structural normalization |
| `semantic_match` | Equal after optimizer-driven semantic rewrites |
| `different` | At least one non-cosmetic AST edit remains |
| `parse_error_src` | sqlglot failed on source; text fallback used |
| `parse_error_tgt` | sqlglot failed on target; text fallback used |
| `parse_error_both` | Both failed; text fallback used |
| `missing_src` | Only target exists |
| `missing_tgt` | Only source exists |

## 3. Repository layout

```
cmp-sql/
├── pyproject.toml
├── PLAN.md
├── README.md
├── assets/
│   ├── code_sql/                   source DDL inputs
│   ├── db_sql/                     target DDL inputs
│   └── cmp_results/
│       ├── text/<name>.diff
│       ├── html/<name>.html        (only for non-identical pairs)
│       ├── summary.json
│       └── missing.log
├── src/cmp_sql/
│   ├── __init__.py
│   ├── cli.py                  click CLI entrypoint
│   ├── pairing.py              filename pairing + missing detection
│   ├── parser.py               sqlglot.parse wrapper + text fallback
│   ├── normalizer.py           structural AST canonicalization
│   ├── semantic.py             optimizer-based semantic rewrites
│   ├── differ.py               sqlglot.diff + edit classifier
│   ├── reporter_text.py        text diff writer
│   ├── reporter_html.py        Jinja2 HTML side-by-side writer
│   ├── runner.py               multiprocessing orchestrator
│   └── types.py                shared dataclasses (PairResult, Edit, …)
├── templates/
│   └── side_by_side.html.j2
└── tests/
    ├── fixtures/{identical,normalized,semantic,different,unparseable,missing}/
    └── test_*.py
```

## 4. Pipeline (per pair)

```
read(src, tgt)
  → parse(dialect="oracle")          [parser.py]
  → on failure: text-fallback flag
  → normalize                        [normalizer.py]
  → semantic rewrite (if mode>=semantic)  [semantic.py]
  → diff + classify edits            [differ.py]
  → text report (always if non-identical)     [reporter_text.py]
  → html report (only if status ≠ identical)  [reporter_html.py]
```

### 4.1 Structural normalization rules
- Uppercase keywords & unquoted identifiers (Oracle folding).
- Strip comments.
- Drop physical/storage clauses by default: `TABLESPACE`, `PCTFREE`, `PCTUSED`,
  `INITRANS`, `MAXTRANS`, `STORAGE(...)`, `LOGGING/NOLOGGING`, `PARALLEL`,
  `LOB(...) STORE AS`, `CACHE/NOCACHE`, `MONITORING/NOMONITORING`.
  Controlled by `--strict-storage` / `--ignore-storage` (default ignore).
- Preserve column order in `CREATE TABLE` (semantic in Oracle).
- Sort unordered sets: table-level constraints (excluding their column lists),
  indexes, grants, `WITH` option lists.
- Type aliasing: `INT` → `NUMBER(38)`, `VARCHAR` → `VARCHAR2`,
  `INTEGER` → `NUMBER(38)`. No BYTE/CHAR auto-conversion.
- Drop system-generated constraint names matching `^SYS_C\d+$`.
- Fold numeric literals to canonical form (`100.0` → `100`, `1e2` → `100`).

### 4.2 Semantic equivalence (additive on normalization)
- Inline vs out-of-line column constraints → canonical out-of-line form.
- `NUMBER` ≡ `NUMBER(*, 0)` ≡ `INTEGER`.
- Views: run `sqlglot.optimizer.optimize(...)` with `dialect="oracle"` to
  qualify identifiers, simplify expressions, normalize booleans, merge subs.
- Index column sets: do NOT reorder (position is semantic for composite B-tree).

### 4.3 Diff engine
- `sqlglot.diff(src_ast, tgt_ast)` returns `Insert | Remove | Update | Move | Keep`.
- Severity classes:
  - **cosmetic** — storage clauses, generated names, pure comment edits.
  - **minor** — constraint reorder, collation, comment updates.
  - **major** — column add/remove, type change, nullability, PK/UK/FK change,
    default expression change, view body change.
- A pair is `different` only if ≥ 1 non-cosmetic edit remains.

### 4.4 PL/SQL fallback
1. Attempt full parse.
2. If parse fails or yields an opaque `Command` node, run a normalized text
   diff: strip `--` and `/* … */` comments, collapse whitespace, uppercase
   keywords.
3. Status gains a `text_fallback: true` flag in `summary.json`.

## 5. Output artefacts

### `cmp_results/text/<name>.diff`
Header (status, parse ok/fallback, edit counts) + unified diff of canonicalized
SQL + a `Structured edits:` list keyed by severity.

### `cmp_results/html/<name>.html`
Two-pane side-by-side (Jinja2 + Pygments `sql` lexer), status chip, edit list
with severity badges, per-edit anchors on gutter.

### `cmp_results/summary.json`
Top-level `totals` + per-file records: `{name, status, edits:{major,minor,cosmetic}, text_fallback, parse_errors}`.

### `cmp_results/missing.log`
One line per missing file: `<name>\t<missing_side>\t<iso-timestamp>`.

## 6. Performance

- `multiprocessing.Pool(cpu_count())`, `imap_unordered(chunksize=25)`.
- Workers are self-contained — read, parse, diff, render, write own outputs.
- `tqdm` progress bar on the orchestrator.
- Per-file hard timeout (5 s) → status `timeout`.

## 7. CLI

```
cmp-sql run
    [--source assets/code_sql] [--target assets/db_sql] [--out assets/cmp_results]
    [--mode strict|normalized|semantic]   # default: semantic
    [--ignore-storage | --strict-storage] # default: ignore
    [--ignore-column-order | --strict-column-order] # default: ignore
    [--workers N]                         # default: cpu_count
    [--html-for all|non-identical]        # default: non-identical
    [--timeout 5]

cmp-sql stats [assets/cmp_results/summary.json]
```

## 8. Phased delivery

| Phase | Deliverable |
| --- | --- |
| 0 | Scaffold (uv, deps, dirs, stubs, PLAN.md) |
| 1 | Pairing + missing detection |
| 2 | Parser wrapper + text fallback |
| 3 | Structural normalizer |
| 4 | Semantic rewriter |
| 5 | Diff + classify |
| 6 | Text + HTML reporters |
| 7 | Runner + CLI |
| 8 | Tests (fixtures per status) |
| 9 | End-to-end smoke run |

## 9. Risks

| Risk | Mitigation |
| --- | --- |
| PL/SQL parse gaps | Text-fallback, flagged in `summary.json` |
| sqlglot.diff O(n²) | Normalize first (smaller tree); per-file timeout |
| False semantic matches | `--mode normalized` toggle to disable optimizer |
| 5000× HTML bloat | Render HTML only for non-identical by default |

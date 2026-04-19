"""Semantic-equivalence rewrites — additive on top of structural normalization.

Two classes of rewrites:

1. View bodies: run `sqlglot.optimizer.optimize` on the SELECT that backs a
   CREATE VIEW so that AND-clause ordering, identifier qualification, alias
   unification, and simple boolean simplifications don't cause spurious diffs.

2. CREATE TABLE: promote inline column-level PRIMARY KEY / UNIQUE constraints
   to out-of-line table-level constraints so that inline-vs-outline stylistic
   differences compare equal.

We deliberately do NOT:
  - Re-order composite index/constraint column lists (position matters).
  - Convert BYTE ↔ CHAR on VARCHAR2 (per project spec: no NLS auto-conversion).
"""
from __future__ import annotations

from sqlglot import exp
from sqlglot.optimizer import optimize

ORACLE = "oracle"


def rewrite(trees: list[exp.Expression]) -> list[exp.Expression]:
    return [_rewrite_one(t) for t in trees]


def _rewrite_one(tree: exp.Expression) -> exp.Expression:
    tree = tree.copy()
    tree = _promote_inline_pk_unique(tree)
    tree = _optimize_view_body(tree)
    return tree


# ---------------------------------------------------------------------------
# 1) Promote inline PK / UNIQUE to out-of-line table constraints.
#    NOT NULL is left inline — it is a column-level notion in Oracle and the
#    inline form is itself canonical.
# ---------------------------------------------------------------------------

_PROMOTABLE = (exp.PrimaryKeyColumnConstraint, exp.UniqueColumnConstraint)


def _promote_inline_pk_unique(tree: exp.Expression) -> exp.Expression:
    if not isinstance(tree, exp.Create):
        return tree
    schema = tree.this if isinstance(tree.this, exp.Schema) else None
    if schema is None or not schema.expressions:
        return tree

    new_children: list[exp.Expression] = []
    promoted: list[exp.Expression] = []

    for item in schema.expressions:
        if isinstance(item, exp.ColumnDef):
            cd, promoted_here = _split_inline_constraints(item)
            new_children.append(cd)
            promoted.extend(promoted_here)
        else:
            new_children.append(item)

    if promoted:
        schema.set("expressions", new_children + promoted)
    return tree


def _split_inline_constraints(col: exp.ColumnDef) -> tuple[exp.ColumnDef, list[exp.Expression]]:
    constraints = col.args.get("constraints") or []
    if not constraints:
        return col, []
    kept: list[exp.ColumnConstraint] = []
    promoted: list[exp.Expression] = []
    for cc in constraints:
        kind = cc.args.get("kind") if isinstance(cc, exp.ColumnConstraint) else None
        if kind and isinstance(kind, _PROMOTABLE):
            promoted.append(_make_table_constraint(col, kind))
        else:
            kept.append(cc)
    col.set("constraints", kept or None)
    return col, promoted


def _make_table_constraint(col: exp.ColumnDef, kind: exp.Expression) -> exp.Expression:
    col_id = col.args.get("this")
    col_ref = exp.Column(this=col_id.copy() if col_id is not None else None)
    if isinstance(kind, exp.PrimaryKeyColumnConstraint):
        return exp.PrimaryKey(expressions=[col_ref])
    if isinstance(kind, exp.UniqueColumnConstraint):
        # Represent an out-of-line UNIQUE as a Constraint wrapper whose body is
        # a column-level UniqueColumnConstraint attached to a synthetic column
        # list; sqlglot renders this as UNIQUE (col).
        return exp.Constraint(
            this=None,
            expressions=[exp.UniqueColumnConstraint(this=exp.Tuple(expressions=[col_ref]))],
        )
    raise AssertionError(f"unhandled promotable constraint: {type(kind).__name__}")


# ---------------------------------------------------------------------------
# 2) Optimize view bodies.
# ---------------------------------------------------------------------------

def _optimize_view_body(tree: exp.Expression) -> exp.Expression:
    if not isinstance(tree, exp.Create):
        return tree
    kind = (tree.args.get("kind") or "").upper()
    if kind != "VIEW":
        return tree
    body = tree.args.get("expression")
    if not isinstance(body, exp.Query):
        return tree
    try:
        optimized = optimize(body, dialect=ORACLE)
    except Exception:  # noqa: BLE001 — optimizer can fail on complex inputs; leave body as-is.
        return tree
    tree.set("expression", optimized)
    return tree

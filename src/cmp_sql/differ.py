from __future__ import annotations

from dataclasses import dataclass

from sqlglot import exp
from sqlglot.diff import Insert, Keep, Move, Remove, Update, diff as sqlglot_diff

from .types import ClassifiedEdit, EditCounts, Severity


# Node types whose change indicates a material (major) DDL difference.
_MAJOR_TYPES: tuple[type[exp.Expression], ...] = (
    exp.ColumnDef,
    exp.DataType,
    exp.DataTypeParam,
    exp.NotNullColumnConstraint,
    exp.PrimaryKey,
    exp.PrimaryKeyColumnConstraint,
    exp.ForeignKey,
    exp.UniqueColumnConstraint,
    exp.Check,
    exp.CheckColumnConstraint,
    exp.Reference,
    exp.DefaultColumnConstraint,
    exp.GeneratedAsIdentityColumnConstraint,
    exp.ComputedColumnConstraint,
    exp.Query,  # a changed view/select body
    exp.From,
    exp.Where,
    exp.Join,
    exp.Group,
    exp.Having,
    exp.Order,
    exp.Select,
)

# Types whose insertion/removal is merely minor — naming, collation, comments.
_MINOR_TYPES: tuple[type[exp.Expression], ...] = (
    exp.Constraint,       # bare wrapper — its *contents* carry the severity
    exp.CharacterSet,
    exp.Collate,
    exp.Comment,
    exp.Identifier,       # constraint/column rename (usually minor in DDL)
    exp.Var,
    exp.Literal,          # numeric tweak inside data type (e.g. VARCHAR2(100→101))
)


@dataclass
class DiffResult:
    edits: list[ClassifiedEdit]
    counts: EditCounts

    @property
    def has_non_cosmetic(self) -> bool:
        return self.counts.non_cosmetic() > 0


def diff_trees(src: list[exp.Expression], tgt: list[exp.Expression]) -> DiffResult:
    """Pairwise diff of two equal-length statement lists. If lengths differ,
    every extra statement on either side is reported as a major Insert/Remove.
    """
    counts = EditCounts()
    classified: list[ClassifiedEdit] = []

    # Align statements by index. Extra ones are whole-statement inserts/removes.
    common = min(len(src), len(tgt))
    for i in range(common):
        for edit in sqlglot_diff(src[i], tgt[i]):
            ce = _classify(edit, stmt_index=i)
            if ce is None:
                continue
            classified.append(ce)
            counts.add(ce.severity)

    for i in range(common, len(src)):
        ce = ClassifiedEdit(
            kind="Remove",
            severity=Severity.MAJOR,
            path=f"stmt[{i}]",
            summary=f"Entire statement only in source: {_short(src[i])}",
        )
        classified.append(ce)
        counts.add(ce.severity)

    for i in range(common, len(tgt)):
        ce = ClassifiedEdit(
            kind="Insert",
            severity=Severity.MAJOR,
            path=f"stmt[{i}]",
            summary=f"Entire statement only in target: {_short(tgt[i])}",
        )
        classified.append(ce)
        counts.add(ce.severity)

    return DiffResult(edits=classified, counts=counts)


def _classify(edit: object, *, stmt_index: int) -> ClassifiedEdit | None:
    if isinstance(edit, Keep):
        return None

    expression, source, target, kind = None, None, None, type(edit).__name__
    if isinstance(edit, (Insert, Remove)):
        expression = edit.expression
    elif isinstance(edit, Update):
        source, target = edit.source, edit.target
        expression = target or source
    elif isinstance(edit, Move):
        expression = getattr(edit, "source", None) or getattr(edit, "expression", None)

    severity = _severity_for(expression)
    summary = _summarize(kind, expression, source, target)
    path = f"stmt[{stmt_index}].{type(expression).__name__ if expression else '?'}"
    return ClassifiedEdit(kind=kind, severity=severity, path=path, summary=summary)


def _severity_for(node: exp.Expression | None) -> Severity:
    if node is None:
        return Severity.MINOR
    if isinstance(node, _MAJOR_TYPES):
        return Severity.MAJOR
    if isinstance(node, _MINOR_TYPES):
        return Severity.MINOR
    # Default: treat unknown structural nodes as major so we never silently
    # swallow a real change. False-positives → user lowers via future tuning.
    return Severity.MAJOR


def _summarize(kind: str, expression, source, target) -> str:
    if kind == "Update" and source is not None and target is not None:
        return f"Update {type(source).__name__}: {_short(source)} → {_short(target)}"
    return f"{kind} {type(expression).__name__}: {_short(expression)}"


def _short(node) -> str:
    if node is None:
        return "<none>"
    try:
        s = node.sql(dialect="oracle")
    except Exception:  # noqa: BLE001
        s = str(node)
    s = " ".join(s.split())
    return s if len(s) <= 80 else s[:77] + "..."

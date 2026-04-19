from pathlib import Path
import json
import shutil

import pytest

from cmp_sql.runner import run_all
from cmp_sql.types import Config, Status

FIX = Path(__file__).parent / "fixtures"


def _prepare(tmp_path: Path, *subsets: str) -> tuple[Path, Path, Path]:
    src = tmp_path / "src"
    tgt = tmp_path / "tgt"
    out = tmp_path / "out"
    src.mkdir()
    tgt.mkdir()
    for subset in subsets:
        sub_src = FIX / subset / "src"
        sub_tgt = FIX / subset / "tgt"
        if sub_src.is_dir():
            for f in sub_src.iterdir():
                shutil.copy(f, src / f.name)
        if sub_tgt.is_dir():
            for f in sub_tgt.iterdir():
                shutil.copy(f, tgt / f.name)
    return src, tgt, out


def _run(src: Path, tgt: Path, out: Path, mode: str = "semantic") -> dict:
    cfg = Config(source_dir=src, target_dir=tgt, out_dir=out, mode=mode, workers=1)
    return run_all(cfg)


def test_identical(tmp_path):
    src, tgt, out = _prepare(tmp_path, "identical")
    summary = _run(src, tgt, out)
    assert summary["totals"].get(Status.IDENTICAL.value) == 1


def test_normalized_match(tmp_path):
    src, tgt, out = _prepare(tmp_path, "normalized")
    summary = _run(src, tgt, out)
    assert summary["totals"].get(Status.NORMALIZED_MATCH.value) == 1


def test_semantic_match(tmp_path):
    src, tgt, out = _prepare(tmp_path, "semantic")
    summary = _run(src, tgt, out)
    # Either NORMALIZED_MATCH (lucky) or SEMANTIC_MATCH both indicate success;
    # the pair must not be different.
    totals = summary["totals"]
    assert totals.get(Status.DIFFERENT.value, 0) == 0
    assert (totals.get(Status.SEMANTIC_MATCH.value, 0) +
            totals.get(Status.NORMALIZED_MATCH.value, 0)) == 1


def test_different(tmp_path):
    src, tgt, out = _prepare(tmp_path, "different")
    summary = _run(src, tgt, out)
    assert summary["totals"].get(Status.DIFFERENT.value) == 1
    assert (out / "text" / "orders.sql.diff").exists()
    assert (out / "html" / "orders.sql.html").exists()


def test_missing(tmp_path):
    src, tgt, out = _prepare(tmp_path, "missing")
    summary = _run(src, tgt, out)
    assert summary["totals"].get(Status.MISSING_SRC.value) == 1
    assert summary["totals"].get(Status.MISSING_TGT.value) == 1
    missing = (out / "missing.log").read_text()
    assert "only_in_src.sql" in missing
    assert "only_in_tgt.sql" in missing


def test_summary_json_shape(tmp_path):
    src, tgt, out = _prepare(tmp_path, "different", "identical")
    _run(src, tgt, out)
    summary = json.loads((out / "summary.json").read_text())
    assert "totals" in summary and "files" in summary
    assert {"name", "status", "edits"} <= set(summary["files"][0])

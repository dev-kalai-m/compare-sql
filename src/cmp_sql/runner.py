from __future__ import annotations

import json
import multiprocessing as mp
import os
import time
from datetime import datetime, timezone
from pathlib import Path

from tqdm import tqdm

from .differ import diff_trees
from .normalizer import normalize, render
from .pairing import FilePair, discover_pairs
from .parser import parse_sql
from .reporter_html import write_html_report
from .reporter_text import write_text_report
from .semantic import rewrite
from .types import Config, PairResult, Severity, Status

SENTINEL_RAW = 0
SENTINEL_NORMALIZED = 1
SENTINEL_SEMANTIC = 2


# --------------------------------------------------------------------------- #
# Per-pair comparison (pure function; runs in worker process)
# --------------------------------------------------------------------------- #

def compare_pair(pair: FilePair, cfg_dict: dict) -> PairResult:
    cfg = _cfg_from_dict(cfg_dict)
    start = time.perf_counter()
    result = _compare_pair_inner(pair, cfg)
    result.duration_ms = (time.perf_counter() - start) * 1000
    _write_outputs(result, pair, cfg)
    return result


def _compare_pair_inner(pair: FilePair, cfg: Config) -> PairResult:
    name = pair.name

    # Missing-file short circuits
    if pair.src is None:
        return PairResult(name=name, status=Status.MISSING_SRC,
                          src_path=None, tgt_path=pair.tgt)
    if pair.tgt is None:
        return PairResult(name=name, status=Status.MISSING_TGT,
                          src_path=pair.src, tgt_path=None)

    src_text = pair.src.read_text(encoding="utf-8", errors="replace")
    tgt_text = pair.tgt.read_text(encoding="utf-8", errors="replace")

    # Byte-identical raw → done.
    if src_text.strip() == tgt_text.strip():
        return PairResult(name=name, status=Status.IDENTICAL,
                          src_path=pair.src, tgt_path=pair.tgt)

    psrc = parse_sql(src_text)
    ptgt = parse_sql(tgt_text)

    # Handle parse-error / text-fallback combinations.
    if not psrc.ok or not ptgt.ok:
        return _text_fallback_result(pair, psrc, ptgt)

    src_trees = psrc.expressions
    tgt_trees = ptgt.expressions

    # Normalized comparison by rendered SQL equality.
    n_src = normalize(
        src_trees,
        ignore_storage=cfg.ignore_storage,
        ignore_column_order=cfg.ignore_column_order,
    )
    n_tgt = normalize(
        tgt_trees,
        ignore_storage=cfg.ignore_storage,
        ignore_column_order=cfg.ignore_column_order,
    )
    r_src = render(n_src)
    r_tgt = render(n_tgt)
    if r_src == r_tgt:
        return PairResult(name=name, status=Status.NORMALIZED_MATCH,
                          src_path=pair.src, tgt_path=pair.tgt)

    if cfg.mode in ("semantic",):
        s_src = rewrite(n_src)
        s_tgt = rewrite(n_tgt)
        rs_src = render(s_src)
        rs_tgt = render(s_tgt)
        if rs_src == rs_tgt:
            return PairResult(name=name, status=Status.SEMANTIC_MATCH,
                              src_path=pair.src, tgt_path=pair.tgt)
        # Real diff — classify on the semantic-rewritten trees so reports
        # describe the smallest residual difference.
        dr = diff_trees(s_src, s_tgt)
        status = Status.DIFFERENT if dr.counts.non_cosmetic() > 0 else Status.NORMALIZED_MATCH
        pr = PairResult(name=name, status=status,
                        src_path=pair.src, tgt_path=pair.tgt,
                        edits=dr.counts, classified=dr.edits)
        pr._rendered = (rs_src, rs_tgt)  # type: ignore[attr-defined]
        return pr

    # mode == normalized (no semantic rewrite): classify on normalized trees.
    dr = diff_trees(n_src, n_tgt)
    status = Status.DIFFERENT if dr.counts.non_cosmetic() > 0 else Status.NORMALIZED_MATCH
    pr = PairResult(name=name, status=status,
                    src_path=pair.src, tgt_path=pair.tgt,
                    edits=dr.counts, classified=dr.edits)
    pr._rendered = (r_src, r_tgt)  # type: ignore[attr-defined]
    return pr


def _text_fallback_result(pair: FilePair, psrc, ptgt) -> PairResult:
    if not psrc.ok and not ptgt.ok:
        status = Status.PARSE_ERROR_BOTH
    elif not psrc.ok:
        status = Status.PARSE_ERROR_SRC
    else:
        status = Status.PARSE_ERROR_TGT

    # Use the normalized text from each side (or raw if the other parsed ok).
    src_text = psrc.fallback_text if not psrc.ok else pair.src.read_text(
        encoding="utf-8", errors="replace"
    )
    tgt_text = ptgt.fallback_text if not ptgt.ok else pair.tgt.read_text(
        encoding="utf-8", errors="replace"
    )

    # If both fall back and text-normalized content matches, mark as NORMALIZED_MATCH
    # (we still flag text_fallback so the user knows).
    if (not psrc.ok and not ptgt.ok) and src_text == tgt_text:
        pr = PairResult(name=pair.name, status=Status.NORMALIZED_MATCH,
                        src_path=pair.src, tgt_path=pair.tgt,
                        text_fallback=True,
                        parse_error_src=psrc.error,
                        parse_error_tgt=ptgt.error)
        pr._rendered = (src_text, tgt_text)  # type: ignore[attr-defined]
        return pr

    pr = PairResult(name=pair.name, status=status,
                    src_path=pair.src, tgt_path=pair.tgt,
                    text_fallback=True,
                    parse_error_src=psrc.error,
                    parse_error_tgt=ptgt.error)
    # Major severity flag so the pair appears in "different" views.
    pr.edits.major = 1
    pr._rendered = (src_text, tgt_text)  # type: ignore[attr-defined]
    return pr


# --------------------------------------------------------------------------- #
# Output writing
# --------------------------------------------------------------------------- #

def _write_outputs(result: PairResult, pair: FilePair, cfg: Config) -> None:
    if result.status in (Status.MISSING_SRC, Status.MISSING_TGT):
        return  # recorded in missing.log by orchestrator

    rendered = getattr(result, "_rendered", None)
    if rendered is None:
        # identical / normalized_match / semantic_match paths — skip detail reports.
        if result.status == Status.IDENTICAL:
            return
        # Safety: render raw texts so the report is still usable.
        src_text = pair.src.read_text(encoding="utf-8", errors="replace") if pair.src else ""
        tgt_text = pair.tgt.read_text(encoding="utf-8", errors="replace") if pair.tgt else ""
        rendered = (src_text, tgt_text)
    src_rendered, tgt_rendered = rendered

    text_path = cfg.out_dir / "text" / f"{result.name}.diff"
    write_text_report(text_path, result, src_rendered, tgt_rendered)

    html_needed = cfg.html_for == "all" or result.status not in (Status.IDENTICAL,)
    if cfg.html_for == "non-identical" and result.status in (
        Status.NORMALIZED_MATCH, Status.SEMANTIC_MATCH,
    ):
        # per spec: render HTML only when there's a real diff to look at.
        html_needed = False
    if html_needed:
        html_path = cfg.out_dir / "html" / f"{result.name}.html"
        write_html_report(html_path, result, src_rendered, tgt_rendered)


# --------------------------------------------------------------------------- #
# Orchestrator
# --------------------------------------------------------------------------- #

def run_all(cfg: Config) -> dict:
    cfg.out_dir.mkdir(parents=True, exist_ok=True)
    (cfg.out_dir / "text").mkdir(exist_ok=True)
    (cfg.out_dir / "html").mkdir(exist_ok=True)

    pairs = list(discover_pairs(cfg.source_dir, cfg.target_dir))
    missing_log = cfg.out_dir / "missing.log"
    missing_log.write_text("", encoding="utf-8")

    workers = cfg.workers or os.cpu_count() or 1
    cfg_dict = _cfg_to_dict(cfg)

    results: list[PairResult] = []
    if workers == 1 or len(pairs) <= 1:
        it = (compare_pair(p, cfg_dict) for p in pairs)
        results = list(tqdm(it, total=len(pairs), desc="compare", unit="pair"))
    else:
        ctx = mp.get_context("spawn")
        with ctx.Pool(processes=workers) as pool:
            args = [(p, cfg_dict) for p in pairs]
            for r in tqdm(pool.imap_unordered(_compare_star, args, chunksize=25),
                          total=len(args), desc="compare", unit="pair"):
                results.append(r)

    # Missing-file log.
    ts = datetime.now(timezone.utc).isoformat()
    with missing_log.open("a", encoding="utf-8") as f:
        for r in results:
            if r.status is Status.MISSING_SRC:
                f.write(f"{r.name}\tsource\t{ts}\n")
            elif r.status is Status.MISSING_TGT:
                f.write(f"{r.name}\ttarget\t{ts}\n")

    summary = _build_summary(results)
    (cfg.out_dir / "summary.json").write_text(
        json.dumps(summary, indent=2), encoding="utf-8"
    )
    return summary


def _compare_star(args):
    pair, cfg_dict = args
    return compare_pair(pair, cfg_dict)


def _build_summary(results: list[PairResult]) -> dict:
    totals: dict[str, int] = {}
    for r in results:
        totals[r.status.value] = totals.get(r.status.value, 0) + 1
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "totals": totals,
        "files": sorted((r.as_dict() for r in results), key=lambda d: d["name"]),
    }


# --------------------------------------------------------------------------- #
# Config serialization for multiprocessing (Path objects need str-ification)
# --------------------------------------------------------------------------- #

def _cfg_to_dict(cfg: Config) -> dict:
    return {
        "source_dir": str(cfg.source_dir),
        "target_dir": str(cfg.target_dir),
        "out_dir": str(cfg.out_dir),
        "mode": cfg.mode,
        "ignore_storage": cfg.ignore_storage,
        "ignore_column_order": cfg.ignore_column_order,
        "workers": cfg.workers,
        "html_for": cfg.html_for,
        "timeout_seconds": cfg.timeout_seconds,
    }


def _cfg_from_dict(d: dict) -> Config:
    return Config(
        source_dir=Path(d["source_dir"]),
        target_dir=Path(d["target_dir"]),
        out_dir=Path(d["out_dir"]),
        mode=d["mode"],
        ignore_storage=d["ignore_storage"],
        ignore_column_order=d["ignore_column_order"],
        workers=d["workers"],
        html_for=d["html_for"],
        timeout_seconds=d["timeout_seconds"],
    )

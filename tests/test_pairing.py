from pathlib import Path

from cmp_sql.pairing import discover_pairs

FIX = Path(__file__).parent / "fixtures"


def test_pair_complete():
    pairs = list(discover_pairs(FIX / "identical" / "src", FIX / "identical" / "tgt"))
    assert len(pairs) == 1
    assert pairs[0].name == "emp.sql"
    assert pairs[0].is_complete


def test_missing_sides():
    pairs = list(discover_pairs(FIX / "missing" / "src", FIX / "missing" / "tgt"))
    names = sorted(p.name for p in pairs)
    assert names == ["only_in_src.sql", "only_in_tgt.sql"]
    src_only = next(p for p in pairs if p.name == "only_in_src.sql")
    tgt_only = next(p for p in pairs if p.name == "only_in_tgt.sql")
    assert src_only.src is not None and src_only.tgt is None
    assert tgt_only.src is None and tgt_only.tgt is not None


def test_nonexistent_dir_is_empty():
    pairs = list(discover_pairs(FIX / "nope_a", FIX / "nope_b"))
    assert pairs == []

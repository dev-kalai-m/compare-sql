from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterator


@dataclass(frozen=True)
class FilePair:
    name: str
    src: Path | None
    tgt: Path | None

    @property
    def is_complete(self) -> bool:
        return self.src is not None and self.tgt is not None


def _index(folder: Path) -> dict[str, Path]:
    if not folder.is_dir():
        return {}
    return {p.name: p for p in folder.iterdir() if p.is_file() and p.suffix.lower() == ".sql"}


def discover_pairs(source_dir: Path, target_dir: Path) -> Iterator[FilePair]:
    """Yield all FilePair entries (complete + one-sided) for files named *.sql."""
    src_idx = _index(source_dir)
    tgt_idx = _index(target_dir)
    all_names = sorted(set(src_idx) | set(tgt_idx))
    for name in all_names:
        yield FilePair(name=name, src=src_idx.get(name), tgt=tgt_idx.get(name))

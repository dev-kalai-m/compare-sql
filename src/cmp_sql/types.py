from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path


class Status(str, Enum):
    IDENTICAL = "identical"
    NORMALIZED_MATCH = "normalized_match"
    SEMANTIC_MATCH = "semantic_match"
    DIFFERENT = "different"
    PARSE_ERROR_SRC = "parse_error_src"
    PARSE_ERROR_TGT = "parse_error_tgt"
    PARSE_ERROR_BOTH = "parse_error_both"
    MISSING_SRC = "missing_src"
    MISSING_TGT = "missing_tgt"
    TIMEOUT = "timeout"


class Severity(str, Enum):
    COSMETIC = "cosmetic"
    MINOR = "minor"
    MAJOR = "major"


@dataclass
class ClassifiedEdit:
    kind: str            # Insert | Remove | Update | Move
    severity: Severity
    path: str            # dotted AST path, e.g. CREATE.SCHEMA.TABLE.COLUMN[3]
    summary: str         # short human description


@dataclass
class EditCounts:
    major: int = 0
    minor: int = 0
    cosmetic: int = 0

    def add(self, sev: Severity) -> None:
        if sev is Severity.MAJOR:
            self.major += 1
        elif sev is Severity.MINOR:
            self.minor += 1
        else:
            self.cosmetic += 1

    def non_cosmetic(self) -> int:
        return self.major + self.minor

    def as_dict(self) -> dict[str, int]:
        return {"major": self.major, "minor": self.minor, "cosmetic": self.cosmetic}


@dataclass
class PairResult:
    name: str
    status: Status
    src_path: Path | None
    tgt_path: Path | None
    edits: EditCounts = field(default_factory=EditCounts)
    classified: list[ClassifiedEdit] = field(default_factory=list)
    text_fallback: bool = False
    parse_error_src: str | None = None
    parse_error_tgt: str | None = None
    duration_ms: float = 0.0

    def as_dict(self) -> dict:
        return {
            "name": self.name,
            "status": self.status.value,
            "edits": self.edits.as_dict(),
            "text_fallback": self.text_fallback,
            "parse_error_src": self.parse_error_src,
            "parse_error_tgt": self.parse_error_tgt,
            "duration_ms": round(self.duration_ms, 2),
        }


@dataclass
class Config:
    source_dir: Path
    target_dir: Path
    out_dir: Path
    mode: str = "semantic"           # strict | normalized | semantic
    ignore_storage: bool = True
    ignore_column_order: bool = True
    workers: int = 0                 # 0 → cpu_count
    html_for: str = "non-identical"  # non-identical | all
    timeout_seconds: float = 5.0

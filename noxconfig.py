from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import (
    Iterable,
)


@dataclass(frozen=True)
class Config:
    root: Path = Path(__file__).parent
    doc: Path = Path(__file__).parent / "doc"
    version_file: Path = (
        Path(__file__).parent / "pyexasol" / "version.py"
    )
    path_filters: Iterable[str] = (
        "dist",
        ".eggs",
        "venv",
    )

    plugins = []


PROJECT_CONFIG = Config()

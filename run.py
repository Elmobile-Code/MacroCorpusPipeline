#!/usr/bin/env python3
"""Download every BLS LABSTAT catalog and write one JSON file per series."""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
src = ROOT / "src"
if str(src) not in sys.path:
    sys.path.insert(0, str(src))

from bls_corpus.pipeline import main


if __name__ == "__main__":
    raise SystemExit(main())

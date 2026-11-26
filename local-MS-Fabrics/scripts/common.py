"""Shared paths/helpers."""
import os
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = Path(os.environ.get("HG_DATA_DIR", ROOT / "data")).resolve()
LANDING = DATA_DIR / "landing"
BRONZE = DATA_DIR / "bronze"
SILVER = DATA_DIR / "silver"
GOLD = DATA_DIR / "gold"
OPS = DATA_DIR / "ops"
DUCKDB_PATH = DATA_DIR / "lakehouse.duckdb"

"""
Normalize columns: map client column names to our snake_case per column_mapping JSON.
Lambda equivalent: Normalize columns.
Input: rows (list of dicts), optional mapping override.
Output: list of dicts with normalized keys.
"""
import json
import os
import re

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CONFIG_PATH = os.path.join(ROOT, "config", "column_mapping.json")


def load_mapping() -> dict:
    with open(CONFIG_PATH, encoding="utf-8") as f:
        data = json.load(f)
    return data.get("mappings", data)


def _to_snake(s: str) -> str:
    s = re.sub(r"[^\w\s]", "", s)
    return "_".join(s.lower().split())


def normalize_row(row: dict, mapping: dict) -> dict:
    """Apply column mapping; unmapped keys converted to snake_case."""
    out = {}
    for k, v in row.items():
        key = (k.strip() if isinstance(k, str) else k) or ""
        mapped = mapping.get(key) or _to_snake(key) if key else None
        if mapped:
            out[mapped] = v
    return out


def normalize(rows: list[dict], mapping: dict = None) -> list[dict]:
    """Normalize all rows. Uses config column_mapping.json if mapping not provided."""
    mapping = mapping or load_mapping()
    return [normalize_row(r, mapping) for r in rows]

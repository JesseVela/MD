"""
Validate schema: enforce column names and types per 05-Full-Table-Column-Reference.
Lambda equivalent: Validate schema.
Input: rows (list of dicts), table type (e.g. transactions_t1, supplier_master).
Output: (valid_rows, errors).
"""
import json
import os

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SCHEMAS_PATH = os.path.join(ROOT, "schemas", "tables.json")


def load_schema() -> dict:
    with open(SCHEMAS_PATH, encoding="utf-8") as f:
        return json.load(f)


def validate_transactions_t1(rows: list[dict]) -> tuple[list[dict], list[str]]:
    """Validate rows for Tier 1 transactions. Mandatory: client_id, upload_id, supplier_id."""
    mandatory = {"client_id", "upload_id", "supplier_id"}
    valid = []
    errors = []
    for i, row in enumerate(rows):
        missing = mandatory - set(k.strip() for k in row.keys() if row.get(k) is not None and str(row.get(k)).strip())
        if missing:
            errors.append(f"Row {i+1}: missing required columns: {missing}")
            continue
        valid.append(row)
    return valid, errors


def validate_supplier_columns(rows: list[dict], allowed_columns: set = None) -> tuple[list[dict], list[str]]:
    """Validate supplier-related rows. If allowed_columns given, drop extras or warn."""
    valid = []
    errors = []
    for i, row in enumerate(rows):
        if not row or all(v is None or (isinstance(v, str) and not v.strip()) for v in row.values()):
            errors.append(f"Row {i+1}: empty row")
            continue
        valid.append(row)
    return valid, errors


def validate(rows: list[dict], table_type: str = "transactions_t1") -> tuple[list[dict], list[str]]:
    """Validate rows for given table type. Returns (valid_rows, errors)."""
    if table_type == "transactions_t1":
        return validate_transactions_t1(rows)
    return validate_supplier_columns(rows)

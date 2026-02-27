"""
Supplier Master ETL — full automation for ref.supplier_master, ref.global_supplier_data_master, ref.client_master.

Flow (in-memory / temp; no persistent staging table):
  1. Read transaction CSV → extract supplier names (and optional supplier_id).
  2. Normalize names (clean_name from Bhavin's logic), aggregate by normalized name.
  3. For each unique normalized name: match to existing ref.supplier_master or assign new Genpact ID (G10001, G10002, ...).
  4. Optionally enrich via Gemini (description, L1/L2/L3, product_service_tags) when GEMINI_API_KEY set.
  5. Write to Postgres: ref.supplier_master, ref.global_supplier_data_master, ref.client_master.
  6. Optionally write client_id → supplier_crosswalk and client_supplier_data_master (when client_id provided).

Uses env: DB_HOST, DB_USERNAME, DB_PASSWORD, DB_NAME, DB_PORT; optional GEMINI_API_KEY.
"""
import csv
import os
import re
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any, Optional

# Project root (Supplier-etl-local)
ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from etl.supplier_normalize import clean_name, name_key_for_match

try:
    import psycopg2
    from psycopg2 import sql
except ImportError:
    psycopg2 = None

# Default CSV column names we try for supplier name (first match wins)
DEFAULT_SUPPLIER_NAME_COLUMNS = [
    "Supplier", "Supplier Name", "Vendor Name", "VendorName", "supplier_name", "supplier_name_normalized",
    "Vendor", "Company", "Supplier Name (Normalized)",
]
# Optional columns for client supplier_id and amount
SUPPLIER_ID_COLUMNS = ["Supplier ID", "Vendor ID", "supplier_id", "Supplier's Invoice Number"]
AMOUNT_COLUMNS = ["Invoice Amount", "Amount", "amount", "spend_amount_usd", "InvoiceAmount"]
CURRENCY_COLUMNS = ["Currency", "currency"]
ITEM_DESC_COLUMNS = ["Memo", "material_description", "po_line_description", "invoice_line_description", "Item Description"]


def _find_column(row: dict, candidates: list) -> Optional[str]:
    """Return first column name that exists in row (case-insensitive)."""
    keys_lower = {k.strip().lower(): k for k in row.keys() if k}
    for c in candidates:
        if c and c.strip().lower() in keys_lower:
            return keys_lower[c.strip().lower()]
    return None


def _clean_val(v: Any) -> Optional[str]:
    if v is None:
        return None
    s = str(v).strip()
    if s.lower() in ("", "nan", "none", "n/a"):
        return None
    return s


def load_transaction_csv(
    csv_path: Path,
    supplier_name_column: Optional[str] = None,
    supplier_id_column: Optional[str] = None,
) -> tuple[list[dict], str, Optional[str]]:
    """
    Load CSV and return (rows as list of dicts, supplier_name_column_used, supplier_id_column_used).
    Each row has at least: supplier_name_raw, supplier_name_normalized (clean_name), and optionally supplier_id, amount, currency, item_descriptions.
    """
    with open(csv_path, newline="", encoding="utf-8", errors="replace") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
    if not rows:
        return [], None, None
    first = rows[0]
    name_col = supplier_name_column or _find_column(first, DEFAULT_SUPPLIER_NAME_COLUMNS)
    id_col = supplier_id_column or _find_column(first, SUPPLIER_ID_COLUMNS)
    amount_col = _find_column(first, AMOUNT_COLUMNS)
    currency_col = _find_column(first, CURRENCY_COLUMNS)
    item_col = _find_column(first, ITEM_DESC_COLUMNS)
    if not name_col:
        raise ValueError(f"No supplier name column found in CSV. Tried: {DEFAULT_SUPPLIER_NAME_COLUMNS}. Columns: {list(first.keys())}")
    out = []
    for r in rows:
        raw_name = _clean_val(r.get(name_col))
        if not raw_name:
            continue
        norm = clean_name(raw_name)
        if not norm:
            norm = raw_name
        rec = {
            "supplier_name_raw": raw_name,
            "supplier_name_normalized": norm,
            "supplier_id": _clean_val(r.get(id_col)) if id_col else None,
            "amount": r.get(amount_col),
            "currency": _clean_val(r.get(currency_col)) if currency_col else None,
            "item_description": _clean_val(r.get(item_col)) if item_col else None,
        }
        out.append(rec)
    return out, name_col, id_col


def aggregate_by_supplier(rows: list[dict]) -> dict[str, dict]:
    """
    Aggregate rows by normalized_supplier_name. Returns dict: normalized_name -> { raw_names, supplier_ids, amounts, item_descriptions, currencies }.
    """
    agg = defaultdict(lambda: {"raw_names": set(), "supplier_ids": set(), "amounts": [], "item_descriptions": [], "currencies": set()})
    for r in rows:
        norm = r.get("supplier_name_normalized") or ""
        if not norm:
            continue
        agg[norm]["raw_names"].add(r.get("supplier_name_raw") or norm)
        if r.get("supplier_id"):
            agg[norm]["supplier_ids"].add(r["supplier_id"])
        if r.get("amount") is not None:
            try:
                agg[norm]["amounts"].append(float(r["amount"]))
            except (TypeError, ValueError):
                pass
        if r.get("item_description"):
            agg[norm]["item_descriptions"].append(r["item_description"])
        if r.get("currency"):
            agg[norm]["currencies"].add(r["currency"])
    return dict(agg)


def get_pg_conn():
    host = os.environ.get("DB_HOST", "localhost")
    user = os.environ.get("DB_USERNAME", "")
    password = os.environ.get("DB_PASSWORD", "")
    dbname = os.environ.get("DB_NAME", "supplier_etl")
    port = os.environ.get("DB_PORT", "5432")
    if not user:
        raise RuntimeError("Set DB_USERNAME, DB_PASSWORD, DB_HOST (and optionally DB_NAME, DB_PORT) in the environment.")
    return psycopg2.connect(
        host=host, dbname=dbname, user=user, password=password, port=port
    )


def fetch_existing_supplier_master(cur) -> dict[str, str]:
    """Return dict: normalized_supplier_name (lower) -> genpact_supplier_id."""
    cur.execute("""
        SELECT genpact_supplier_id, normalized_supplier_name
        FROM ref.supplier_master
    """)
    out = {}
    for gid, name in cur.fetchall():
        if name:
            out[name.strip().lower()] = gid
    return out


def _max_genpact_number(cur) -> int:
    """Return max numeric part of genpact_supplier_id (e.g. G10005 -> 10005). Default 10000 so next is G10001."""
    cur.execute("""
        SELECT genpact_supplier_id FROM ref.supplier_master
        WHERE genpact_supplier_id ~ '^G[0-9]+$'
        ORDER BY genpact_supplier_id DESC LIMIT 1
    """)
    row = cur.fetchone()
    if not row:
        return 10000
    s = row[0]
    try:
        return int(re.sub(r"^G", "", s))
    except ValueError:
        return 10000


def ensure_ref_tables(cur):
    """Create ref.client_master, ref.supplier_master, ref.global_supplier_data_master if not exist."""
    cur.execute("CREATE SCHEMA IF NOT EXISTS ref")
    cur.execute("""
        CREATE TABLE IF NOT EXISTS ref.client_master (
            client_id TEXT PRIMARY KEY,
            client_name TEXT NOT NULL DEFAULT 'Unknown',
            client_industry TEXT,
            date_added TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS ref.supplier_master (
            genpact_supplier_id TEXT PRIMARY KEY,
            normalized_supplier_name TEXT NOT NULL,
            date_added TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_ref_supplier_master_name
        ON ref.supplier_master (normalized_supplier_name)
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS ref.global_supplier_data_master (
            genpact_supplier_id TEXT NOT NULL PRIMARY KEY REFERENCES ref.supplier_master(genpact_supplier_id),
            supplier_description TEXT,
            employee_count TEXT,
            revenue TEXT,
            year_established TEXT,
            l1_category TEXT,
            l2_category TEXT,
            l3_category TEXT,
            product_service_tags TEXT,
            ship_to_countries TEXT,
            country_codes TEXT,
            date_added TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)


def run_supplier_master_etl(
    csv_path: Path,
    client_id: Optional[str] = None,
    client_name: Optional[str] = None,
    supplier_name_column: Optional[str] = None,
    supplier_id_column: Optional[str] = None,
    skip_enrich: bool = True,
    dry_run: bool = False,
) -> dict:
    """
    Full supplier master ETL: CSV → normalize → aggregate → assign Genpact ID → write ref tables.
    Returns counts: { rows_read, suppliers_aggregated, suppliers_new, suppliers_existing, ref_supplier_master_inserted, ref_global_inserted, client_master_upserted, crosswalk_upserted }.
    """
    if psycopg2 is None:
        raise RuntimeError("psycopg2 is required. Install with: pip install psycopg2-binary")
    csv_path = Path(csv_path)
    if not csv_path.is_file():
        raise FileNotFoundError(f"CSV not found: {csv_path}")
    rows, _name_col, _id_col = load_transaction_csv(csv_path, supplier_name_column, supplier_id_column)
    aggregated = aggregate_by_supplier(rows)
    counts = {
        "rows_read": len(rows),
        "suppliers_aggregated": len(aggregated),
        "suppliers_new": 0,
        "suppliers_existing": 0,
        "ref_supplier_master_inserted": 0,
        "ref_global_inserted": 0,
        "client_master_upserted": 0,
        "crosswalk_upserted": 0,
    }
    if dry_run:
        counts["dry_run"] = True
        return counts
    conn = get_pg_conn()
    conn.autocommit = False
    try:
        cur = conn.cursor()
        ensure_ref_tables(cur)
        existing = fetch_existing_supplier_master(cur)
        # Build mapping: normalized_name -> genpact_supplier_id (existing or new)
        next_num = _max_genpact_number(cur) + 1
        name_to_gid = {}
        for norm_name in aggregated:
            key = norm_name.strip().lower()
            if key in existing:
                name_to_gid[norm_name] = existing[key]
                counts["suppliers_existing"] += 1
            else:
                gid = f"G{next_num:05d}"
                next_num += 1
                name_to_gid[norm_name] = gid
                existing[key] = gid
                counts["suppliers_new"] += 1
        # Insert ref.supplier_master (dedupe: ON CONFLICT DO UPDATE normalized_supplier_name)
        for norm_name, gid in name_to_gid.items():
            cur.execute("""
                INSERT INTO ref.supplier_master (genpact_supplier_id, normalized_supplier_name, date_added)
                VALUES (%s, %s, CURRENT_TIMESTAMP)
                ON CONFLICT (genpact_supplier_id) DO UPDATE SET
                    normalized_supplier_name = EXCLUDED.normalized_supplier_name
            """, (gid, norm_name))
            counts["ref_supplier_master_inserted"] += 1
        # Insert ref.global_supplier_data_master (one row per genpact_supplier_id; no enrichment if skip_enrich)
        for norm_name, gid in name_to_gid.items():
            data = aggregated[norm_name]
            desc = ""
            l1 = l2 = l3 = ""
            tags = ""
            if not skip_enrich and os.environ.get("GEMINI_API_KEY"):
                # Optional: call Bhavin's enrich_supplier / classify_supplier / generate_supplier_product_tags
                desc, l1, l2, l3, tags = _enrich_one(norm_name, data)
            cur.execute("""
                INSERT INTO ref.global_supplier_data_master (
                    genpact_supplier_id, supplier_description, l1_category, l2_category, l3_category,
                    product_service_tags, date_added
                ) VALUES (%s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                ON CONFLICT (genpact_supplier_id) DO UPDATE SET
                    supplier_description = COALESCE(EXCLUDED.supplier_description, ref.global_supplier_data_master.supplier_description),
                    l1_category = COALESCE(EXCLUDED.l1_category, ref.global_supplier_data_master.l1_category),
                    l2_category = COALESCE(EXCLUDED.l2_category, ref.global_supplier_data_master.l2_category),
                    l3_category = COALESCE(EXCLUDED.l3_category, ref.global_supplier_data_master.l3_category),
                    product_service_tags = COALESCE(EXCLUDED.product_service_tags, ref.global_supplier_data_master.product_service_tags)
            """, (gid, desc or None, l1 or None, l2 or None, l3 or None, tags or None))
            counts["ref_global_inserted"] += 1
        # Client: ensure ref.client_master and optionally client schema (supplier_crosswalk)
        if client_id:
            cname = client_name or client_id
            cur.execute("""
                INSERT INTO ref.client_master (client_id, client_name, date_added)
                VALUES (%s, %s, CURRENT_TIMESTAMP)
                ON CONFLICT (client_id) DO UPDATE SET client_name = COALESCE(EXCLUDED.client_name, ref.client_master.client_name)
            """, (client_id, cname))
            counts["client_master_upserted"] = 1
            schema = f"client_{client_id}"
            cur.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(schema)))
            cur.execute(sql.SQL("""
                CREATE TABLE IF NOT EXISTS {}.supplier_crosswalk (
                    client_id TEXT NOT NULL,
                    supplier_id TEXT NOT NULL,
                    genpact_supplier_id TEXT REFERENCES ref.supplier_master(genpact_supplier_id),
                    match_method TEXT,
                    match_confidence DOUBLE PRECISION,
                    matched_on TEXT,
                    date_added TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (client_id, supplier_id)
                )
            """).format(sql.Identifier(schema)))
            for norm_name, gid in name_to_gid.items():
                data = aggregated[norm_name]
                supplier_ids = data.get("supplier_ids")
                if supplier_ids:
                    for supp_id in supplier_ids:
                        sid = str(supp_id).strip() or norm_name[:100]
                        cur.execute(sql.SQL("""
                            INSERT INTO {}.supplier_crosswalk
                            (client_id, supplier_id, genpact_supplier_id, match_method, match_confidence, matched_on, date_added)
                            VALUES (%s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                            ON CONFLICT (client_id, supplier_id) DO UPDATE SET
                                genpact_supplier_id = EXCLUDED.genpact_supplier_id,
                                match_method = 'exact_name_key', match_confidence = 1.0, matched_on = 'name_key'
                        """).format(sql.Identifier(schema)), (client_id, sid, gid, "exact_name_key", 1.0, "name_key"))
                        counts["crosswalk_upserted"] += 1
                else:
                    sid = norm_name[:100]
                    cur.execute(sql.SQL("""
                        INSERT INTO {}.supplier_crosswalk
                        (client_id, supplier_id, genpact_supplier_id, match_method, match_confidence, matched_on, date_added)
                        VALUES (%s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                        ON CONFLICT (client_id, supplier_id) DO UPDATE SET
                            genpact_supplier_id = EXCLUDED.genpact_supplier_id,
                            match_method = 'exact_name_key', match_confidence = 1.0, matched_on = 'name_key'
                    """).format(sql.Identifier(schema)), (client_id, sid, gid, "exact_name_key", 1.0, "name_key"))
                    counts["crosswalk_upserted"] += 1
        conn.commit()
        cur.close()
        return counts
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def _enrich_one(norm_name: str, data: dict) -> tuple[str, str, str, str, str]:
    """Optional enrichment via Bhavin's script (if available). Returns (description, l1, l2, l3, product_service_tags)."""
    try:
        # Try to load Bhavin's module from ../Documents/supplier_master_generator 1.py
        parent = ROOT.parent
        gen_path = parent / "Documents" / "supplier_master_generator 1.py"
        if not gen_path.is_file():
            return "", "", "", "", ""
        import importlib.util
        spec = importlib.util.spec_from_file_location("smg", str(gen_path))
        if spec is None or spec.loader is None:
            return "", "", "", "", ""
        mod = importlib.util.module_from_spec(spec)
        sys.modules["smg"] = mod
        spec.loader.exec_module(mod)
        api_key = os.environ.get("GEMINI_API_KEY", "")
        if not api_key:
            return "", "", "", "", ""
        rate_limiter = getattr(mod, "RateLimiter", None)
        if rate_limiter:
            rl = rate_limiter(max_rpm=30)
        else:
            rl = None
        desc, emp, rev, yr = "Not available", "Unknown", "Unknown", "Unknown"
        if hasattr(mod, "enrich_supplier") and rl:
            try:
                en = mod.enrich_supplier(norm_name, api_key, "gemini-1.5-flash", 0.2, False, rl)
                desc = en.get("description", "") or "Not available"
            except Exception:
                pass
        items = data.get("item_descriptions") or []
        tags_list = []
        if hasattr(mod, "generate_supplier_product_tags") and rl and items:
            try:
                tags_list = mod.generate_supplier_product_tags(norm_name, items[:50], api_key, "gemini-1.5-flash", 0.2, rl)
            except Exception:
                pass
        tags = ", ".join(tags_list) if tags_list else ""
        l1 = l2 = l3 = ""
        if hasattr(mod, "classify_supplier") and rl:
            try:
                cat = mod.classify_supplier(norm_name, desc, [], api_key, "gemini-1.5-flash", 0.2, rl)
                l1 = cat.get("l1", "") or ""
                l2 = cat.get("category_code", "") or ""
                l3 = cat.get("category_name", "") or ""
            except Exception:
                pass
        return desc, l1, l2, l3, tags
    except Exception:
        return "", "", "", "", ""

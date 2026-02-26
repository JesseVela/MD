"""
Export sample ref and client tables to CSV aligned with Excel-Example-CSVs shared with Kristaq/Bhavin.
Column order and date format (YYYY-MM-DD) match Meeting-Prep/02,-FEB/Excel-Example-CSVs.
Run from project root: python scripts/export_ref_and_client_sample.py
Writes to data/ref/.
"""
import os
import csv
import sqlite3
import re

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(ROOT, "db", "supplier_etl.db")
OUT_DIR = os.path.join(ROOT, "data", "ref")


def _date_only(ts: str) -> str:
    """Format timestamp as YYYY-MM-DD for Kristaq alignment."""
    if not ts:
        return ""
    m = re.match(r"(\d{4}-\d{2}-\d{2})", str(ts))
    return m.group(1) if m else ts


def export_ref_clients(conn, out_path: str) -> int:
    cols = ["client_id", "client_name", "created_at", "updated_at"]
    cur = conn.execute("SELECT client_id, client_name, created_at, updated_at FROM ref_clients")
    rows = cur.fetchall()
    if not rows:
        return 0
    os.makedirs(OUT_DIR, exist_ok=True)
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(cols)
        for r in rows:
            w.writerow([r[0], r[1], _date_only(r[2]), _date_only(r[3])])
    return len(rows)


def export_ref_supplier_master_global(conn, out_path: str) -> int:
    """Columns aligned with Kristaq: canonical_supplier_id, normalized_supplier_name, name_key, l1, l2, l3, supplier_description, country_codes, last_enriched_at, last_updated_at."""
    cols = ["canonical_supplier_id", "normalized_supplier_name", "name_key", "l1_category", "l2_category", "l3_category", "supplier_description", "country_codes", "last_enriched_at", "last_updated_at"]
    cur = conn.execute(
        "SELECT canonical_supplier_id, normalized_supplier_name, name_key, l1_category, l2_category, l3_category, supplier_description, country_codes, last_enriched_at, last_updated_at FROM ref_supplier_master_global"
    )
    rows = cur.fetchall()
    if not rows:
        return 0
    os.makedirs(OUT_DIR, exist_ok=True)
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(cols)
        for r in rows:
            w.writerow([r[0], r[1], r[2], r[3] or "", r[4] or "", r[5] or "", r[6] or "", r[7] or "", _date_only(r[8]) if r[8] else "", _date_only(r[9]) if r[9] else ""])
    return len(rows)


def export_client_acme_supplier_master(conn, out_path: str) -> int:
    """Columns aligned with Kristaq (Table 3 + aggregates): client_id, supplier_id, supplier_name_raw, supplier_name_normalized, l1, l2, l3, product_description, total_spend, total_quantity, window_start_date, window_end_date, preferred_supplier_flag, created_at, updated_at."""
    cols = ["client_id", "supplier_id", "supplier_name_raw", "supplier_name_normalized", "l1_category", "l2_category", "l3_category", "product_description", "total_spend", "total_quantity", "window_start_date", "window_end_date", "preferred_supplier_flag", "created_at", "updated_at"]
    cur = conn.execute(
        "SELECT client_id, supplier_id, supplier_name_raw, supplier_name_normalized, l1_category, l2_category, l3_category, product_description, total_spend, total_quantity, window_start_date, window_end_date, preferred_supplier_flag, created_at, updated_at FROM client_acme_supplier_master"
    )
    rows = cur.fetchall()
    if not rows:
        return 0
    os.makedirs(OUT_DIR, exist_ok=True)
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(cols)
        for r in rows:
            w.writerow([r[0], r[1], r[2] or r[3], r[3], r[4] or "", r[5] or "", r[6] or "", r[7] or "", r[8], r[9], _date_only(r[10]) if r[10] else "", _date_only(r[11]) if r[11] else "", r[12] or "", _date_only(r[13]) if r[13] else "", _date_only(r[14]) if r[14] else ""])
    return len(rows)


def export_client_acme_supplier_crosswalk(conn, out_path: str) -> int:
    """Columns aligned with Kristaq: client_id, supplier_id, canonical_supplier_id, match_method, match_confidence, matched_on, created_at, updated_at."""
    cols = ["client_id", "supplier_id", "canonical_supplier_id", "match_method", "match_confidence", "matched_on", "created_at", "updated_at"]
    cur = conn.execute("SELECT client_id, supplier_id, canonical_supplier_id, match_method, match_confidence, matched_on, created_at, updated_at FROM client_acme_supplier_crosswalk")
    rows = cur.fetchall()
    if not rows:
        return 0
    os.makedirs(OUT_DIR, exist_ok=True)
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(cols)
        for r in rows:
            w.writerow([r[0], r[1], r[2], r[3] or "", r[4], r[5] or "", _date_only(r[6]) if r[6] else "", _date_only(r[7]) if r[7] else ""])
    return len(rows)


def export_client_acme_transactions_t1(conn, out_path: str) -> int:
    """Columns aligned with Kristaq Tier 1 sample: client_id, po_number, supplier_id, amount, currency, supplier_name_normalized, l1_category, l2_category, l3_category."""
    cols = ["client_id", "po_number", "supplier_id", "amount", "currency", "supplier_name_normalized", "l1_category", "l2_category", "l3_category"]
    cur = conn.execute(
        "SELECT client_id, po_number, supplier_id, amount, currency, supplier_name_normalized, l1_category, l2_category, l3_category FROM client_acme_transactions_t1"
    )
    rows = cur.fetchall()
    if not rows:
        return 0
    os.makedirs(OUT_DIR, exist_ok=True)
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(cols)
        w.writerows(rows)
    return len(rows)


def main():
    if not os.path.isfile(DB_PATH):
        print(f"DB not found: {DB_PATH}. Run db/init_db.py and run_pipeline.py first.")
        return
    conn = sqlite3.connect(DB_PATH)
    exports = [
        ("ref_clients", export_ref_clients, "ref_clients.csv"),
        ("ref_supplier_master_global", export_ref_supplier_master_global, "ref_supplier_master_global.csv"),
        ("client_acme_supplier_master", export_client_acme_supplier_master, "client_acme_supplier_master.csv"),
        ("client_acme_supplier_crosswalk", export_client_acme_supplier_crosswalk, "client_acme_supplier_crosswalk.csv"),
        ("client_acme_transactions_t1", export_client_acme_transactions_t1, "client_acme_transactions_t1.csv"),
    ]
    for name, fn, filename in exports:
        out_path = os.path.join(OUT_DIR, filename)
        try:
            n = fn(conn, out_path)
            print(f"Exported {name} -> {out_path} ({n} rows)")
        except sqlite3.OperationalError as e:
            print(f"Skip {name}: {e}")
    conn.close()
    print("Done. CSVs in data/ref/ are aligned with Excel-Example-CSVs shared with Kristaq/Bhavin.")


if __name__ == "__main__":
    main()

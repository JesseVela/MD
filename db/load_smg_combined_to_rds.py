"""
Load SMG_combined(supplier_data).csv into RDS ref tables:
  ref.client_master, ref.supplier_master, ref.global_supplier_data_master

Aligns with 10-Feb schema: genpact_supplier_id (G10001, G10002, ...), one row per supplier.
CSV has multiple rows per supplier (different L1/L2/L3/tags); we dedupe and use first occurrence
for global_supplier_data_master.

Usage (from Supplier-etl-local or project root):
  Set DB_HOST, DB_USERNAME, DB_PASSWORD, DB_NAME, DB_PORT then:
  python db/load_smg_combined_to_rds.py [path_to_csv]

Default CSV path: ../../Meeting-Records/11,-FEB/SMG_combined(supplier_data).csv (relative to script dir)
"""
import csv
import os
import re
import sys
from pathlib import Path

try:
    import psycopg2
    from psycopg2 import sql
except ImportError:
    print("psycopg2 is required. Install with: pip install psycopg2-binary", file=sys.stderr)
    sys.exit(1)

DB_HOST = os.environ.get("DB_HOST", "localhost")
DB_USERNAME = os.environ.get("DB_USERNAME", "")
DB_PASSWORD = os.environ.get("DB_PASSWORD", "")
DB_NAME = os.environ.get("DB_NAME", "supplier_etl")
DB_PORT = os.environ.get("DB_PORT", "5432")

# Default CSV path relative to this script's directory
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent.parent
DEFAULT_CSV = PROJECT_ROOT / "Meeting-Records" / "11,-FEB" / "SMG_combined(supplier_data).csv"


def _clean(s: str) -> str:
    if s is None or (isinstance(s, str) and s.strip() in ("", "Unknown", "NaN", "None")):
        return None
    s = str(s).strip()
    if re.search(r"NaN$", s):
        s = re.sub(r"NaN$", "", s).strip()
    return s if s else None


def ensure_ref_tables(cur):
    """Create ref.client_master, ref.supplier_master, ref.global_supplier_data_master if not exist."""
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
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_ref_supplier_master_name
        ON ref.supplier_master (normalized_supplier_name)
    """)


def load_csv(csv_path: Path):
    """Read CSV and return (clients set, list of supplier rows deduped by normalized name)."""
    clients = set()
    # key: normalized_supplier_name (first occurrence wins for global_supplier_data_master)
    suppliers = {}
    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            client_num = _clean(row.get("client #", ""))
            if client_num:
                clients.add(client_num)
            name = _clean(row.get("Normalized_Supplier_Name", ""))
            if not name:
                continue
            if name not in suppliers:
                suppliers[name] = {
                    "normalized_supplier_name": name,
                    "supplier_description": _clean(row.get("Supplier_Description", "")),
                    "employee_count": _clean(row.get("Employee_Count", "")),
                    "revenue": _clean(row.get("Revenue", "")),
                    "year_established": _clean(row.get("Year_Established", "")),
                    "l1_category": _clean(row.get("LI", "")),
                    "l2_category": _clean(row.get("L2", "")),
                    "l3_category": _clean(row.get("L3", "")),
                    "product_service_tags": _clean(row.get("Product_Service_Tags", "")),
                    "ship_to_countries": _clean(row.get("Ship_To_Countries", "")),
                    "country_codes": _clean(row.get("Country_Codes", "")),
                }
    return clients, list(suppliers.values())


def main():
    csv_path = Path(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_CSV
    if not csv_path.is_file():
        print(f"CSV not found: {csv_path}", file=sys.stderr)
        sys.exit(1)

    if not DB_USERNAME:
        print("Set DB_USERNAME, DB_PASSWORD, DB_HOST, DB_NAME in the environment.", file=sys.stderr)
        sys.exit(1)

    clients, supplier_rows = load_csv(csv_path)
    # Assign genpact_supplier_id: G10001, G10002, ...
    for i, row in enumerate(supplier_rows, start=1):
        row["genpact_supplier_id"] = f"G{i:05d}"

    conn = psycopg2.connect(
        host=DB_HOST, dbname=DB_NAME, user=DB_USERNAME, password=DB_PASSWORD, port=DB_PORT
    )
    conn.autocommit = False
    try:
        cur = conn.cursor()
        ensure_ref_tables(cur)

        # Insert clients
        for cid in sorted(clients):
            cur.execute("""
                INSERT INTO ref.client_master (client_id, client_name)
                VALUES (%s, %s)
                ON CONFLICT (client_id) DO UPDATE SET client_name = COALESCE(ref.client_master.client_name, EXCLUDED.client_name)
            """, (cid, f"Client {cid}"))

        # Insert ref.supplier_master
        for row in supplier_rows:
            cur.execute("""
                INSERT INTO ref.supplier_master (genpact_supplier_id, normalized_supplier_name, date_added)
                VALUES (%s, %s, CURRENT_TIMESTAMP)
                ON CONFLICT (genpact_supplier_id) DO UPDATE SET
                    normalized_supplier_name = EXCLUDED.normalized_supplier_name
            """, (row["genpact_supplier_id"], row["normalized_supplier_name"]))

        # Insert ref.global_supplier_data_master
        for row in supplier_rows:
            cur.execute("""
                INSERT INTO ref.global_supplier_data_master (
                    genpact_supplier_id, supplier_description, employee_count, revenue,
                    year_established, l1_category, l2_category, l3_category,
                    product_service_tags, ship_to_countries, country_codes, date_added
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                ON CONFLICT (genpact_supplier_id) DO UPDATE SET
                    supplier_description = COALESCE(EXCLUDED.supplier_description, ref.global_supplier_data_master.supplier_description),
                    employee_count = COALESCE(EXCLUDED.employee_count, ref.global_supplier_data_master.employee_count),
                    revenue = COALESCE(EXCLUDED.revenue, ref.global_supplier_data_master.revenue),
                    year_established = COALESCE(EXCLUDED.year_established, ref.global_supplier_data_master.year_established),
                    l1_category = COALESCE(EXCLUDED.l1_category, ref.global_supplier_data_master.l1_category),
                    l2_category = COALESCE(EXCLUDED.l2_category, ref.global_supplier_data_master.l2_category),
                    l3_category = COALESCE(EXCLUDED.l3_category, ref.global_supplier_data_master.l3_category),
                    product_service_tags = COALESCE(EXCLUDED.product_service_tags, ref.global_supplier_data_master.product_service_tags),
                    ship_to_countries = COALESCE(EXCLUDED.ship_to_countries, ref.global_supplier_data_master.ship_to_countries),
                    country_codes = COALESCE(EXCLUDED.country_codes, ref.global_supplier_data_master.country_codes)
            """, (
                row["genpact_supplier_id"],
                row["supplier_description"],
                row["employee_count"],
                row["revenue"],
                row["year_established"],
                row["l1_category"],
                row["l2_category"],
                row["l3_category"],
                row["product_service_tags"],
                row["ship_to_countries"],
                row["country_codes"],
            ))

        conn.commit()
        cur.close()
        print(f"Loaded: ref.client_master ({len(clients)}), ref.supplier_master ({len(supplier_rows)}), ref.global_supplier_data_master ({len(supplier_rows)}).")
    except Exception as e:
        conn.rollback()
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        conn.close()


if __name__ == "__main__":
    main()

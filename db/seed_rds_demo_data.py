"""
Seed RDS with demo data — 10-Feb schema (ref.supplier_master, ref.global_supplier_data_master,
client_supplier_data_master, supplier_crosswalk with genpact_supplier_id).

Run after init_postgres_db.py. Uses same env: DB_HOST, DB_USERNAME, DB_PASSWORD, DB_NAME, DB_PORT.
Safe to run multiple times (uses ON CONFLICT where applicable).

  From Supplier-etl-local: python db/seed_rds_demo_data.py
"""
import os
import sys

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

# Genpact supplier IDs (10-Feb schema)
G_STAPLES = "G10001"
G_AMAZON = "G10002"


def main():
    if not DB_USERNAME:
        print("Set DB_USERNAME, DB_PASSWORD, DB_HOST, DB_NAME in the environment.", file=sys.stderr)
        sys.exit(1)
    conn = psycopg2.connect(
        host=DB_HOST, dbname=DB_NAME, user=DB_USERNAME, password=DB_PASSWORD, port=DB_PORT
    )
    conn.autocommit = False
    try:
        cur = conn.cursor()

        # 1. ref.supplier_master (2 rows)
        cur.execute("""
            INSERT INTO ref.supplier_master (genpact_supplier_id, normalized_supplier_name, date_added)
            VALUES
                (%s, 'Staples Inc', '2026-01-20'::timestamp),
                (%s, 'Amazon Business', '2026-01-20'::timestamp)
            ON CONFLICT (genpact_supplier_id) DO UPDATE SET
                normalized_supplier_name = EXCLUDED.normalized_supplier_name
        """, (G_STAPLES, G_AMAZON))

        # 2. ref.global_supplier_data_master (2 rows)
        cur.execute("""
            INSERT INTO ref.global_supplier_data_master (
                genpact_supplier_id, l1_category, l2_category, l3_category, country_codes, date_added
            ) VALUES
                (%s, 'Office Supplies', 'Facilities', 'Office Supplies', 'US;IN', '2026-01-20'::timestamp),
                (%s, 'IT', 'Technology', 'Software', 'US', '2026-01-20'::timestamp)
            ON CONFLICT (genpact_supplier_id) DO UPDATE SET
                l1_category = EXCLUDED.l1_category,
                l2_category = EXCLUDED.l2_category,
                l3_category = EXCLUDED.l3_category,
                country_codes = EXCLUDED.country_codes
        """, (G_STAPLES, G_AMAZON))

        # 3. client_acme.supplier_crosswalk (2 rows: client supplier_id -> genpact_supplier_id)
        cur.execute("""
            INSERT INTO client_acme.supplier_crosswalk (
                client_id, supplier_id, genpact_supplier_id, match_method, match_confidence, matched_on, date_added
            ) VALUES
                ('acme', '100782', %s, 'exact_name_key', 1.0, 'name_key', '2026-01-20'::timestamp),
                ('acme', '100891', %s, 'exact_name_key', 1.0, 'name_key', '2026-01-20'::timestamp)
            ON CONFLICT (client_id, supplier_id) DO UPDATE SET
                genpact_supplier_id = EXCLUDED.genpact_supplier_id,
                match_method = EXCLUDED.match_method
        """, (G_STAPLES, G_AMAZON))

        # 4. client_acme.client_supplier_data_master (1 demo row — rolling 12-month spend)
        cur.execute("""
            INSERT INTO client_acme.client_supplier_data_master (
                client_id, genpact_supplier_id, item_description, l1_category, l2_category, l3_category,
                spend_year, spend_month, total_spend, total_quantity, currency, preferred_flag_normalized,
                date_added, date_refreshed
            ) VALUES
                ('acme', %s, 'Office supplies & facilities', 'Office Supplies', 'Facilities', 'Office Supplies',
                 2025, 1, 45000.00, 120, 'USD', 'Y', '2026-01-15'::timestamp, '2026-01-20'::timestamp)
            ON CONFLICT (client_id, genpact_supplier_id) DO UPDATE SET
                total_spend = EXCLUDED.total_spend,
                date_refreshed = EXCLUDED.date_refreshed
        """, (G_STAPLES,))

        # 5. client_acme.transactions_t1 (1 sample row — uses genpact_supplier_id)
        cur.execute("DELETE FROM client_acme.transactions_t1 WHERE client_id = 'acme' AND upload_id = 'UP-2026-DEMO'")
        cur.execute("""
            INSERT INTO client_acme.transactions_t1 (
                client_id, upload_id, genpact_supplier_id, supplier_id, supplier_name_normalized,
                l1_category, l2_category, l3_category, country_code,
                po_number, amount, currency, quantity, created_at
            ) VALUES
                ('acme', 'UP-2026-DEMO', %s, '100782', 'Staples Inc',
                 'Office Supplies', 'Facilities', 'Office Supplies', 'US',
                 'PO-2026-001', 15000.00, 'USD', 10, '2026-02-10'::timestamp)
        """, (G_STAPLES,))

        conn.commit()
        cur.close()
        print("Demo data seeded (10-Feb schema):")
        print("  ref.supplier_master (2), ref.global_supplier_data_master (2),")
        print("  client_acme.supplier_crosswalk (2), client_acme.client_supplier_data_master (1), client_acme.transactions_t1 (1).")
    except Exception as e:
        conn.rollback()
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        conn.close()


if __name__ == "__main__":
    main()

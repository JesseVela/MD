"""
Verify RDS — 10-Feb schema (ref.client_master, ref.supplier_master, ref.global_supplier_data_master,
client_acme.supplier_crosswalk, client_acme.client_supplier_data_master, client_acme.transactions_t1).

Set DB_HOST, DB_USERNAME, DB_PASSWORD, DB_NAME, DB_PORT in environment, then:
  python db/verify_rds.py
"""
import os
import sys

try:
    import psycopg2
except ImportError:
    print("Install psycopg2-binary first.", file=sys.stderr)
    sys.exit(1)


def main():
    host = os.environ.get("DB_HOST")
    user = os.environ.get("DB_USERNAME")
    password = os.environ.get("DB_PASSWORD")
    dbname = os.environ.get("DB_NAME")
    port = os.environ.get("DB_PORT", "5432")
    if not all([host, user, password, dbname]):
        print("Set DB_HOST, DB_USERNAME, DB_PASSWORD, DB_NAME (and optionally DB_PORT).", file=sys.stderr)
        sys.exit(1)
    try:
        conn = psycopg2.connect(host=host, dbname=dbname, user=user, password=password, port=port)
    except Exception as e:
        print(f"Connection failed: {e}", file=sys.stderr)
        sys.exit(1)

    cur = conn.cursor()

    print("--- Schemas ---")
    cur.execute("""
        SELECT schema_name FROM information_schema.schemata
        WHERE schema_name IN ('ref', 'client_acme', 'client_beta', 'vec', 'public')
        ORDER BY schema_name
    """)
    for row in cur.fetchall():
        print(" ", row[0])

    print("\n--- ref.client_master ---")
    cur.execute("SELECT * FROM ref.client_master ORDER BY client_id")
    for row in cur.fetchall():
        print(" ", row)

    print("\n--- ref.supplier_master ---")
    cur.execute("SELECT genpact_supplier_id, normalized_supplier_name FROM ref.supplier_master ORDER BY genpact_supplier_id")
    for row in cur.fetchall():
        print(" ", row)

    print("\n--- ref.global_supplier_data_master (row count) ---")
    cur.execute("SELECT COUNT(*) FROM ref.global_supplier_data_master")
    print("  rows:", cur.fetchone()[0])

    print("\n--- client_acme.supplier_crosswalk ---")
    cur.execute("SELECT client_id, supplier_id, genpact_supplier_id, match_method FROM client_acme.supplier_crosswalk ORDER BY supplier_id")
    for row in cur.fetchall():
        print(" ", row)

    print("\n--- client_acme.client_supplier_data_master (row count) ---")
    cur.execute("SELECT COUNT(*) FROM client_acme.client_supplier_data_master")
    print("  rows:", cur.fetchone()[0])

    print("\n--- client_acme.transactions_t1 (sample) ---")
    cur.execute("""
        SELECT client_id, upload_id, genpact_supplier_id, supplier_id, po_number, amount, currency
        FROM client_acme.transactions_t1 ORDER BY row_id LIMIT 3
    """)
    for row in cur.fetchall():
        print(" ", row)

    cur.close()
    conn.close()
    print("\nRDS connection OK (10-Feb schema).")


if __name__ == "__main__":
    main()

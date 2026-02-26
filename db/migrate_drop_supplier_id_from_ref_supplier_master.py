"""
Drop supplier_id from ref.supplier_master on RDS (17-Feb alignment).

Run after init_postgres_db was previously run with supplier_id. Safe to run multiple times.
Uses env: DB_HOST, DB_USERNAME, DB_PASSWORD, DB_NAME, DB_PORT.

  cd Supplier-etl-local
  python db/migrate_drop_supplier_id_from_ref_supplier_master.py
"""
import os
import sys

try:
    import psycopg2
except ImportError:
    print("psycopg2 is required. Install with: pip install psycopg2-binary", file=sys.stderr)
    sys.exit(1)

DB_HOST = os.environ.get("DB_HOST", "localhost")
DB_USERNAME = os.environ.get("DB_USERNAME", "")
DB_PASSWORD = os.environ.get("DB_PASSWORD", "")
DB_NAME = os.environ.get("DB_NAME", "supplier_etl")
DB_PORT = os.environ.get("DB_PORT", "5432")


def main():
    if not DB_USERNAME:
        print("Set DB_USERNAME, DB_PASSWORD, DB_HOST, DB_NAME in the environment.", file=sys.stderr)
        sys.exit(1)
    conn = psycopg2.connect(
        host=DB_HOST, dbname=DB_NAME, user=DB_USERNAME, password=DB_PASSWORD, port=DB_PORT
    )
    conn.autocommit = True
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT column_name FROM information_schema.columns
            WHERE table_schema = 'ref' AND table_name = 'supplier_master' AND column_name = 'supplier_id'
        """)
        if cur.fetchone():
            cur.execute("ALTER TABLE ref.supplier_master DROP COLUMN IF EXISTS supplier_id")
            print("Dropped ref.supplier_master.supplier_id (17-Feb alignment).")
        else:
            print("ref.supplier_master has no supplier_id column; nothing to do.")
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()

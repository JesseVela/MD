"""
Drop legacy ref tables only (ref.clients, ref.supplier_master_global).
For full cleanup of all legacy tables (ref + client_*.supplier_master + vec), use:
  python db/drop_all_legacy_tables.py
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
    try:
        cur = conn.cursor()
        cur.execute("DROP TABLE IF EXISTS ref.supplier_master_global CASCADE")
        print("Dropped ref.supplier_master_global (legacy).")
        cur.execute("DROP TABLE IF EXISTS ref.clients CASCADE")
        print("Dropped ref.clients (legacy).")
        cur.close()
        print("Done. For full legacy cleanup (client_*.supplier_master, vec), run: python db/drop_all_legacy_tables.py")
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        conn.close()


if __name__ == "__main__":
    main()

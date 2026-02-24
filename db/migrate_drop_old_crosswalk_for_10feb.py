"""
One-time migration: drop OLD client-schema tables so init_postgres_db.py can recreate them
with the 10-Feb schema (genpact_supplier_id in supplier_crosswalk and transactions_t1).

Your RDS currently has:
- transactions_t1 with supplier_id but NO genpact_supplier_id
- supplier_crosswalk with canonical_supplier_id instead of genpact_supplier_id

This script drops all client_acme and client_beta tables that init recreates, so the next
init run creates them with the correct columns (see Meeting-Prep/02,-FEB/Docs/05-Full-Table-Column-Reference.md).

Run this, then init, then seed.

Usage (same env as init): python db/migrate_drop_old_crosswalk_for_10feb.py
Then: python db/init_postgres_db.py
Then: python db/seed_rds_demo_data.py
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

CLIENT_IDS = ["acme", "beta"]

# All client-schema tables that init_postgres_db.py creates (10-Feb schema).
# We drop these so init can recreate them with genpact_supplier_id etc.
TABLES_TO_DROP = [
    "client_supplier_data_master",
    "supplier_crosswalk",
    "transactions_t1",
    "transactions_t2",
    "transactions_t3",
    "supplier_master",  # old table; new schema uses client_supplier_data_master
]


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
        for cid in CLIENT_IDS:
            schema = f"client_{cid}"
            for table in TABLES_TO_DROP:
                cur.execute(
                    sql.SQL("DROP TABLE IF EXISTS {}.{} CASCADE").format(
                        sql.Identifier(schema), sql.Identifier(table)
                    )
                )
                print(f"Dropped {schema}.{table} (if existed).")
        cur.close()
        print("Done. Now run: python db/init_postgres_db.py  then  python db/seed_rds_demo_data.py")
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        conn.close()


if __name__ == "__main__":
    main()

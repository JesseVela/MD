"""
Drop ALL legacy tables on RDS so ref has only the 10-Feb schema.
Run this to satisfy: "only one set of tables; no duplicate global supplier / client tables."

10-Feb canonical schema (what should remain after init_postgres_db.py):
  ref:  client_master, supplier_master, global_supplier_data_master  (only these 3)
  client_acme / client_beta:  client_supplier_data_master, supplier_crosswalk, transactions_t1, transactions_t2, transactions_t3
  vec:  vector_embeddings
  public:  etl_run_logs

Legacy (dropped by this script):
  ref:  clients, supplier_master_global
  client_<id>:  supplier_master, supplier_crosswalk, transactions_t1/t2/t3, client_supplier_data_master
                 (all dropped so init recreates them with correct 10-Feb columns; data in client schemas is lost)
  vec:  vector_embeddings  (dropped so init can recreate with genpact_supplier_id)
Ref tables ref.client_master, ref.supplier_master, ref.global_supplier_data_master are NOT dropped (data preserved).

Usage: Set DB_HOST, DB_USERNAME, DB_PASSWORD, DB_NAME, DB_PORT then:
  python db/drop_all_legacy_tables.py
Then run: python db/init_postgres_db.py  (recreates vec.vector_embeddings and client tables if missing)
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

# Legacy tables to drop (order: client first in case of FK to ref, then ref, then vec)
LEGACY_REF_TABLES = ["supplier_master_global", "clients"]
# Client: legacy name + tables init recreates (so they get correct 10-Feb structure; data in these is lost)
LEGACY_CLIENT_TABLES = [
    "supplier_master",              # old name; 10-Feb uses client_supplier_data_master
    "supplier_crosswalk",           # init recreates with genpact_supplier_id
    "transactions_t1", "transactions_t2", "transactions_t3",
    "client_supplier_data_master",  # init recreates
]
LEGACY_VEC_TABLES = ["vector_embeddings"]   # drop so init recreates with genpact_supplier_id


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

        # 1. Client schemas: drop legacy + all tables init recreates (so init can create correct 10-Feb structure)
        for cid in CLIENT_IDS:
            schema = f"client_{cid}"
            for table in LEGACY_CLIENT_TABLES:
                cur.execute(
                    sql.SQL("DROP TABLE IF EXISTS {}.{} CASCADE").format(
                        sql.Identifier(schema), sql.Identifier(table)
                    )
                )
                print(f"Dropped {schema}.{table} (if existed).")

        # 2. ref: drop legacy tables
        for table in LEGACY_REF_TABLES:
            cur.execute(
                sql.SQL("DROP TABLE IF EXISTS ref.{} CASCADE").format(sql.Identifier(table))
            )
            print(f"Dropped ref.{table} (if existed).")

        # 3. vec: drop so init can recreate with genpact_supplier_id
        for table in LEGACY_VEC_TABLES:
            cur.execute(
                sql.SQL("DROP TABLE IF EXISTS vec.{} CASCADE").format(sql.Identifier(table))
            )
            print(f"Dropped vec.{table} (if existed).")

        cur.close()
        print("\nLegacy tables removed.")
        print("ref should now have only: client_master, supplier_master, global_supplier_data_master.")
        print("Run: python db/init_postgres_db.py  to recreate vec.vector_embeddings (and any missing client tables).")
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        conn.close()


if __name__ == "__main__":
    main()

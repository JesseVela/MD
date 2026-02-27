"""
Initialize PostgreSQL database — 10-Feb schema (aligned with Kristaq docs).

Creates: ref.client_master, ref.supplier_master, ref.global_supplier_data_master,
         client_<id>.client_supplier_data_master, client_<id>.supplier_crosswalk,
         client_<id>.transactions_t1/t2/t3, vec.vector_embeddings, public.etl_run_logs.

Uses env: DB_HOST, DB_USERNAME, DB_PASSWORD; optional: DB_NAME (default supplier_etl), DB_PORT (default 5432).
Run from Supplier-etl-local: python db/init_postgres_db.py

    Requires: pip install psycopg2-binary
"""
import os
import sys

try:
    import psycopg2
    from psycopg2 import sql
    from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
except ImportError:
    print("psycopg2 is required. Install with: pip install psycopg2-binary", file=sys.stderr)
    sys.exit(1)

DB_HOST = os.environ.get("DB_HOST", "localhost")
DB_USERNAME = os.environ.get("DB_USERNAME", "")
DB_PASSWORD = os.environ.get("DB_PASSWORD", "")
DB_NAME = os.environ.get("DB_NAME", "supplier_etl")
DB_PORT = os.environ.get("DB_PORT", "5432")

CLIENT_IDS = ["acme", "beta"]


def get_connect_kwargs(use_db_name: bool = True):
    """Build connection kwargs; use_db_name=False for connecting to 'postgres' to create DB."""
    kwargs = {
        "host": DB_HOST,
        "user": DB_USERNAME,
        "password": DB_PASSWORD,
        "port": DB_PORT,
    }
    if use_db_name:
        kwargs["dbname"] = DB_NAME
    else:
        kwargs["dbname"] = "postgres"
    return kwargs


def ensure_database_exists():
    """Create database if it does not exist."""
    kwargs = get_connect_kwargs(use_db_name=False)
    conn = psycopg2.connect(**kwargs)
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cur = conn.cursor()
    cur.execute(
        "SELECT 1 FROM pg_database WHERE datname = %s",
        (DB_NAME,),
    )
    if cur.fetchone() is None:
        cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(DB_NAME)))
        print(f"Created database: {DB_NAME}")
    cur.close()
    conn.close()


def init_postgres_db() -> None:
    """Create schemas and all tables — 10-Feb schema (genpact_supplier_id, ref.client_master, etc.)."""
    if not DB_USERNAME:
        print("Set DB_USERNAME (and DB_PASSWORD, DB_HOST) in the environment.", file=sys.stderr)
        sys.exit(1)

    ensure_database_exists()

    conn = psycopg2.connect(**get_connect_kwargs())
    conn.autocommit = False
    try:
        cur = conn.cursor()

        cur.execute("CREATE SCHEMA IF NOT EXISTS ref")
        cur.execute("CREATE SCHEMA IF NOT EXISTS vec")
        for cid in CLIENT_IDS:
            cur.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(f"client_{cid}")))

        # --- ref.client_master ---
        cur.execute("""
            CREATE TABLE IF NOT EXISTS ref.client_master (
                client_id TEXT PRIMARY KEY,
                client_name TEXT NOT NULL,
                client_industry TEXT,
                date_added TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # --- ref.supplier_master (17-Feb: no supplier_id; client supplier_id only in client-level tables) ---
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

        # --- ref.global_supplier_data_master ---
        cur.execute("""
            CREATE TABLE IF NOT EXISTS ref.global_supplier_data_master (
                genpact_supplier_id TEXT NOT NULL PRIMARY KEY
                    REFERENCES ref.supplier_master(genpact_supplier_id),
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

        # --- Seed ref.client_master (acme, beta) ---
        cur.execute("""
            INSERT INTO ref.client_master (client_id, client_name, date_added)
            VALUES ('acme', 'Acme Corp', '2026-01-15'::timestamp),
                   ('beta', 'Beta Inc', '2026-01-20'::timestamp)
            ON CONFLICT (client_id) DO NOTHING
        """)

        for cid in CLIENT_IDS:
            schema = f"client_{cid}"

            # --- client_<id>.client_supplier_data_master ---
            cur.execute(sql.SQL("""
                CREATE TABLE IF NOT EXISTS {}.client_supplier_data_master (
                    client_id TEXT NOT NULL,
                    genpact_supplier_id TEXT NOT NULL
                        REFERENCES ref.supplier_master(genpact_supplier_id),
                    item_description TEXT,
                    l1_category TEXT,
                    l2_category TEXT,
                    l3_category TEXT,
                    spend_year INTEGER,
                    spend_month INTEGER,
                    client_industry TEXT,
                    total_spend NUMERIC(18, 4),
                    total_quantity NUMERIC(18, 4),
                    unit_of_measure TEXT,
                    unit_price NUMERIC(18, 4),
                    currency TEXT,
                    ship_to_countries TEXT,
                    country_codes TEXT,
                    product_service_tags TEXT,
                    preferred_flag_original TEXT,
                    preferred_flag_normalized CHAR(1),
                    date_added TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    date_refreshed TIMESTAMP,
                    first_invoice_date DATE,
                    payment_terms TEXT,
                    PRIMARY KEY (client_id, genpact_supplier_id)
                )
            """).format(sql.Identifier(schema)))

            # --- client_<id>.supplier_crosswalk ---
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

            # --- client_<id>.transactions_t1 (genpact_supplier_id required) ---
            cur.execute(sql.SQL("""
                CREATE TABLE IF NOT EXISTS {}.transactions_t1 (
                    row_id BIGSERIAL PRIMARY KEY,
                    client_id TEXT NOT NULL,
                    upload_id TEXT NOT NULL,
                    genpact_supplier_id TEXT NOT NULL REFERENCES ref.supplier_master(genpact_supplier_id),
                    supplier_id TEXT,
                    supplier_name_normalized TEXT,
                    l1_category TEXT,
                    l2_category TEXT,
                    l3_category TEXT,
                    country_code TEXT,
                    po_number TEXT,
                    amount NUMERIC(18, 4),
                    currency TEXT,
                    quantity NUMERIC(18, 4),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """).format(sql.Identifier(schema)))

            cur.execute(sql.SQL("""
                CREATE TABLE IF NOT EXISTS {}.transactions_t2 (
                    client_id TEXT NOT NULL,
                    upload_id TEXT NOT NULL,
                    row_id TEXT,
                    extra_columns JSONB
                )
            """).format(sql.Identifier(schema)))

            cur.execute(sql.SQL("""
                CREATE TABLE IF NOT EXISTS {}.transactions_t3 (
                    client_id TEXT NOT NULL,
                    upload_id TEXT NOT NULL,
                    original_columns JSONB,
                    file_name TEXT,
                    uploaded_at TIMESTAMP
                )
            """).format(sql.Identifier(schema)))

        # --- public.etl_run_logs ---
        cur.execute("""
            CREATE TABLE IF NOT EXISTS public.etl_run_logs (
                run_id TEXT NOT NULL,
                stage TEXT NOT NULL,
                status TEXT NOT NULL,
                row_count INTEGER,
                error_message TEXT,
                timestamp TIMESTAMP NOT NULL
            )
        """)

        # --- vec.vector_embeddings (genpact_supplier_id for 10-Feb alignment; client_id='global' for ref-only) ---
        cur.execute("""
            CREATE TABLE IF NOT EXISTS vec.vector_embeddings (
                client_id TEXT NOT NULL,
                genpact_supplier_id TEXT NOT NULL,
                chunk_id TEXT NOT NULL DEFAULT '0',
                embedding BYTEA,
                source_text TEXT,
                indexed_at TIMESTAMP,
                PRIMARY KEY (client_id, genpact_supplier_id, chunk_id)
            )
        """)
        # pgvector: enable extension (may require superuser; if it fails, run migrate_add_pgvector.sql manually)
        try:
            cur.execute("CREATE EXTENSION IF NOT EXISTS vector")
        except Exception:
            pass
        # Add embedding_vec for load_vec_to_rds.py (Bedrock Titan 1536 dim)
        try:
            cur.execute("""
                ALTER TABLE vec.vector_embeddings
                ADD COLUMN IF NOT EXISTS embedding_vec vector(1536)
            """)
        except Exception:
            pass

        conn.commit()
        cur.close()
        print(f"PostgreSQL initialized (10-Feb schema): {DB_HOST}:{DB_PORT}/{DB_NAME}")
        print("  ref: client_master, supplier_master, global_supplier_data_master")
        print("  client_acme, client_beta: client_supplier_data_master, supplier_crosswalk, transactions_t1/t2/t3")
        print("  vec: vector_embeddings")
    except Exception as e:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    init_postgres_db()

"""
Initialize SQLite database with ref and client schemas.
Run from project root: python db/init_db.py
"""
import os
import sqlite3

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(ROOT, "db", "supplier_etl.db")


def init_db(db_path: str = None) -> None:
    db_path = db_path or DB_PATH
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    conn = sqlite3.connect(db_path, timeout=15.0)
    try:
        _create_schema(conn)
        conn.commit()
        print(f"Database initialized: {db_path}")
    except sqlite3.OperationalError as e:
        conn.close()
        if "disk I/O" in str(e) or "locked" in str(e).lower():
            journal = db_path + "-journal"
            hint = ""
            if os.path.isfile(journal):
                hint = f" Try closing other apps using the DB, then delete the file '{journal}' and run again."
            raise SystemExit(
                f"SQLite error: {e}\nRun this script in a normal terminal (e.g. PowerShell or Cursor Terminal), not via Cursor agent.{hint}"
            ) from e
        raise
    else:
        conn.close()


def _migrate_client_tables(conn: sqlite3.Connection) -> None:
    """Add missing columns to existing client tables (e.g. quantity in transactions_t1)."""
    try:
        conn.execute("ALTER TABLE client_acme_transactions_t1 ADD COLUMN quantity REAL")
    except sqlite3.OperationalError as e:
        if "duplicate column" not in str(e).lower():
            raise

def _create_schema(conn: sqlite3.Connection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS ref_clients (
            client_id TEXT PRIMARY KEY,
            client_name TEXT NOT NULL,
            created_at TEXT,
            updated_at TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS ref_supplier_master_global (
            canonical_supplier_id TEXT PRIMARY KEY,
            normalized_supplier_name TEXT NOT NULL,
            name_key TEXT NOT NULL,
            original_name_variants TEXT,
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
            enrichment_confidence REAL,
            last_enriched_at TEXT,
            last_updated_at TEXT,
            provenance TEXT
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_ref_global_name_key ON ref_supplier_master_global(name_key)")

    conn.execute(
        "INSERT OR IGNORE INTO ref_clients (client_id, client_name, created_at, updated_at) VALUES ('acme', 'Acme Corp', '2026-01-15', '2026-01-15'), ('beta', 'Beta Inc', '2026-01-20', '2026-01-20')"
    )

    conn.execute("""
        CREATE TABLE IF NOT EXISTS client_acme_supplier_master (
            client_id TEXT NOT NULL,
            supplier_id TEXT NOT NULL,
            supplier_name_raw TEXT,
            supplier_name_normalized TEXT,
            l1_category TEXT,
            l2_category TEXT,
            l3_category TEXT,
            product_description TEXT,
            total_spend REAL,
            total_quantity REAL,
            time_period TEXT,
            window_start_date TEXT,
            window_end_date TEXT,
            preferred_supplier_flag TEXT,
            created_at TEXT,
            updated_at TEXT,
            PRIMARY KEY (client_id, supplier_id)
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS client_acme_supplier_crosswalk (
            client_id TEXT NOT NULL,
            supplier_id TEXT NOT NULL,
            canonical_supplier_id TEXT,
            match_method TEXT,
            match_confidence REAL,
            matched_on TEXT,
            created_at TEXT,
            updated_at TEXT,
            PRIMARY KEY (client_id, supplier_id),
            FOREIGN KEY (canonical_supplier_id) REFERENCES ref_supplier_master_global(canonical_supplier_id)
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS client_acme_transactions_t1 (
            client_id TEXT NOT NULL,
            upload_id TEXT NOT NULL,
            supplier_id TEXT NOT NULL,
            supplier_name_normalized TEXT,
            l1_category TEXT,
            l2_category TEXT,
            l3_category TEXT,
            country_code TEXT,
            po_number TEXT,
            amount REAL,
            currency TEXT,
            quantity REAL,
            created_at TEXT,
            row_id INTEGER PRIMARY KEY AUTOINCREMENT
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS client_acme_transactions_t2 (
            client_id TEXT NOT NULL,
            upload_id TEXT NOT NULL,
            row_id TEXT,
            extra_columns TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS client_acme_transactions_t3 (
            client_id TEXT NOT NULL,
            upload_id TEXT NOT NULL,
            original_columns TEXT,
            file_name TEXT,
            uploaded_at TEXT
        )
    """)
    _migrate_client_tables(conn)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS etl_run_logs (
            run_id TEXT NOT NULL,
            stage TEXT NOT NULL,
            status TEXT NOT NULL,
            row_count INTEGER,
            error_message TEXT,
            timestamp TEXT NOT NULL
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS vector_embeddings (
            client_id TEXT NOT NULL,
            supplier_id TEXT NOT NULL,
            canonical_supplier_id TEXT,
            chunk_id TEXT NOT NULL DEFAULT '0',
            embedding BLOB,
            source_text TEXT,
            indexed_at TEXT,
            PRIMARY KEY (client_id, supplier_id, chunk_id)
        )
    """)


if __name__ == "__main__":
    init_db()

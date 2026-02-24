"""
Structured path: normalization & deduplication → write to SQLite (ref + client tables).
Aligned with Excel-Example-CSVs shared with Kristaq: client_id acme, supplier_id 100782, canonical_supplier_id SUP-00001, match_method exact_name_key.
"""
import os
import sqlite3
from datetime import datetime, timezone

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(ROOT, "db", "supplier_etl.db")

# Map client_id (acme, beta) to DB table prefix (client_acme, client_beta). Aligned with Kristaq.
CLIENT_ID_TO_TABLE_PREFIX = {"acme": "client_acme", "beta": "client_beta"}


def _name_key(name: str) -> str:
    """Normalized match key: lowercase, strip, collapse spaces (Kristaq: name_key)."""
    if not name or not isinstance(name, str):
        return ""
    return " ".join(name.lower().strip().split())


def get_conn(db_path: str = None):
    db_path = db_path or DB_PATH
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    return sqlite3.connect(db_path)


def _table_prefix(client_id: str) -> str:
    return CLIENT_ID_TO_TABLE_PREFIX.get(client_id, "client_" + client_id)


def _ensure_quantity_column(conn, table_name: str) -> None:
    """Add quantity column to transactions_t1 if missing (migration for older DBs)."""
    try:
        conn.execute(f"ALTER TABLE {table_name} ADD COLUMN quantity REAL")
    except sqlite3.OperationalError as e:
        if "duplicate column" not in str(e).lower():
            raise


def ensure_ref_clients(conn, client_id: str, client_name: str):
    """Insert ref.clients if not exists. Dates as YYYY-MM-DD for Kristaq alignment."""
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    conn.execute(
        "INSERT OR IGNORE INTO ref_clients (client_id, client_name, created_at, updated_at) VALUES (?, ?, ?, ?)",
        (client_id, client_name, now, now),
    )


def find_or_create_canonical(conn, normalized_name: str, name_key: str, l1: str = None, l2: str = None, l3: str = None) -> str:
    """Match by name_key; if not found insert with SUP-00001 style ID. Returns canonical_supplier_id."""
    cur = conn.execute(
        "SELECT canonical_supplier_id FROM ref_supplier_master_global WHERE name_key = ?",
        (name_key,),
    )
    row = cur.fetchone()
    if row:
        return row[0]
    cur = conn.execute("SELECT COUNT(*) FROM ref_supplier_master_global")
    n = cur.fetchone()[0]
    canonical_id = f"SUP-{str(n + 1).zfill(5)}"
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    conn.execute(
        """INSERT INTO ref_supplier_master_global
           (canonical_supplier_id, normalized_supplier_name, name_key, l1_category, l2_category, l3_category, last_enriched_at, last_updated_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
        (canonical_id, normalized_name, name_key, l1 or "", l2 or "", l3 or "", now, now),
    )
    return canonical_id


def write_structured(
    client_id: str,
    upload_id: str,
    rows: list[dict],
    client_name: str = None,
    db_path: str = None,
) -> dict:
    """
    Normalize & dedupe; write ref + client tables. Uses exact_name_key, match_confidence 1.0, matched_on name_key.
    After transactions, computes total_spend, total_quantity per supplier and updates supplier_master (Table 3 aggregates).
    """
    conn = get_conn(db_path)
    conn.row_factory = sqlite3.Row
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    now_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    counts = {"ref_global_inserted": 0, "client_master_upserted": 0, "crosswalk_upserted": 0, "transactions_t1_inserted": 0}

    prefix = _table_prefix(client_id)
    tbl_master = f"{prefix}_supplier_master"
    tbl_crosswalk = f"{prefix}_supplier_crosswalk"
    tbl_t1 = f"{prefix}_transactions_t1"
    _ensure_quantity_column(conn, tbl_t1)

    if client_name:
        ensure_ref_clients(conn, client_id, client_name)

    for row in rows:
        supp_id = str(row.get("supplier_id") or "").strip() or None
        supp_name = (row.get("supplier_name_normalized") or row.get("supplier_name_raw") or "").strip() or "Unknown"
        name_key = _name_key(supp_name)
        if not name_key:
            continue

        canonical_id = find_or_create_canonical(
            conn, supp_name, name_key,
            row.get("l1_category"), row.get("l2_category"), row.get("l3_category"),
        )
        counts["ref_global_inserted"] += 1

        conn.execute(
            f"""INSERT INTO {tbl_master}
               (client_id, supplier_id, supplier_name_raw, supplier_name_normalized, l1_category, l2_category, l3_category, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(client_id, supplier_id) DO UPDATE SET
               supplier_name_normalized = excluded.supplier_name_normalized,
               l1_category = excluded.l1_category, l2_category = excluded.l2_category, l3_category = excluded.l3_category,
               updated_at = excluded.updated_at""",
            (client_id, supp_id, supp_name, supp_name, row.get("l1_category"), row.get("l2_category"), row.get("l3_category"), now),
        )
        counts["client_master_upserted"] += 1

        conn.execute(
            f"""INSERT INTO {tbl_crosswalk}
               (client_id, supplier_id, canonical_supplier_id, match_method, match_confidence, matched_on, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(client_id, supplier_id) DO UPDATE SET
               canonical_supplier_id = excluded.canonical_supplier_id,
               match_method = 'exact_name_key', match_confidence = 1.0, matched_on = 'name_key',
               updated_at = excluded.updated_at""",
            (client_id, supp_id, canonical_id, "exact_name_key", 1.0, "name_key", now),
        )
        counts["crosswalk_upserted"] += 1

        amount = row.get("amount")
        try:
            amount = float(amount) if amount is not None else None
        except (TypeError, ValueError):
            amount = None
        quantity = row.get("quantity")
        try:
            quantity = float(quantity) if quantity is not None else None
        except (TypeError, ValueError):
            quantity = None

        conn.execute(
            f"""INSERT INTO {tbl_t1}
               (client_id, upload_id, supplier_id, supplier_name_normalized, l1_category, l2_category, l3_category, po_number, amount, currency, quantity, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                client_id, upload_id, supp_id, supp_name,
                row.get("l1_category"), row.get("l2_category"), row.get("l3_category"),
                row.get("po_number"), amount, row.get("currency"), quantity, now,
            ),
        )
        counts["transactions_t1_inserted"] += 1

    # Aggregates (Table 3): total_spend, total_quantity, window_start_date, window_end_date from Tier 1
    cur = conn.execute(
        f"""SELECT supplier_id,
            SUM(amount) AS total_spend, SUM(COALESCE(quantity, 0)) AS total_quantity,
            MIN(created_at) AS window_start_date, MAX(created_at) AS window_end_date
            FROM {tbl_t1} WHERE client_id = ? AND upload_id = ?
            GROUP BY client_id, supplier_id""",
        (client_id, upload_id),
    )
    for r in cur.fetchall():
        ws = r["window_start_date"]
        we = r["window_end_date"]
        ws_date = str(ws)[:10] if ws else None
        we_date = str(we)[:10] if we else None
        conn.execute(
            f"""UPDATE {tbl_master}
                SET total_spend = ?, total_quantity = ?, window_start_date = ?, window_end_date = ?, updated_at = ?
                WHERE client_id = ? AND supplier_id = ?""",
            (r["total_spend"], r["total_quantity"], ws_date, we_date, now, client_id, r["supplier_id"]),
        )

    conn.commit()
    conn.close()
    return counts

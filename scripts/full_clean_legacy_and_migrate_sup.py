"""
One-off: (1) Remove all legacy rows (client_id = 'client_acme').
        (2) Migrate ref_supplier_master_global UUIDs to SUP-00001, SUP-00002 style.
        (3) Update client_acme_supplier_crosswalk to use new canonical_supplier_id.
Run from project root. Then run: python scripts/export_ref_and_client_sample.py
"""
import sqlite3
import os

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(ROOT, "db", "supplier_etl.db")

def main():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    # ---- 1. Delete legacy rows (client_acme) ----
    for table, key in [
        ("ref_clients", "client_id"),
        ("client_acme_supplier_crosswalk", "client_id"),
        ("client_acme_supplier_master", "client_id"),
        ("client_acme_transactions_t1", "client_id"),
    ]:
        cur = conn.execute(f"DELETE FROM {table} WHERE {key} = ?", ("client_acme",))
        n = cur.rowcount
        if n:
            print(f"Deleted {n} legacy row(s) from {table}")

    conn.commit()

    # ---- 2. Build mapping: UUID -> SUP-00001, SUP-00002; keep existing SUP-* as is ----
    rows = conn.execute(
        "SELECT canonical_supplier_id FROM ref_supplier_master_global ORDER BY canonical_supplier_id"
    ).fetchall()
    used = {r[0] for r in rows if r[0].startswith("SUP-")}
    next_sup = 1
    mapping = {}
    for r in rows:
        old_id = r[0]
        if old_id.startswith("SUP-"):
            mapping[old_id] = old_id
        else:
            while f"SUP-{str(next_sup).zfill(5)}" in used:
                next_sup += 1
            new_id = f"SUP-{str(next_sup).zfill(5)}"
            mapping[old_id] = new_id
            used.add(new_id)
            next_sup += 1

    if not any(k != v for k, v in mapping.items()):
        print("No UUID canonical IDs to migrate.")
        conn.close()
        return

    # ---- 3. Update crosswalk ----
    for old_id, new_id in mapping.items():
        if old_id == new_id:
            continue
        n = conn.execute(
            "UPDATE client_acme_supplier_crosswalk SET canonical_supplier_id = ? WHERE canonical_supplier_id = ?",
            (new_id, old_id),
        ).rowcount
        if n:
            print(f"Updated crosswalk: {old_id} -> {new_id} ({n} row(s))")

    # ---- 4. Update ref_supplier_master_global PKs (crosswalk already points to new IDs). ----
    # Update in reverse new_id order to avoid temporary UNIQUE violation (e.g. A->SUP-00001, B->SUP-00002: do B first).
    to_update = [(old, new) for old, new in mapping.items() if old != new]
    to_update.sort(key=lambda x: x[1], reverse=True)
    for old_id, new_id in to_update:
        n = conn.execute(
            "UPDATE ref_supplier_master_global SET canonical_supplier_id = ? WHERE canonical_supplier_id = ?",
            (new_id, old_id),
        ).rowcount
        if n:
            print(f"Updated ref_supplier_master_global: {old_id} -> {new_id}")
    conn.commit()

    conn.close()
    print("Done. Run: python scripts/export_ref_and_client_sample.py")

if __name__ == "__main__":
    main()

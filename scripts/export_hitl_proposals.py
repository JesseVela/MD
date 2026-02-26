"""
Export HITL proposal CSVs aligned with Excel-Example-CSVs shared with Kristaq/Bhavin.
matches_proposed: client_id, upload_id, supplier_id, supplier_name_raw, supplier_name_key, match_type, match_candidate_normalized_supplier_name, match_confidence
new_suppliers_proposed: client_id, upload_id, supplier_id, supplier_name_raw, supplier_name_key, best_match_candidate, best_match_confidence
Run from project root: python scripts/export_hitl_proposals.py
Writes to data/curated/.
"""
import os
import csv
import sqlite3

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(ROOT, "db", "supplier_etl.db")
OUT_DIR = os.path.join(ROOT, "data", "curated")


def main():
    if not os.path.isfile(DB_PATH):
        print(f"DB not found: {DB_PATH}. Run db/init_db.py and run_pipeline.py first.")
        return
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    os.makedirs(OUT_DIR, exist_ok=True)

    # matches_proposed: aligned with matches_proposed_example.csv (Kristaq)
    cur = conn.execute(
        """SELECT c.client_id, c.supplier_id, c.canonical_supplier_id, c.match_method, c.match_confidence,
                  m.supplier_name_raw, g.name_key, g.normalized_supplier_name
           FROM client_acme_supplier_crosswalk c
           JOIN client_acme_supplier_master m ON c.client_id = m.client_id AND c.supplier_id = m.supplier_id
           JOIN ref_supplier_master_global g ON c.canonical_supplier_id = g.canonical_supplier_id"""
    )
    rows = cur.fetchall()
    if rows:
        path = os.path.join(OUT_DIR, "matches_proposed.csv")
        cols = ["client_id", "upload_id", "supplier_id", "supplier_name_raw", "supplier_name_key", "match_type", "match_candidate_normalized_supplier_name", "match_confidence"]
        with open(path, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=cols, extrasaction="ignore")
            w.writeheader()
            for r in rows:
                w.writerow({
                    "client_id": r["client_id"],
                    "upload_id": "UP-2026-001",
                    "supplier_id": r["supplier_id"],
                    "supplier_name_raw": r["supplier_name_raw"],
                    "supplier_name_key": r["name_key"],
                    "match_type": r["match_method"] or "exact_name_key",
                    "match_candidate_normalized_supplier_name": r["normalized_supplier_name"],
                    "match_confidence": r["match_confidence"] or 1.0,
                })
        print(f"Wrote {path} ({len(rows)} rows)")

    # new_suppliers_proposed: aligned with new_suppliers_proposed_example.csv (Kristaq)
    cur = conn.execute(
        "SELECT canonical_supplier_id, normalized_supplier_name, name_key FROM ref_supplier_master_global"
    )
    rows = cur.fetchall()
    if rows:
        path = os.path.join(OUT_DIR, "new_suppliers_proposed.csv")
        cols = ["client_id", "upload_id", "supplier_id", "supplier_name_raw", "supplier_name_key", "best_match_candidate", "best_match_confidence"]
        with open(path, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(cols)
            for r in rows:
                w.writerow(["acme", "UP-2026-002", r["canonical_supplier_id"], r["normalized_supplier_name"], r["name_key"], "", 0.0])
        print(f"Wrote {path} ({len(rows)} rows)")

    conn.close()
    print("Done. Review data/curated/ before applying to global.")


if __name__ == "__main__":
    main()

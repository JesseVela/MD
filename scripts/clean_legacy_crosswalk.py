"""One-off: remove legacy crosswalk rows (client_id = 'client_acme', S-prefix supplier_id, exact)."""
import sqlite3
import os

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(ROOT, "db", "supplier_etl.db")

conn = sqlite3.connect(DB_PATH)
n = conn.execute("DELETE FROM client_acme_supplier_crosswalk WHERE client_id = ?", ("client_acme",)).rowcount
conn.commit()
conn.close()
print(f"Deleted {n} legacy crosswalk row(s). Re-run export_ref_and_client_sample.py to refresh data/ref/client_acme_supplier_crosswalk.csv")

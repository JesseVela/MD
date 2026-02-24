"""One-off: Remove supplier_id row from ref.supplier_master in two Kristaq docs.
Run from Genpact Project root: python Supplier-etl-local/db/_fix_docs_remove_supplier_id.py
"""
import pathlib

ROOT = pathlib.Path(__file__).resolve().parent.parent.parent

for name in ["05-Full-Table-Column-Reference.md", "05-Global-Supplier-Master-Refresh-Plan.md"]:
    p = ROOT / "Meeting-Prep" / "02,-FEB" / "Docs" / name
    if not p.exists():
        continue
    text = p.read_text(encoding="utf-8")
    lines = text.splitlines()
    out = []
    for line in lines:
        # Drop the ref.supplier_master table row: | supplier_id | ... | Client's raw supplier ID — backup only ...
        if "supplier_id" in line and ("backup only" in line or "not reliable as PK" in line):
            continue
        out.append(line)
    p.write_text("\n".join(out) + "\n", encoding="utf-8")
    print(f"Fixed: {p}")

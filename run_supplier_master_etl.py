"""
Run Supplier Master ETL — full automation from transaction CSV to ref tables (and optional client schema).

Usage (from Supplier-etl-local or project root):

  # Required env: DB_HOST, DB_USERNAME, DB_PASSWORD (and optionally DB_NAME, DB_PORT)
  # Optional: GEMINI_API_KEY for enrichment (description, L1/L2/L3, product tags)

  python run_supplier_master_etl.py "path/to/transaction.csv"
  python run_supplier_master_etl.py "path/to/Invoice Report.csv" --client-id hershey --client-name "Hershey's"
  python run_supplier_master_etl.py "path/to/data.csv" --supplier-column "Supplier" --dry-run
  python run_supplier_master_etl.py "path/to/data.csv" --no-skip-enrich   # use Gemini if GEMINI_API_KEY set

Default: --skip-enrich (no Gemini calls). Set GEMINI_API_KEY and use --no-skip-enrich to run enrichment.
"""
import argparse
import json
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from etl.supplier_master_etl import run_supplier_master_etl


def main():
    ap = argparse.ArgumentParser(
        description="Supplier Master ETL: CSV → ref.supplier_master, ref.global_supplier_data_master, ref.client_master"
    )
    ap.add_argument("csv_path", type=Path, help="Path to transaction CSV (e.g. Invoice Report.csv)")
    ap.add_argument("--client-id", default=None, help="Client ID (e.g. hershey, acme). If set, creates/updates client schema and supplier_crosswalk.")
    ap.add_argument("--client-name", default=None, help="Client display name for ref.client_master")
    ap.add_argument("--supplier-column", default=None, help="CSV column name for supplier (default: auto-detect)")
    ap.add_argument("--supplier-id-column", default=None, help="CSV column name for client supplier ID")
    ap.add_argument("--skip-enrich", action="store_true", default=True, help="Skip Gemini enrichment (default: True)")
    ap.add_argument("--no-skip-enrich", action="store_false", dest="skip_enrich", help="Run Gemini enrichment (requires GEMINI_API_KEY)")
    ap.add_argument("--dry-run", action="store_true", help="Only read CSV and aggregate; do not write to DB")
    ap.add_argument("--json", action="store_true", help="Output result as JSON only")
    args = ap.parse_args()

    csv_path = args.csv_path
    if not csv_path.is_absolute():
        csv_path = (ROOT / csv_path).resolve()
    if not csv_path.is_file():
        print(f"Error: CSV not found: {csv_path}", file=sys.stderr)
        sys.exit(1)

    try:
        counts = run_supplier_master_etl(
            csv_path=csv_path,
            client_id=args.client_id,
            client_name=args.client_name or args.client_id,
            supplier_name_column=args.supplier_column,
            supplier_id_column=args.supplier_id_column,
            skip_enrich=args.skip_enrich,
            dry_run=args.dry_run,
        )
        if args.json:
            print(json.dumps(counts, indent=2))
        else:
            if counts.get("dry_run"):
                print("Dry run:", counts)
            else:
                print("Supplier Master ETL complete.")
                print(f"  Rows read: {counts.get('rows_read', 0)}")
                print(f"  Suppliers (aggregated): {counts.get('suppliers_aggregated', 0)} (new: {counts.get('suppliers_new', 0)}, existing: {counts.get('suppliers_existing', 0)})")
                print(f"  ref.supplier_master: {counts.get('ref_supplier_master_inserted', 0)}")
                print(f"  ref.global_supplier_data_master: {counts.get('ref_global_inserted', 0)}")
                if counts.get("client_master_upserted"):
                    print(f"  ref.client_master: 1")
                if counts.get("crosswalk_upserted"):
                    print(f"  client schema supplier_crosswalk: {counts['crosswalk_upserted']}")
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        if not args.json:
            raise
        sys.exit(1)


if __name__ == "__main__":
    main()

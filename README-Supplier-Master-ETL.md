# Supplier Master ETL Automation

This folder implements **Supplier Master ETL automation** as requested by Bhavin: transaction CSV → normalize → aggregate by supplier name → assign Genpact ID → write to **ref.supplier_master**, **ref.global_supplier_data_master**, **ref.client_master** (and optional client schema + supplier_crosswalk). No persistent staging table; staging is in-memory.

---

## Quick start

### 1. Set up RDS (PostgreSQL)

```bash
set DB_HOST=your-rds-host
set DB_USERNAME=your-user
set DB_PASSWORD=your-password
set DB_NAME=supplier_etl
set DB_PORT=5432
```

### 2. Initialize schema (if not already done)

```bash
cd Supplier-etl-local
python db/init_postgres_db.py
```

This creates `ref.client_master`, `ref.supplier_master`, `ref.global_supplier_data_master`, client schemas, `vec.vector_embeddings` (and tries to add pgvector + `embedding_vec` for vector load).

### 3. Run Supplier Master ETL from a transaction CSV

```bash
# From project root or Supplier-etl-local
python run_supplier_master_etl.py "Documents/Invoice Report.csv"
```

With client ID (creates/updates client schema and supplier_crosswalk):

```bash
python run_supplier_master_etl.py "Documents/Invoice Report.csv" --client-id hershey --client-name "Hershey's"
```

Optional: specify supplier column name if auto-detect fails:

```bash
python run_supplier_master_etl.py "path/to/file.csv" --supplier-column "Supplier"
```

Dry run (no DB write):

```bash
python run_supplier_master_etl.py "path/to/file.csv" --dry-run
```

### 4. (Optional) Enrichment via Gemini

Set `GEMINI_API_KEY` and run with enrichment (description, L1/L2/L3, product_service_tags from Bhavin’s script):

```bash
set GEMINI_API_KEY=your-key
python run_supplier_master_etl.py "path/to/file.csv" --no-skip-enrich
```

By default `--skip-enrich` is used so the ETL runs without calling Gemini.

### 5. Load vector embeddings (after ref tables are populated)

```bash
python db/load_vec_to_rds.py
```

Uses Bedrock Titan to embed supplier text and write to `vec.vector_embeddings`. Requires AWS credentials and `vec.vector_embeddings.embedding_vec` (from init or `db/migrate_add_pgvector.sql`).

---

## What the ETL does

1. **Read CSV** — Detects supplier name column (e.g. "Supplier", "Vendor Name", "Supplier Name") and optional supplier ID, amount, currency, item description.
2. **Normalize** — Uses `clean_name()` from Bhavin’s supplier_master_generator logic (in `etl/supplier_normalize.py`): lowercase, strip legal suffixes, unicode normalize, etc.
3. **Aggregate** — Groups rows by normalized supplier name (in-memory); collects raw names, supplier IDs, amounts, item descriptions.
4. **Assign Genpact ID** — For each unique normalized name: if it exists in `ref.supplier_master` (by normalized_supplier_name), reuse its `genpact_supplier_id`; otherwise assign next `G10001`, `G10002`, …
5. **Dedupe** — Inserts into `ref.supplier_master` with `ON CONFLICT (genpact_supplier_id) DO UPDATE`; same for `ref.global_supplier_data_master`.
6. **Optional enrichment** — If `GEMINI_API_KEY` is set and `--no-skip-enrich` is used, loads Bhavin’s `Documents/supplier_master_generator 1.py` and calls `enrich_supplier`, `generate_supplier_product_tags`, `classify_supplier` to fill description, L1/L2/L3, product_service_tags.
7. **Client** — If `--client-id` is set, ensures `ref.client_master` and creates/updates `client_<id>.supplier_crosswalk` (client supplier_id → genpact_supplier_id).

---

## Files

| File | Purpose |
|------|--------|
| `run_supplier_master_etl.py` | CLI entry point for supplier master ETL |
| `etl/supplier_master_etl.py` | Core ETL: CSV → aggregate → ref tables (and optional client crosswalk) |
| `etl/supplier_normalize.py` | `clean_name`, `get_group_key`, `classify_entity` (from Bhavin’s script, no GUI) |
| `db/init_postgres_db.py` | Create ref + client schemas and vec.vector_embeddings (+ pgvector if available) |
| `db/load_smg_combined_to_rds.py` | Load pre-built SMG CSV into ref tables (alternative to ETL from transaction CSV) |
| `db/load_vec_to_rds.py` | Embed ref suppliers via Bedrock and write to vec.vector_embeddings |
| `db/migrate_add_pgvector.sql` | Add pgvector extension and embedding_vec column (if init_postgres_db didn’t) |

---

## End-to-end test (e.g. Hershey’s)

1. Put transaction CSV in project (e.g. `Documents/Invoice Report.csv` or Hershey’s file).
2. `python run_supplier_master_etl.py "Documents/Invoice Report.csv" --client-id hershey --client-name "Hershey's"`.
3. Check `ref.supplier_master`, `ref.global_supplier_data_master`, `ref.client_master`, `client_hershey.supplier_crosswalk`.
4. Run `python db/load_vec_to_rds.py` to refresh vector embeddings.
5. Use `python db/run_semantic_search.py "query"` to test search.

---

## Local SQLite pipeline (unchanged)

`run_pipeline.py` still runs the **local SQLite** flow (parse → validate → normalize → structured_write → chunk → embed → vector_write) with `canonical_supplier_id` and `ref_supplier_master_global`. For RDS and **genpact_supplier_id**, use `run_supplier_master_etl.py` and `db/load_vec_to_rds.py` as above.

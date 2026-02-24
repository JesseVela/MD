# Perfect Local Build — Walkthrough & Checklist

This document walks through every CSV and key file in **Supplier-etl-local** and confirms the build is fully resolved and aligned with Kristaq/Bhavin (Excel-Example-CSVs) and with **Bhavin's Supplier Data ETL Workflow diagram**. Production target: **PostgreSQL on AWS (RDS)**; local uses SQLite with the same table/column names so you can swap the connection to Postgres later.

---

## 1. CSV files

### 1.1 Input (upload)

| File | Purpose | Content check |
|------|--------|----------------|
| `data/upload/acme/sample_upload.csv` | **Canonical** sample input for client `acme` | `client_id=acme`, `supplier_id` 100782, 100891; `quantity` column; 4 transaction rows. Aligned with shared example. |
| `data/upload/client_acme/sample_upload.csv` | Legacy (old format) | Optional; pipeline uses `acme/` only. Can delete to avoid confusion. See `data/upload/README.md`. |

### 1.2 Output (ref — after export)

| File | Purpose | Content check |
|------|--------|----------------|
| `data/ref/ref_clients.csv` | Reference clients | One row: `acme`, Acme Corp. Dates YYYY-MM-DD. |
| `data/ref/ref_supplier_master_global.csv` | Global canonical suppliers | `canonical_supplier_id`: SUP-00001 (Staples), SUP-00002 (IBM), SUP-00003 (Amazon). `name_key` lowercase. |
| `data/ref/client_acme_supplier_master.csv` | Client supplier master + aggregates | Rows: `acme`, 100782 and 100891. `total_spend`, `total_quantity`, `window_start_date`, `window_end_date` from Tier 1. |
| `data/ref/client_acme_supplier_crosswalk.csv` | Client → canonical mapping | `acme`, 100782→SUP-00001, 100891→SUP-00003. `match_method=exact_name_key`, `match_confidence=1.0`, `matched_on=name_key`. |
| `data/ref/client_acme_transactions_t1.csv` | Tier 1 transactions | 4 rows from sample_upload: `acme`, PO-2026-001..004, 100782/100891, amounts, categories. |

All ref CSVs use **client_id = acme** (not client_acme), **canonical_supplier_id = SUP-00001** style, and column order/names aligned with Excel-Example-CSVs.

---

## 2. Config & schema

| File | Purpose |
|------|--------|
| `config/clients.json` | Registered clients: `acme`, `beta`. |
| `config/column_mapping.json` | Upload column → snake_case (e.g. Quantity → quantity). |
| `config/settings.json` | `db_path`, `logs_path`, `vector_index_path`, `use_mock_embeddings`. |
| `schemas/tables.json` | Table definitions and mandatory columns for validation. |

---

## 3. Database

| File | Purpose |
|------|--------|
| `db/init_db.py` | Creates SQLite schema: ref_clients, ref_supplier_master_global, client_acme_* (supplier_master, supplier_crosswalk, transactions_t1/t2/t3), etl_run_logs, vector_embeddings. Seeds ref_clients (acme, beta). Migration adds `quantity` to transactions_t1 if missing. |
| `db/supplier_etl.db` | SQLite DB (created by init_db + pipeline). |

---

## 4. ETL pipeline

| File | Purpose |
|------|--------|
| `run_pipeline.py` | Orchestrator: parse → validate → normalize → structured_write → chunk → embed → vector_write → log. Usage: `python run_pipeline.py acme sample_upload.csv`. |
| `etl/parse.py` | Reads `data/upload/<client_id>/<file_name>`. Test uses `acme`. |
| `etl/validate.py` | Validates rows against schema (transactions_t1). |
| `etl/normalize.py` | Normalizes column names via config mapping. |
| `etl/structured_write.py` | Writes ref + client tables; SUP-00001 style canonical IDs; exact_name_key, 1.0, name_key; aggregates total_spend, total_quantity, window dates; ensures `quantity` column exists. |
| `etl/extract_and_chunk.py` | Builds text chunks for embedding. |
| `vector/mock_embedding.py` | Mock embeddings. |
| `vector/vector_store.py` | Writes to vector_index.db. |
| `logs/etl_logger.py` | Writes to etl_runs.jsonl. |

---

## 5. Scripts

| Script | Purpose |
|--------|--------|
| `scripts/export_ref_and_client_sample.py` | Exports ref + client_acme tables to `data/ref/*.csv` (column order/names aligned with Kristaq). |
| `scripts/export_hitl_proposals.py` | Exports HITL proposals to `data/curated/matches_proposed.csv`, `new_suppliers_proposed.csv`. |
| `scripts/clean_legacy_crosswalk.py` | One-off: deletes crosswalk rows where `client_id = 'client_acme'`. |
| `scripts/full_clean_legacy_and_migrate_sup.py` | One-off: (1) Deletes all legacy rows (client_acme) from ref_clients, client_acme_supplier_*, client_acme_transactions_t1. (2) Migrates UUID canonical IDs in ref_supplier_master_global to SUP-00001 style; updates crosswalk. Run once for a clean DB, then export. |

---

## 6. Docs & data layout

| File | Purpose |
|------|--------|
| `README.md` | Layout, quick start, runbook, local build criteria. |
| `guideline.md` | Step-by-step setup (prereqs, folder structure, run pipeline, export, troubleshooting). Uses `acme` and `data/upload/acme/`. |
| `data/upload/README.md` | Canonical upload path: `data/upload/<client_id>/`; sample = `acme`. |
| `curation/README.md` | Curation / HITL phase. |
| `data/reference-Kristaq/README.md` | Documents alignment with Meeting-Prep/Excel-Example-CSVs. |

---

## 7. Perfect local build checklist

| # | Criterion | Status |
|---|-----------|--------|
| 1 | Folders and config per README (config/, schemas/, data/upload\|ref\|curated, etl/, db/, vector/, logs/, scripts/) | Yes |
| 2 | SQLite holds ref and client tables; init_db.py creates schema and seeds acme/beta | Yes |
| 3 | One orchestrator: `run_pipeline.py` runs parse → validate → normalize → structured_write → chunk → embed → vector write → log | Yes |
| 4 | Logs: logs/etl_runs.jsonl with audit trail | Yes |
| 5 | Sample client `acme` has data in ref, client tables, and vector store after one run | Yes |
| 6 | Artifacts for Bhavin/Kristaq: data/ref/*.csv (and HITL exports) via export scripts | Yes |
| 7 | Docs explain flow and mapping to AWS/diagram | Yes |
| 8 | **CSV alignment:** client_id = acme; supplier_id 100782/100891; canonical_supplier_id SUP-00001 style; match_method exact_name_key; match_confidence 1.0; matched_on name_key; Table 3 aggregates (total_spend, total_quantity, window dates) | Yes |
| 9 | **No legacy in exports:** ref_clients and client_acme_* CSVs contain only acme rows (no client_acme); ref_supplier_master_global uses SUP-00001, SUP-00002, SUP-00003 | Yes |
| 10 | **Single canonical upload path:** data/upload/acme/sample_upload.csv; guideline and parse use acme | Yes |

---

## 8. How to get this build from scratch

1. **Init DB (once)**  
   `python db/init_db.py`

2. **Run pipeline**  
   `python run_pipeline.py acme sample_upload.csv`

3. **Export ref + client sample**  
   `python scripts/export_ref_and_client_sample.py`

4. **If you ever have legacy data (client_acme, UUID canonicals):**  
   `python scripts/full_clean_legacy_and_migrate_sup.py`  
   then run the export again.

Result: **Fully resolved local build** with CSVs and DB aligned to Kristaq/Bhavin and ready to share or extend.

---

## 9. Alignment with Bhavin's diagram and PostgreSQL on AWS

**Diagram (source of truth):** `Documents/Supplier-Data-ETL-Workflow/.md` — Mermaid flowchart: Triggers (S3 → EventBridge → Step Function) → Ingestion (Get object → Parse → Validate → Normalize) → split into Structured path (Normalization & Deduplication → **Write to Amazon RDS**: Ref Global Supplier Master, Client Supplier Table, Transactions Table) and Unstructured path (Extract text → Chunk → Titan 1536 → OpenSearch) → DynamoDB Logs.

**Other refs:** `Documents/Supplier-Data-ETL-Workflow/Diagram-to-Schema-Mapping.md`; `Alignment-with-Roadmap-05.md`; `Roadmap/05-Global-Supplier-Master-Refresh-Plan.md`.

### Diagram flow (all steps implemented locally)

| Diagram step | Local implementation | AWS (production) |
|--------------|----------------------|-------------------|
| Get object → Parse CSV/Excel | `etl/parse.py` from `data/upload/<client_id>/` | Lambda: S3 Get → Parse |
| Validate schema | `etl/validate.py` (05-Full-Table-Column-Reference) | Lambda |
| Normalize columns | `etl/normalize.py` (column_mapping.json) | Lambda |
| Normalization & deduplication → RDS | `etl/structured_write.py` → SQLite | Lambda → **PostgreSQL (RDS)** |
| Ref: Global Supplier Master | `ref_supplier_master_global` | **ref.supplier_master_global** |
| Client: Supplier Table + Crosswalk | `client_acme_supplier_master`, `client_acme_supplier_crosswalk` | **client_<id>.supplier_master**, **client_<id>.supplier_crosswalk** |
| Client: Transactions Table | `client_acme_transactions_t1` (t2, t3) | **client_<id>.transactions_t1/t2/t3** |
| Extract text → Chunk → Embed → Vector | `extract_and_chunk` → `mock_embedding` → `vector_store` | Lambda → Bedrock Titan → OpenSearch |
| Logs | `logs/etl_runs.jsonl` | DynamoDB |

### SQLite → PostgreSQL table mapping

We use **PostgreSQL on AWS (RDS)** in production. Local SQLite uses flat table names; they map 1:1 to Postgres **schema.table**:

| Local (SQLite) | PostgreSQL on AWS (RDS) |
|----------------|--------------------------|
| `ref_clients` | **ref.clients** |
| `ref_supplier_master_global` | **ref.supplier_master_global** |
| `client_acme_supplier_master` | **client_acme.supplier_master** |
| `client_acme_supplier_crosswalk` | **client_acme.supplier_crosswalk** |
| `client_acme_transactions_t1` (t2, t3) | **client_acme.transactions_t1** (t2, t3) |

Column names and types match Roadmap 05 / 05-Full-Table-Column-Reference. When moving to Postgres: use schemas `ref` and `client_<client_id>`, same table and column names; adjust only connection string, dialect (e.g. SERIAL vs AUTOINCREMENT, JSONB where used), and any Postgres-specific constraints.

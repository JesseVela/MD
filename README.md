# Supplier ETL Local

Local reference build of **Bhavin's Supplier Data ETL Workflow**. Validates logic, schema, and flow without cloud access. **Aligned with the diagram** in `Documents/Supplier-Data-ETL-Workflow/.md`; production target is **PostgreSQL on AWS (RDS)**. Same table/column names locally (SQLite) so you can swap the connection to Postgres; other AWS equivalents: S3, Lambda, RDS, Bedrock, OpenSearch (see LOCAL_BUILD.md §9).

**Source:** `To-do-Lists/04,-FEB/Done/Start-local-build/Local-Reference-Build.md`

---

## Layout

```
Supplier-etl-local/
  config/           column_mapping.json, clients.json, settings.json
  data/
    upload/         raw uploads by client (S3-style); client_id acme = folder acme
      acme/         sample_upload.csv (aligned with Excel-Example-CSVs shared with Kristaq)
    transactions/   staged (optional)
    curated/        curated output + HITL CSVs (Phase 2 trigger)
    ref/            sample ref and client table exports (CSV)
  schemas/          tables.json (validation)
  db/               supplier_etl.db + init_db.py
  etl/              parse, validate, normalize, structured_write, extract_and_chunk
  curation/         Phase 1 curation (README + HITL export via scripts)
  vector/           mock_embedding.py, vector_store.py
  logs/             etl_runs.jsonl
  scripts/          export_ref_and_client_sample.py, export_hitl_proposals.py
  run_pipeline.py   orchestrator (one command for full run)
```

---

## Quick start

1. **Initialize DB (once)**  
   From project root:
   ```bash
   python db/init_db.py
   ```

2. **Run full pipeline**  
   Uses sample file `data/upload/acme/sample_upload.csv` (client_id **acme** aligned with Kristaq):
   ```bash
   python run_pipeline.py acme sample_upload.csv
   ```
   This will: parse → validate → normalize → write to SQLite (ref + client tables) → extract text → chunk → mock embed → write vectors → log each stage.

3. **Check results**  
   - **Logs:** `logs/etl_runs.jsonl`  
   - **DB:** `db/supplier_etl.db` (ref_clients, ref_supplier_master_global, client_acme_*, vector_embeddings)  
   - **Vectors:** `vector/vector_index.db`

4. **Export sample tables for review (optional)**  
   ```bash
   python scripts/export_ref_and_client_sample.py   # -> data/ref/*.csv
   python scripts/export_hitl_proposals.py          # -> data/curated/matches_proposed.csv, new_suppliers_proposed.csv
   ```

---

## What each step does

| Step              | Local implementation              | AWS equivalent        |
|-------------------|------------------------------------|------------------------|
| Upload            | Put CSV in `data/upload/<client_id>/` | S3 bucket/prefix       |
| Parse             | `etl/parse.py`                    | Lambda: Get object, Parse |
| Validate          | `etl/validate.py`                 | Lambda: Validate schema |
| Normalize         | `etl/normalize.py`                | Lambda: Normalize columns |
| Structured write  | `etl/structured_write.py` → SQLite | Lambda → RDS          |
| Extract + chunk   | `etl/extract_and_chunk.py`        | Lambda: Extract text, Chunk |
| Embed             | `vector/mock_embedding.py`        | Bedrock Titan         |
| Vector write      | `vector/vector_store.py` → SQLite | OpenSearch            |
| Log               | `logs/etl_logger.py` → logs/      | DynamoDB               |

---

## Adding another client

1. Add client to `config/clients.json`.  
2. Create folder `data/upload/<client_id>/` and add a CSV (columns per `config/column_mapping.json`).  
3. Run: `python run_pipeline.py <client_id> <file_name>`.  
4. To add DB tables for the new client, extend `db/init_db.py` with the new `client_<id>_*` tables (or use a generic schema and pass client_id).

---

## Requirements

- **Python 3.10+** (uses standard library only for CSV, SQLite, logging; no pip install required for basic run).

Optional later: `openpyxl` for Excel, `pandas` for richer transforms.

---

## Local Build Complete (per Local Reference Build section 7)

| # | Criterion | Status |
|---|-----------|--------|
| 1 | Folders and config per section 2; schema and column-mapping in place | Yes: config/, schemas/, data/upload|transactions|curated|ref/, etl/, curation/, vector/, logs/, db/ |
| 2 | SQLite holds ref and client tables per Roadmap 05 | Yes: db/init_db.py + supplier_etl.db |
| 3 | Runbook steps 4.1–4.10 runnable in order via one orchestrator | Yes: run_pipeline.py (parse → validate → normalize → structured write → extract → chunk → mock embed → vector write → log) |
| 4 | Logs show one full run with audit trail | Yes: logs/etl_runs.jsonl |
| 5 | At least one sample client has data in ref, client tables, and vector store | Yes: client_acme after one run |
| 6 | Artifacts for Bhavin available (this doc, folder structure, schemas/config, logs, sample table exports) | Yes: data/ref/*.csv via scripts/export_ref_and_client_sample.py; scripts/export_hitl_proposals.py for HITL |
| 7 | Documentation explains flow and 1:1 mapping to diagram and AWS | Yes: this README + source To-do-Lists/04,-FEB/Done/Start-local-build/Local-Reference-Build.md |

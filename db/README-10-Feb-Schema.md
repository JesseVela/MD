# 10-Feb schema — canonical table list (RDS)

After cleanup and init, RDS should have **only** these tables. No duplicates, no legacy names.

## ref (3 tables only)

| Table | Purpose |
|-------|---------|
| **client_master** | client_id, client_name, client_industry?, date_added |
| **supplier_master** | genpact_supplier_id (PK), normalized_supplier_name, supplier_id?, date_added |
| **global_supplier_data_master** | genpact_supplier_id (FK), enrichment columns, date_added |

**Legacy (must not exist):** ref.clients, ref.supplier_master_global

---

## client_acme, client_beta (5 tables each)

| Table | Purpose |
|-------|---------|
| **client_supplier_data_master** | Rolling 12-month spend; client_id, genpact_supplier_id, spend_year, total_spend, etc. |
| **supplier_crosswalk** | client_id, supplier_id → genpact_supplier_id |
| **transactions_t1** | Tier 1; must have genpact_supplier_id |
| **transactions_t2** | JSONB extras |
| **transactions_t3** | Raw upload |

**Legacy (must not exist):** client_<id>.supplier_master (old client-level table; 10-Feb uses client_supplier_data_master)

---

## vec (1 table)

| Table | Purpose |
|-------|---------|
| **vector_embeddings** | client_id, genpact_supplier_id, chunk_id, embedding, source_text, indexed_at |

---

## public (1 table)

| Table | Purpose |
|-------|---------|
| **etl_run_logs** | run_id, stage, status, row_count, error_message, timestamp |

---

## Full cleanup (one-time)

```powershell
cd "d:\work\Genpact\Genpact Project\Supplier-etl-local"
# Same env as init: DB_HOST, DB_USERNAME, DB_PASSWORD, DB_NAME, DB_PORT

python db/drop_all_legacy_tables.py
python db/init_postgres_db.py
```

Then optionally: `python db/seed_rds_demo_data.py` and/or `python db/load_smg_combined_to_rds.py` for data.

# Step-by-Step Guide: Build Supplier ETL Local on Your Company Laptop

Use this guide to build the **exact same structure** as the reference build on your company laptop. No cloud access required.

---

## Step 1. Prerequisites

| Requirement | What to do |
|-------------|------------|
| **Python 3.10 or newer** | Install from [python.org](https://www.python.org/downloads/) or your company’s standard. Ensure **Add Python to PATH** is checked. |
| **Terminal** | Use **PowerShell** or **Command Prompt** (Windows). |

**Check Python:**

```powershell
python --version
```

You should see something like `Python 3.10.x` or higher. No `pip install` is required for the basic run (standard library only).

---

## Step 2. Get the Project onto Your Laptop

**Option A – Copy the whole folder (recommended)**

1. Copy the entire **Supplier-etl-local** folder from your other machine (USB, OneDrive, shared drive, or from the Genpact Project repo).
2. Put it where you want on the laptop, e.g.  
   `C:\Users\<YourName>\Documents\Genpact Project\Supplier-etl-local`  
   or  
   `D:\work\Genpact\Genpact Project\Supplier-etl-local`.
3. You can **exclude** the `.venv` folder when copying (you can create a new one on the laptop if you use venvs).  
4. You can **exclude** `db/supplier_etl.db` and `vector/vector_index.db` and `logs/etl_runs.jsonl` and `data/ref/*.csv` if you prefer a clean start; Step 4 will recreate them.

**Option B – You already have Genpact Project on the laptop**

1. If the full **Genpact Project** (including **Supplier-etl-local**) is already on the laptop, go to that folder and continue from Step 3.

---

## Step 3. Confirm Folder Structure

From the **Supplier-etl-local** folder (project root), you should have this layout:

```
Supplier-etl-local/
  config/
    clients.json
    column_mapping.json
    settings.json
  data/
    upload/
      acme/
        sample_upload.csv
    transactions/     (can be empty; .gitkeep optional)
    curated/          (can be empty)
    ref/              (can be empty until after export script)
  schemas/
    tables.json
  db/
    init_db.py
  etl/
    __init__.py
    parse.py
    validate.py
    normalize.py
    structured_write.py
    extract_and_chunk.py
  curation/
    README.md
  vector/
    __init__.py
    mock_embedding.py
    vector_store.py
  logs/
    __init__.py
    etl_logger.py
  scripts/
    export_ref_and_client_sample.py
    export_hitl_proposals.py
  run_pipeline.py
  README.md
  requirements.txt
  guideline.md        (this file)
```

If you copied the folder, this should already be there. If anything is missing, copy it from the reference machine or repo.

---

## Step 4. Run the Build (in order)

Open PowerShell (or Command Prompt). All commands are run from the **Supplier-etl-local** folder (project root).

### 4.1 Go to project root

```powershell
Set-Location "C:\Users\<YourName>\Documents\Genpact Project\Supplier-etl-local"
```

(Replace the path with your actual path to **Supplier-etl-local**.)

On PowerShell you can also:

```powershell
cd "C:\path\to\Supplier-etl-local"
```

### 4.2 Initialize the database (once)

```powershell
python db/init_db.py
```

**Expected output:**  
`Database initialized: ...\Supplier-etl-local\db\supplier_etl.db`

This creates `db/supplier_etl.db` and all tables (ref_clients, ref_supplier_master_global, client_acme_*, etl_run_logs, vector_embeddings).

### 4.3 Run the full pipeline

```powershell
python run_pipeline.py acme sample_upload.csv
```

**Expected:** A JSON summary with `"status": "ok"` for stages: parse, validate, normalize, structured_write, chunk, embed, vector_write.

This will:

- Read `data/upload/acme/sample_upload.csv`
- Parse → validate → normalize
- Write to SQLite (ref + client_acme tables)
- Extract text → chunk → mock embed → write vectors
- Append log entries to `logs/etl_runs.jsonl`

### 4.4 Export sample tables for review (optional)

```powershell
python scripts/export_ref_and_client_sample.py
```

**Expected:** Messages like  
`Exported ref_clients -> ...\data\ref\ref_clients.csv (1 rows)`  
and similar for other tables. CSVs appear in `data/ref/`.

```powershell
python scripts/export_hitl_proposals.py
```

**Expected:** Messages that `matches_proposed.csv` and `new_suppliers_proposed.csv` were written to `data/curated/`.

---

## Step 5. Verify the Build

| Check | Where | What to see |
|-------|--------|-------------|
| Database | `db/supplier_etl.db` | File exists (can open with SQLite browser or leave as-is). |
| Logs | `logs/etl_runs.jsonl` | One line per stage (parse, validate, normalize, structured_write, chunk, embed, vector_write) with run_id, stage, status, timestamp. |
| Vectors | `vector/vector_index.db` | File exists after pipeline run. |
| Sample CSVs | `data/ref/` | After export script: ref_clients.csv, ref_supplier_master_global.csv, client_acme_supplier_master.csv, client_acme_supplier_crosswalk.csv, client_acme_transactions_t1.csv. |
| HITL CSVs | `data/curated/` | After HITL script: matches_proposed.csv, new_suppliers_proposals.csv. |

---

## Step 6. Run Again Later (after first build)

Whenever you want to run the pipeline again from the same folder:

```powershell
Set-Location "C:\path\to\Supplier-etl-local"
python run_pipeline.py acme sample_upload.csv
```

You do **not** need to run `db/init_db.py` again unless you delete `db/supplier_etl.db` or add a new client and extend `db/init_db.py`.

---

## Step 7. If Something Fails

| Issue | What to do |
|-------|------------|
| `python` not found | Reinstall Python and tick “Add Python to PATH”, or use the full path to `python.exe`. |
| `FileNotFoundError` for CSV | Confirm `data/upload/acme/sample_upload.csv` exists and the path in the command is correct. |
| `disk I/O error` or SQLite error | Run PowerShell as normal user (no special sandbox); ensure the folder is writable. |
| Validation errors (e.g. missing upload_id) | The pipeline injects `upload_id` automatically; ensure you use the latest `run_pipeline.py` from the reference build. |
| Wrong or missing folders | Compare with Step 3 and copy missing files/folders from the reference machine. |

---

## Step 8. One-Page Command Summary

Run these from **Supplier-etl-local** (replace the path with yours):

```powershell
Set-Location "D:\work\Genpact\Genpact Project\Supplier-etl-local"
python db/init_db.py
python run_pipeline.py acme sample_upload.csv
python scripts/export_ref_and_client_sample.py
python scripts/export_hitl_proposals.py
```

After that, you have the same structure and data as the reference build and can share the folder or artifacts (e.g. `data/ref/*.csv`, `logs/etl_runs.jsonl`) for review.

---

## Reference

- **Local Reference Build spec:** `To-do-Lists/04,-FEB/Done/Start-local-build/Local-Reference-Build.md` (in Genpact Project).
- **This project’s README:** `Supplier-etl-local/README.md`.

# Curation (Phase 1)

**Purpose:** Curation engine (normalize, match, enrich, HITL) per Local Reference Build section 2.  
**AWS equivalent:** Curation logic later may run on ECS/Glue/Lambda.

**In this local build:**

- **Normalize, match, dedupe** are implemented in `etl/structured_write.py` and run from `run_pipeline.py` (structured path).
- **HITL outputs** (matches_proposed.csv, new_suppliers_proposed.csv) are produced by:
  ```bash
  python scripts/export_hitl_proposals.py
  ```
  (Run from project root. Writes to `data/curated/`.)
  That script reads from SQLite and writes CSV to `data/curated/` for human review before applying to ref.supplier_master_global.

**To add later:** Enrichment (description, L1/L2/L3 from external source); full HITL UI. For "local build complete," CSV review is sufficient per Local Reference Build assumptions.

"""
Orchestrator: run full local ETL pipeline (Step Function + Lambda equivalent).
Order: parse → validate → normalize → structured write → extract/chunk → mock embed → vector write → log each stage.
Usage: python run_pipeline.py [client_id] [file_name]
Example: python run_pipeline.py acme sample_upload.csv
"""
import os
import sys
import json
import uuid
from datetime import datetime, timezone

# Project root
ROOT = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, ROOT)

from etl.parse import parse_upload
from etl.validate import validate
from etl.normalize import normalize
from etl.structured_write import write_structured, get_conn, _table_prefix
from etl.extract_and_chunk import build_chunks
from logs.etl_logger import log_run
from vector.mock_embedding import embed_texts
from vector.vector_store import write_embeddings_batch


def load_client_name(client_id: str) -> str:
    path = os.path.join(ROOT, "config", "clients.json")
    with open(path, encoding="utf-8") as f:
        data = json.load(f)
    for c in data.get("clients", []):
        if c.get("client_id") == client_id:
            return c.get("client_name", client_id)
    return client_id


def ensure_db():
    """Initialize DB if not present."""
    db_path = os.path.join(ROOT, "db", "supplier_etl.db")
    if not os.path.isfile(db_path):
        from db.init_db import init_db
        init_db(db_path)


def run_pipeline(client_id: str, file_name: str, upload_id: str = None) -> dict:
    ensure_db()
    upload_id = upload_id or datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    run_id = str(uuid.uuid4())[:8]
    result = {"run_id": run_id, "client_id": client_id, "file_name": file_name, "upload_id": upload_id, "stages": {}}

    # 1. Parse
    try:
        rows_raw, columns = parse_upload(client_id, file_name)
        for row in rows_raw:
            row["upload_id"] = row.get("upload_id") or upload_id
        result["stages"]["parse"] = {"status": "ok", "row_count": len(rows_raw)}
        log_run(run_id, "parse", "ok", row_count=len(rows_raw))
    except Exception as e:
        result["stages"]["parse"] = {"status": "error", "error": str(e)}
        log_run(run_id, "parse", "error", error_message=str(e))
        return result

    # 2. Validate
    valid_rows, errors = validate(rows_raw, "transactions_t1")
    if errors:
        result["stages"]["validate"] = {"status": "error", "errors": errors}
        log_run(run_id, "validate", "error", error_message="; ".join(errors[:3]))
    else:
        result["stages"]["validate"] = {"status": "ok", "row_count": len(valid_rows)}
        log_run(run_id, "validate", "ok", row_count=len(valid_rows))

    # 3. Normalize
    rows_norm = normalize(valid_rows)
    result["stages"]["normalize"] = {"status": "ok", "row_count": len(rows_norm)}
    log_run(run_id, "normalize", "ok", row_count=len(rows_norm))

    db_path = os.path.join(ROOT, "db", "supplier_etl.db")
    # 4. Structured write (ref + client tables)
    try:
        client_name = load_client_name(client_id)
        counts = write_structured(client_id, upload_id, rows_norm, client_name=client_name, db_path=db_path)
        result["stages"]["structured_write"] = {"status": "ok", "counts": counts}
        log_run(run_id, "structured_write", "ok", row_count=counts.get("transactions_t1_inserted", 0))
    except Exception as e:
        result["stages"]["structured_write"] = {"status": "error", "error": str(e)}
        log_run(run_id, "structured_write", "error", error_message=str(e))
        return result

    # 5. Enrich rows with canonical_supplier_id for vector path (read from DB)
    conn = get_conn(db_path)
    conn.row_factory = lambda c, r: dict(zip([col[0] for col in c.description], r))
    prefix = _table_prefix(client_id)
    tbl_cw = f"{prefix}_supplier_crosswalk"
    cur = conn.execute(f"SELECT supplier_id, canonical_supplier_id FROM {tbl_cw}")
    id_map = {str(r["supplier_id"]): r["canonical_supplier_id"] for r in cur.fetchall()}
    conn.close()
    for r in rows_norm:
        r["canonical_supplier_id"] = id_map.get(str((r.get("supplier_id") or "")).strip())

    # 6. Extract text + chunk
    chunks = build_chunks(rows_norm, client_id)
    result["stages"]["chunk"] = {"status": "ok", "chunk_count": len(chunks)}
    log_run(run_id, "chunk", "ok", row_count=len(chunks))

    # 7. Mock embed
    texts = [c["source_text"] for c in chunks]
    vectors = embed_texts(texts)
    for c, vec in zip(chunks, vectors):
        c["vector"] = vec
    result["stages"]["embed"] = {"status": "ok", "count": len(vectors)}
    log_run(run_id, "embed", "ok", row_count=len(vectors))

    # 8. Vector write
    entries = [
        {
            "client_id": c["client_id"],
            "supplier_id": c["supplier_id"],
            "canonical_supplier_id": c.get("canonical_supplier_id"),
            "source_text": c["source_text"],
            "vector": c["vector"],
        }
        for c in chunks
    ]
    write_embeddings_batch(entries)
    result["stages"]["vector_write"] = {"status": "ok", "count": len(entries)}
    log_run(run_id, "vector_write", "ok", row_count=len(entries))

    return result


if __name__ == "__main__":
    client_id = sys.argv[1] if len(sys.argv) > 1 else "acme"
    file_name = sys.argv[2] if len(sys.argv) > 2 else "sample_upload.csv"
    res = run_pipeline(client_id, file_name)
    print(json.dumps(res, indent=2))

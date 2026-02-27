"""
Load supplier embeddings from RDS ref tables into vec.vector_embeddings using AWS Bedrock (Titan).

Flow:
  1. Read ref.supplier_master JOIN ref.global_supplier_data_master from RDS.
  2. Build source_text per supplier (name + description + L1/L2/L3 + product_service_tags).
  3. Call Amazon Bedrock Titan Embeddings G1 (amazon.titan-embed-text-v1, 1536 dim) in parallel.
  4. Write to vec.vector_embeddings (client_id, genpact_supplier_id, chunk_id, embedding BYTEA, source_text, indexed_at).

Uses env: DB_HOST, DB_USERNAME, DB_PASSWORD, DB_NAME, DB_PORT, AWS_REGION (default us-east-1).
Requires: pip install psycopg2-binary boto3

  cd Supplier-etl-local
  set DB_HOST=... set DB_NAME=... set DB_USERNAME=... set DB_PASSWORD=...
  python db/load_vec_to_rds.py                  # all suppliers, 10 parallel workers
  python db/load_vec_to_rds.py --limit 100      # test with first 100 only
  python db/load_vec_to_rds.py --workers 20     # faster, 20 concurrent Bedrock calls
  python db/load_vec_to_rds.py --skip-existing  # resume: skip already-embedded suppliers
  python db/load_vec_to_rds.py --no-verify-ssl # if you get SSL CERTIFICATE_VERIFY_FAILED (e.g. corporate proxy)
  set LOAD_VEC_NO_VERIFY_SSL=1 & python db/load_vec_to_rds.py   # same, via env (no flag)
  Note: boto3.client() does NOT accept verify=False; use --no-verify-ssl or LOAD_VEC_NO_VERIFY_SSL instead.
"""
import json
import os
import ssl
import struct
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

# Apply SSL skip *before* boto3 is imported (boto3 uses urllib3 for HTTPS)
# Use --no-verify-ssl on the command line, or set env LOAD_VEC_NO_VERIFY_SSL=1
def _make_unverified_ssl_context():
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    return ctx

_do_no_verify_ssl = "--no-verify-ssl" in sys.argv or os.environ.get("LOAD_VEC_NO_VERIFY_SSL", "")
if _do_no_verify_ssl:
    ssl._create_default_https_context = ssl._create_unverified_context
    try:
        import urllib3.util.ssl_ as _urllib3_ssl
        _urllib3_ssl.create_urllib3_context = _make_unverified_ssl_context
    except Exception:
        pass
    try:
        import urllib3.util.ssl_ as _u3
        _orig_ssl_wrap = getattr(_u3, "ssl_wrap_socket", None)
        if _orig_ssl_wrap is not None:
            def _wrap_no_verify(sock, *a, **kw):
                kw["ssl_context"] = kw.get("ssl_context") or _make_unverified_ssl_context()
                return _orig_ssl_wrap(sock, *a, **kw)
            _u3.ssl_wrap_socket = _wrap_no_verify
    except Exception:
        pass

try:
    import psycopg2
except ImportError:
    print("psycopg2 is required. Install with: pip install psycopg2-binary", file=sys.stderr)
    sys.exit(1)
try:
    import boto3
except ImportError:
    print("boto3 is required for Bedrock. Install with: pip install boto3", file=sys.stderr)
    sys.exit(1)

DB_HOST = os.environ.get("DB_HOST", "localhost")
DB_USERNAME = os.environ.get("DB_USERNAME", "")
DB_PASSWORD = os.environ.get("DB_PASSWORD", "")
DB_NAME = os.environ.get("DB_NAME", "supplier_etl")
DB_PORT = os.environ.get("DB_PORT", "5432")
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")

# Bedrock Titan Embeddings G1 - Text: 1536 dimensions (use ID without :0 per AWS docs)
BEDROCK_MODEL_ID = "amazon.titan-embed-text-v1"
EMBED_DIM = 1536

# client_id for reference/global supplier embeddings (no client-specific slice)
GLOBAL_CLIENT_ID = "global"


def _build_source_text(row: dict) -> str:
    """Concatenate supplier fields for embedding (name, description, categories, tags)."""
    parts = []
    if row.get("normalized_supplier_name"):
        parts.append(str(row["normalized_supplier_name"]))
    if row.get("supplier_description"):
        parts.append(str(row["supplier_description"]))
    for col in ("l1_category", "l2_category", "l3_category"):
        if row.get(col):
            parts.append(str(row[col]))
    if row.get("product_service_tags"):
        parts.append(str(row["product_service_tags"]))
    text = " ".join(parts).strip()
    return text if text else row.get("normalized_supplier_name") or ""


def _embed_bedrock(text: str, client) -> list:
    """Call Bedrock Titan Embeddings v1; return list of 1536 floats."""
    if not text or not text.strip():
        return [0.0] * EMBED_DIM
    body = json.dumps({"inputText": text.strip()[:8000]})  # stay within token limit
    response = client.invoke_model(
        modelId=BEDROCK_MODEL_ID,
        accept="application/json",
        contentType="application/json",
        body=body,
    )
    out = json.loads(response["body"].read())
    emb = out.get("embedding")
    if not emb or len(emb) != EMBED_DIM:
        raise ValueError(f"Bedrock returned embedding length {len(emb) if emb else 0}, expected {EMBED_DIM}")
    return emb


def _float_list_to_bytes(vec: list) -> bytes:
    """Pack 1536 floats as little-endian float32 for BYTEA."""
    return struct.pack(f"{len(vec)}f", *vec)


def _float_list_to_pgvector(vec: list) -> str:
    """Format 1536 floats as pgvector literal: '[a,b,c,...]' for embedding_vec column."""
    return "[" + ",".join(str(float(x)) for x in vec) + "]"


def _embed_one(row: dict, client) -> tuple:
    """One supplier -> (genpact_supplier_id, blob, vec_list, source_text) or (gid, None, None, err_msg) on error."""
    gid = row["genpact_supplier_id"]
    source_text = _build_source_text(row)
    try:
        emb = _embed_bedrock(source_text, client)
        return (gid, _float_list_to_bytes(emb), emb, source_text[:10000])
    except Exception as e:
        return (gid, None, None, str(e))


def fetch_suppliers(cur) -> list:
    """SELECT suppliers with optional global_supplier_data_master fields."""
    cur.execute("""
        SELECT
            s.genpact_supplier_id,
            s.normalized_supplier_name,
            g.supplier_description,
            g.l1_category,
            g.l2_category,
            g.l3_category,
            g.product_service_tags
        FROM ref.supplier_master s
        LEFT JOIN ref.global_supplier_data_master g ON g.genpact_supplier_id = s.genpact_supplier_id
        ORDER BY s.genpact_supplier_id
    """)
    columns = [d[0] for d in cur.description]
    return [dict(zip(columns, row)) for row in cur.fetchall()]


def fetch_existing_vec_ids(cur) -> set:
    """Return set of (client_id, genpact_supplier_id, chunk_id) already in vec.vector_embeddings."""
    cur.execute("""
        SELECT client_id, genpact_supplier_id, chunk_id FROM vec.vector_embeddings
        WHERE client_id = %s
    """, (GLOBAL_CLIENT_ID,))
    return {(r[0], r[1], r[2]) for r in cur.fetchall()}


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Load supplier embeddings from RDS ref tables into vec.vector_embeddings via Bedrock.")
    parser.add_argument("--limit", type=int, default=0, help="Process only first N suppliers (0 = all). Use for testing.")
    parser.add_argument("--workers", type=int, default=10, help="Concurrent Bedrock embedding calls (default 10).")
    parser.add_argument("--skip-existing", action="store_true", help="Skip suppliers already in vec.vector_embeddings (resume).")
    parser.add_argument("--no-verify-ssl", action="store_true", help="Disable SSL cert verification for Bedrock (use if you get CERTIFICATE_VERIFY_FAILED).")
    args = parser.parse_args()

    if args.no_verify_ssl:
        print("SSL verification disabled for this run (--no-verify-ssl).")

    if not DB_USERNAME:
        print("Set DB_USERNAME, DB_PASSWORD, DB_HOST, DB_NAME (and optionally DB_PORT, AWS_REGION).", file=sys.stderr)
        sys.exit(1)

    pg_conn = psycopg2.connect(
        host=DB_HOST,
        dbname=DB_NAME,
        user=DB_USERNAME,
        password=DB_PASSWORD,
        port=DB_PORT,
    )
    pg_conn.autocommit = False
    cur = pg_conn.cursor()

    suppliers = fetch_suppliers(cur)
    if not suppliers:
        print("No rows in ref.supplier_master. Load ref data first (e.g. load_smg_combined_to_rds.py).")
        cur.close()
        pg_conn.close()
        return

    chunk_id = "0"
    if args.skip_existing:
        existing = fetch_existing_vec_ids(cur)
        before = len(suppliers)
        suppliers = [r for r in suppliers if (GLOBAL_CLIENT_ID, r["genpact_supplier_id"], chunk_id) not in existing]
        print(f"Skipping {before - len(suppliers)} already in vec; {len(suppliers)} to process.")
    if not suppliers:
        print("Nothing to do (all already embedded or --limit 0).")
        cur.close()
        pg_conn.close()
        return

    if args.limit > 0:
        suppliers = suppliers[: args.limit]
        print(f"Limiting to first {len(suppliers)} suppliers.")

    total = len(suppliers)
    print(f"Embedding {total} suppliers with {args.workers} workers...")

    # Create Bedrock client (boto3 does not support verify=False; SSL skip is done via patch above)
    try:
        bedrock = boto3.client("bedrock-runtime", region_name=AWS_REGION)
    except Exception as e:
        print(f"AWS/Bedrock client failed: {e}", file=sys.stderr)
        print("Set AWS credentials (aws configure, or AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY, or AWS_PROFILE for SSO). See db/README-AWS-Credentials-for-Bedrock.md.", file=sys.stderr)
        sys.exit(1)
    # Fail fast: one test call so we don't run 1000+ Bedrock calls only to find credentials missing
    try:
        _embed_bedrock("test", bedrock)
    except Exception as e:
        err_msg = str(e).strip().lower()
        if "credential" in err_msg or "unable to locate" in err_msg:
            print("Bedrock call failed: no valid AWS credentials.", file=sys.stderr)
            print("Configure credentials (aws configure, or env vars, or AWS_PROFILE for SSO). See db/README-AWS-Credentials-for-Bedrock.md.", file=sys.stderr)
            sys.exit(1)
        raise
    now = datetime.now(timezone.utc)
    inserted = 0
    errors = []

    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = {executor.submit(_embed_one, row, bedrock): row for row in suppliers}
        done = 0
        for future in as_completed(futures):
            done += 1
            if done % 50 == 0 or done == total:
                print(f"  Bedrock: {done}/{total} ...")
            try:
                gid, blob, vec_list, source_text = future.result()
                if blob is None:
                    errors.append((gid, source_text))  # on error, source_text is the error message
                    continue
                vec_str = _float_list_to_pgvector(vec_list) if vec_list else None
                # Write to both embedding (BYTEA) and embedding_vec (pgvector) when column exists
                cur.execute("""
                    INSERT INTO vec.vector_embeddings
                        (client_id, genpact_supplier_id, chunk_id, embedding, embedding_vec, source_text, indexed_at)
                    VALUES (%s, %s, %s, %s, %s::vector(1536), %s, %s)
                    ON CONFLICT (client_id, genpact_supplier_id, chunk_id)
                    DO UPDATE SET
                        embedding = EXCLUDED.embedding,
                        embedding_vec = EXCLUDED.embedding_vec,
                        source_text = EXCLUDED.source_text,
                        indexed_at = EXCLUDED.indexed_at
                """, (GLOBAL_CLIENT_ID, gid, chunk_id, blob, vec_str, source_text, now))
                inserted += 1
            except Exception as e:
                errors.append((None, str(e)))

    pg_conn.commit()
    cur.close()
    pg_conn.close()

    print(f"Done. Inserted/updated {inserted} rows in vec.vector_embeddings (client_id={GLOBAL_CLIENT_ID}).")
    if errors:
        print(f"Errors ({len(errors)}):", file=sys.stderr)
        for gid, msg in errors[:10]:
            print(f"  {gid}: {msg}", file=sys.stderr)
        if len(errors) > 10:
            print(f"  ... and {len(errors) - 10} more", file=sys.stderr)


if __name__ == "__main__":
    main()

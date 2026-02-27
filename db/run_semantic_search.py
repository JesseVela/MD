"""
Run semantic search on vec.vector_embeddings using a text query.

Flow:
  1. Embed the query string with Bedrock Titan (same as load_vec_to_rds).
  2. Query vec.vector_embeddings by cosine distance (<=>), return top K.

Usage (from Supplier-etl-local, with DB_* and AWS_REGION set):
  python db/run_semantic_search.py "IT hardware reseller North America"
  python db/run_semantic_search.py "janitorial services" --top 10

Requires: vec.vector_embeddings.embedding_vec column (run migrate_add_pgvector.sql first).
"""
import json
import os
import sys

try:
    import psycopg2
except ImportError:
    print("psycopg2 required: pip install psycopg2-binary", file=sys.stderr)
    sys.exit(1)
try:
    import boto3
except ImportError:
    print("boto3 required for Bedrock: pip install boto3", file=sys.stderr)
    sys.exit(1)

DB_HOST = os.environ.get("DB_HOST", "localhost")
DB_USERNAME = os.environ.get("DB_USERNAME", "")
DB_PASSWORD = os.environ.get("DB_PASSWORD", "")
DB_NAME = os.environ.get("DB_NAME", "supplier_etl")
DB_PORT = os.environ.get("DB_PORT", "5432")
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")

BEDROCK_MODEL_ID = "amazon.titan-embed-text-v1"
EMBED_DIM = 1536
GLOBAL_CLIENT_ID = "global"


def embed_query(text: str, client) -> list:
    """Return 1536-dim embedding for query text."""
    if not text or not text.strip():
        return [0.0] * EMBED_DIM
    body = json.dumps({"inputText": text.strip()[:8000]})
    response = client.invoke_model(
        modelId=BEDROCK_MODEL_ID,
        accept="application/json",
        contentType="application/json",
        body=body,
    )
    out = json.loads(response["body"].read())
    emb = out.get("embedding")
    if not emb or len(emb) != EMBED_DIM:
        raise ValueError(f"Bedrock returned len {len(emb) if emb else 0}, expected {EMBED_DIM}")
    return emb


def vec_to_pg_str(vec: list) -> str:
    return "[" + ",".join(str(float(x)) for x in vec) + "]"


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Semantic search over vec.vector_embeddings.")
    parser.add_argument("query", nargs="?", default="", help="Search phrase (or leave empty to read from stdin).")
    parser.add_argument("--top", type=int, default=5, help="Number of results (default 5).")
    args = parser.parse_args()

    query = args.query.strip()
    if not query:
        query = sys.stdin.read().strip()
    if not query:
        print("Provide a query: python db/run_semantic_search.py \"your phrase\"", file=sys.stderr)
        sys.exit(1)

    if not DB_USERNAME:
        print("Set DB_HOST, DB_NAME, DB_USERNAME, DB_PASSWORD (and optionally DB_PORT, AWS_REGION).", file=sys.stderr)
        sys.exit(1)

    # 1. Embed query
    bedrock = boto3.client("bedrock-runtime", region_name=AWS_REGION)
    try:
        query_vec = embed_query(query, bedrock)
    except Exception as e:
        print(f"Bedrock embedding failed: {e}", file=sys.stderr)
        sys.exit(1)

    vec_str = vec_to_pg_str(query_vec)

    # 2. Search DB (cosine distance: smaller = more similar)
    conn = psycopg2.connect(
        host=DB_HOST,
        dbname=DB_NAME,
        user=DB_USERNAME,
        password=DB_PASSWORD,
        port=DB_PORT,
    )
    cur = conn.cursor()
    cur.execute("""
        SELECT genpact_supplier_id, source_text,
               (embedding_vec <=> %s::vector(1536)) AS cosine_distance
        FROM vec.vector_embeddings
        WHERE client_id = %s AND embedding_vec IS NOT NULL
        ORDER BY embedding_vec <=> %s::vector(1536)
        LIMIT %s
    """, (vec_str, GLOBAL_CLIENT_ID, vec_str, args.top))
    rows = cur.fetchall()
    cur.close()
    conn.close()

    print(f"Query: \"{query}\"")
    print(f"Top {len(rows)} results (cosine distance; lower = more similar):")
    print("-" * 80)
    for i, (gid, source_text, dist) in enumerate(rows, 1):
        preview = (source_text or "")[:100] + ("..." if (source_text and len(source_text) > 100) else "")
        # Optional: similarity score = 1 - (distance/2) for cosine in [0,1]; pgvector <=> is 1 - cos for cosine_ops
        sim = 1.0 - float(dist) if dist is not None else None
        print(f"  {i}. {gid}  distance={dist:.4f}  sim≈{sim:.4f}")
        print(f"     {preview}")
        print()


if __name__ == "__main__":
    main()

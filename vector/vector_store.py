"""
Local vector store: SQLite table (embedding BLOB). OpenSearch substitute.
Contract: index by (client_id, supplier_id); store 1536-dim vector + metadata.
"""
import os
import sqlite3
import struct
from datetime import datetime

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
VECTOR_DB = os.path.join(ROOT, "vector", "vector_index.db")
DIM = 1536


def _float_list_to_blob(vec: list[float]) -> bytes:
    return struct.pack(f"{len(vec)}f", *vec)


def _blob_to_float_list(blob: bytes) -> list[float]:
    n = len(blob) // 4
    return list(struct.unpack(f"{n}f", blob))


def get_conn(db_path: str = None):
    db_path = db_path or VECTOR_DB
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS vector_embeddings (
            client_id TEXT NOT NULL,
            supplier_id TEXT NOT NULL,
            canonical_supplier_id TEXT,
            chunk_id TEXT DEFAULT '0',
            embedding BLOB NOT NULL,
            source_text TEXT,
            indexed_at TEXT,
            PRIMARY KEY (client_id, supplier_id, chunk_id)
        )
    """)
    conn.commit()
    return conn


def write_embedding(
    client_id: str,
    supplier_id: str,
    vector: list[float],
    canonical_supplier_id: str = None,
    source_text: str = None,
    chunk_id: str = "0",
    db_path: str = None,
):
    conn = get_conn(db_path)
    blob = _float_list_to_blob(vector)
    now = datetime.utcnow().isoformat()
    conn.execute(
        """INSERT OR REPLACE INTO vector_embeddings
           (client_id, supplier_id, canonical_supplier_id, chunk_id, embedding, source_text, indexed_at)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        (client_id, supplier_id, canonical_supplier_id, chunk_id, blob, source_text, now),
    )
    conn.commit()
    conn.close()


def write_embeddings_batch(entries: list[dict], db_path: str = None):
    """entries: list of { client_id, supplier_id, vector, canonical_supplier_id?, source_text? }"""
    conn = get_conn(db_path)
    now = datetime.utcnow().isoformat()
    for e in entries:
        blob = _float_list_to_blob(e["vector"])
        conn.execute(
            """INSERT OR REPLACE INTO vector_embeddings
               (client_id, supplier_id, canonical_supplier_id, chunk_id, embedding, source_text, indexed_at)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (
                e["client_id"],
                e["supplier_id"],
                e.get("canonical_supplier_id"),
                e.get("chunk_id", "0"),
                blob,
                e.get("source_text"),
                now,
            ),
        )
    conn.commit()
    conn.close()

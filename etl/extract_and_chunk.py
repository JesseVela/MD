"""
Unstructured path: extract text (name + description + tags) and build chunks keyed by client_id + supplier_id.
Lambda equivalent: Extract text → Chunk by client_id + supplier_id + metadata.
"""
from typing import List, Dict, Any


def extract_text(row: dict) -> str:
    """Embedding source: normalized_supplier_name + supplier_description + product_service_tags (Roadmap 07)."""
    name = (row.get("normalized_supplier_name") or row.get("supplier_name_normalized") or "").strip()
    desc = (row.get("supplier_description") or "").strip()
    tags = (row.get("product_service_tags") or "").strip()
    parts = [p for p in [name, desc, tags] if p]
    return " | ".join(parts) if parts else name or ""


def build_chunks(rows: list[dict], client_id: str) -> list[dict]:
    """
    One chunk per supplier row. Keys: client_id, supplier_id; metadata: source_text, chunk_id.
    """
    chunks = []
    for row in rows:
        supp_id = (row.get("supplier_id") or "").strip()
        if not supp_id:
            continue
        source_text = extract_text(row)
        chunks.append({
            "client_id": client_id,
            "supplier_id": supp_id,
            "canonical_supplier_id": row.get("canonical_supplier_id"),
            "chunk_id": "0",
            "source_text": source_text,
            "metadata": {
                "normalized_supplier_name": row.get("normalized_supplier_name") or row.get("supplier_name_normalized"),
                "l1_category": row.get("l1_category"),
                "l2_category": row.get("l2_category"),
            },
        })
    return chunks

"""
Mock embeddings: deterministic 1536-dim vectors (no Bedrock call).
Contract: input = text string(s); output = list of 1536-dim vectors.
Swap to AWS: replace with Bedrock Titan invoke; same input/output shape.
"""
import hashlib
import struct

DIM = 1536


def _hash_to_floats(text: str, dim: int = DIM) -> list[float]:
    """Deterministic: hash text and expand to dim floats in [-1, 1]."""
    h = hashlib.sha256(text.encode("utf-8")).digest()
    floats = []
    for i in range(dim):
        # Use 4 bytes per float from repeated hash if needed
        start = (i * 4) % len(h)
        chunk = (h[start : start + 4] + h * ( (4 - (len(h) - start)) // len(h) + 1))[:4]
        n = struct.unpack("<I", chunk)[0]
        floats.append((n / (2**32 - 1)) * 2 - 1)
    return floats[:dim]


def embed_text(text: str, dim: int = DIM) -> list[float]:
    """Single text -> one 1536-dim vector."""
    if not text or not text.strip():
        return [0.0] * dim
    return _hash_to_floats(text.strip(), dim)


def embed_texts(texts: list[str], dim: int = DIM) -> list[list[float]]:
    """Multiple texts -> list of 1536-dim vectors."""
    return [embed_text(t, dim) for t in texts]

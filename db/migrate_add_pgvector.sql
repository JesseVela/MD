-- Run this in DBeaver (or any SQL client) connected to S2P-Semantic-Layer.
-- Execute each block separately (e.g. Ctrl+Enter per block).

-- 1. Enable pgvector
CREATE EXTENSION IF NOT EXISTS vector;

-- 2. Add vector column (1536 = Bedrock Titan embed dimension)
ALTER TABLE vec.vector_embeddings
ADD COLUMN IF NOT EXISTS embedding_vec vector(1536);

-- 3. Index for cosine similarity search (use HNSW if available, else IVFFlat)
-- HNSW (pgvector 0.5+):
CREATE INDEX IF NOT EXISTS idx_vec_embeddings_embedding_vec_cosine
ON vec.vector_embeddings
USING hnsw (embedding_vec vector_cosine_ops);

-- If the above fails with "access method hnsw not available", use IVFFlat instead:
-- CREATE INDEX IF NOT EXISTS idx_vec_embeddings_embedding_vec_cosine
-- ON vec.vector_embeddings
-- USING ivfflat (embedding_vec vector_cosine_ops)
-- WITH (lists = 100);

-- Setup script for weather_embeddings table
-- Run this manually in your Lakebase Postgres database (SQL Editor) before
-- running the ingest_weather_embeddings notebook.
--
-- Mirrors sql/02_setup_embeddings_table.sql. all-MiniLM-L6-v2 is 384-dim, so
-- the vector column is VECTOR(384). If you switch models, change 384 to match.

-- pgvector must be enabled (already enabled on this Lakebase instance; harmless if repeated)
CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE IF NOT EXISTS weather_embeddings (
    id          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    document_id TEXT NOT NULL REFERENCES weather_documents(id) ON DELETE CASCADE,
    chunk_index INT  NOT NULL,
    chunk_text  TEXT NOT NULL,
    embedding   VECTOR(384) NOT NULL,
    model_name  TEXT NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (document_id, chunk_index)   -- lets the ingest re-run safely (ON CONFLICT target)
);

-- HNSW index for fast cosine similarity search. Pairs with the <=> operator
-- and vector_cosine_ops used by POST /weather/search.
CREATE INDEX IF NOT EXISTS idx_weather_embeddings_embedding
    ON weather_embeddings
    USING hnsw (embedding vector_cosine_ops);

-- Verify
SELECT column_name, data_type, udt_name
FROM information_schema.columns
WHERE table_name = 'weather_embeddings'
ORDER BY ordinal_position;
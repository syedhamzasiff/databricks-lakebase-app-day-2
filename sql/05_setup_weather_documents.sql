-- Setup script for weather_documents table
-- Run this manually in your Lakebase Postgres database (SQL Editor) before
-- running POST /weather/sync or the ingest_weather_embeddings notebook.
--
-- Mirrors sql/01_setup_news_table.sql but for the weather source.

CREATE TABLE IF NOT EXISTS weather_documents (
    id             TEXT PRIMARY KEY,          -- stable dedup key (alert id, or hash for forecasts)
    location       TEXT NOT NULL,             -- "City, ST" or "lat,lon"
    source_type    TEXT NOT NULL,             -- 'alert' | 'forecast'
    headline       TEXT,                      -- e.g. "Flash Flood Warning" / forecast period name
    narrative_text TEXT NOT NULL,             -- the free-text body that gets embedded
    issued_at      TIMESTAMPTZ,               -- effective/onset (alerts) or startTime (forecast)
    payload        JSONB NOT NULL,            -- raw JSON, for provenance
    synced_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_weather_documents_location
    ON weather_documents (location);

CREATE INDEX IF NOT EXISTS idx_weather_documents_source_type
    ON weather_documents (source_type);

-- Verify
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_name = 'weather_documents'
ORDER BY ordinal_position;
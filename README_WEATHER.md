# Weather Intelligence Pipeline (Homework)

Unstructured weather text → Lakebase (Postgres + pgvector) → semantic search REST API.
Mirrors the ticker-news RAG pipeline in the reference app, for a new data source.

## Data source & why

**National Weather Service API (`api.weather.gov`).** Free, no API key, and returns rich
free-text ideal for embedding:

- `GET /alerts/active?area={ST}` — active alerts; each has a narrative `description` plus an
  `instruction` field.
- `GET /gridpoints/.../forecast` (via the `forecast` URL from `GET /points/{lat},{lon}`) —
  multi-day forecast, each period with a narrative `detailedForecast`.

Considered OpenWeatherMap (needs a key) and NOAA CPC discussions (harder to normalize
per-location); NWS gave the cleanest no-auth path to genuinely unstructured text, used as the
single source.

## Schema decisions

**`weather_documents`** (raw text — mirrors `ticker_news_documents`): `id` TEXT PK (stable
dedup key: the alert's own `id`, or a SHA-256 of `location + period` for forecasts, so
re-syncing upserts instead of duplicating), `location`, `source_type` (`alert`/`forecast`),
`headline`, `narrative_text` (the embedded text), `issued_at`, `payload` JSONB (provenance),
`synced_at`.

**`weather_embeddings`** (vectors — mirrors `ticker_news_embeddings`): `id` identity PK,
`document_id` FK → `weather_documents.id` (`ON DELETE CASCADE`), `chunk_index`, `chunk_text`,
`embedding VECTOR(384)`, `model_name`, `created_at`, with `UNIQUE (document_id, chunk_index)`
for idempotent re-runs.

## Chunking, model, and write path

- **Model:** `sentence-transformers/all-MiniLM-L6-v2`, **384-dim** — same model as the news
  pipeline, so both are queryable with the same cosine (`<=>`) conventions. The `/weather/search`
  endpoint embeds the query with this same model, loaded once (lazily) at module scope.
- **Chunking:** sliding window `CHUNK_SIZE=800`, `CHUNK_OVERLAP=100`. Most NWS text is short, so
  this usually yields one chunk; it only splits long combined alert+instruction bodies.
- **Distance/index:** cosine via pgvector `<=>`, with an **HNSW** index using
  `vector_cosine_ops`; similarity reported as `1 - distance`.
- **Write path = psycopg2, not Spark.** Embeddings are written by
  `notebooks/ingest_weather_embeddings.py` using **psycopg2 `execute_values`**, inserting each
  vector directly as the pgvector text literal `'[...]'::vector` with
  `ON CONFLICT (document_id, chunk_index)`. **`spark.write.jdbc` is NOT used** — Spark JDBC
  writes are unsupported/unreliable against this Lakebase instance, so the entire write path is
  plain psycopg2.

## How to run (end to end)

1. **Create tables** — run `sql/05_setup_weather_documents.sql` and
   `sql/06_setup_weather_embeddings.sql` in the Lakebase SQL Editor (the latter also
   `CREATE EXTENSION vector` + builds the HNSW index).
2. **Harvest** — `POST /weather/sync` with
   `{"locations": ["Chicago, IL", "Austin, TX"], "limit": 50}` → `{"synced": N}`.
3. **Embed** — run `notebooks/ingest_weather_embeddings.py` (attach to Serverless, Run All). It
   reads un-embedded `weather_documents` via a left anti-join, chunks + embeds, and writes
   `weather_embeddings` with psycopg2.
4. **Search** — `POST /weather/search` with
   `{"query": "flash flood risk this weekend", "top_k": 5}` → ranked matches with `location`,
   `headline`, `chunk_text`, `similarity`.

## Known limitations / would-improve-with-more-time

- City → lat/lon is a small hardcoded map; a geocoder would resolve any location.
- `/weather/search` loads sentence-transformers in-process (heavy torch dependency, slow first
  call); a Databricks model-serving embedding endpoint would be lighter.
- Freshness: alerts expire, so results are only as current as the last `/weather/sync`;
  scheduling sync + embed as a Databricks Job would keep it fresh.
- Set a real contact email in `weather_client.py`'s `_USER_AGENT` — NWS may return 403 for a
  placeholder/missing User-Agent.
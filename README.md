# Weather Intelligence Pipeline (Homework)

Unstructured weather text → Lakebase (Postgres + pgvector) → semantic search REST API.
This mirrors the ticker-news RAG pipeline in the reference app, for a new data source.

## Data source & why

**National Weather Service API (`api.weather.gov`).** Chosen because it's free, needs no
API key, and returns rich free-text ideal for embedding:

- `GET /alerts/active?area={ST}` — active alerts; each has a narrative `description` plus an
  `instruction` field ("A Flash Flood Warning means…").
- `GET /gridpoints/.../forecast` (reached via the `forecast` URL from `GET /points/{lat},{lon}`)
  — multi-day forecast, each period with a narrative `detailedForecast`.

I considered OpenWeatherMap (needs a key) and NOAA CPC discussion products (harder to
normalize per-location). NWS gave the cleanest no-auth path to genuinely unstructured text,
so I used it as the single source.

## Schema decisions

**`weather_documents`** (raw text store — mirrors `ticker_news_documents`):

| column | type | notes |
|---|---|---|
| `id` | TEXT PK | stable dedup key: the alert's own `id`, or a SHA-256 hash of `location + startTime` for forecasts → re-syncing upserts instead of duplicating |
| `location` | TEXT | "City, ST" or "lat,lon" |
| `source_type` | TEXT | `alert` or `forecast` |
| `headline` | TEXT | event name / forecast period name |
| `narrative_text` | TEXT | the free text that gets embedded (alert `description`+`instruction`, or `detailedForecast`) |
| `issued_at` | TIMESTAMPTZ | alert `effective`/`onset`, or forecast `startTime` |
| `payload` | JSONB | raw JSON for provenance |
| `synced_at` | TIMESTAMPTZ | default `now()` |

**`weather_embeddings`** (vectors — mirrors `ticker_news_embeddings`):

| column | type | notes |
|---|---|---|
| `id` | BIGINT identity PK | |
| `document_id` | TEXT FK → `weather_documents.id` | `ON DELETE CASCADE` |
| `chunk_index` | INT | |
| `chunk_text` | TEXT | |
| `embedding` | `VECTOR(384)` | pgvector |
| `model_name` | TEXT | |
| `created_at` | TIMESTAMPTZ | |
| | | `UNIQUE (document_id, chunk_index)` → idempotent re-runs |

## Chunking & model

- **Model:** `sentence-transformers/all-MiniLM-L6-v2`, **384-dim** — the same model as the
  news pipeline, so both tables are queryable with the same cosine (`<=>`) conventions.
- **Chunking:** sliding window `CHUNK_SIZE=800`, `CHUNK_OVERLAP=100`. Most NWS text is short,
  so this usually yields a single chunk; it only splits long combined alert+instruction bodies.
- **Distance/index:** cosine via pgvector `<=>`, with an **HNSW** index using
  `vector_cosine_ops`. Similarity in the API is reported as `1 - distance`.

**Vector insert note:** unlike the reference news notebook (which inserts a `double
precision[]` array literal and then runs a separate `::vector` cast step), this pipeline
inserts embeddings directly as the pgvector text literal `'[...]'::vector`. Cleaner, one
step, and it's what the assignment recommends.

## How to run (end to end)

1. **Create tables** — run `sql/05_setup_weather_documents.sql` and
   `sql/06_setup_weather_embeddings.sql` in the Lakebase SQL Editor.
2. **Harvest** — `POST /weather/sync` with
   `{"locations": ["Chicago, IL", "Austin, TX"], "limit": 50}` → returns `{"synced": N}`.
3. **Embed** — run the `notebooks/ingest_weather_embeddings.py` notebook (reads un-embedded
   `weather_documents`, chunks + embeds, writes `weather_embeddings`).
4. **Search** — `POST /weather/search` with
   `{"query": "flash flood risk this weekend", "top_k": 5}` → ranked matches with
   `location`, `headline`, `chunk_text`, `similarity`.

## Known limitations / would-improve-with-more-time

- **City → lat/lon is a small hardcoded map.** A production version would call a geocoder
  (e.g. the free US Census geocoder) so any location resolves.
- **The app loads sentence-transformers in-process** for `/weather/search`, which makes the
  first request after deploy slow and adds a heavy (torch) dependency. Better: call a
  Databricks model-serving / Foundation Model embedding endpoint and drop the in-app model.
- **Freshness:** alerts expire, so results are only as current as the last `/weather/sync`.
  Scheduling the sync + embed as a Databricks Job (like the news job in `resources/`) would
  keep it fresh automatically.
- **No pagination** on search results beyond `top_k` (clamped 1–20).
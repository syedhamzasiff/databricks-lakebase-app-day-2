# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest Weather Documents -> Vector Embeddings (Lakebase)
# MAGIC
# MAGIC Weather counterpart to `ingest_ticker_news_embeddings.py`.
# MAGIC
# MAGIC It:
# MAGIC 1. Reads rows from `weather_documents` that have NOT been embedded yet
# MAGIC    (left anti-join against `weather_embeddings`).
# MAGIC 2. Chunks each `narrative_text` (sliding window, 800/100; most NWS text is
# MAGIC    short enough to be a single chunk).
# MAGIC 3. Embeds each chunk with `sentence-transformers/all-MiniLM-L6-v2` (384-dim).
# MAGIC 4. Writes vectors into `weather_embeddings` with **psycopg2**
# MAGIC    (`execute_values`), inserting directly as `'[...]'::vector` and using
# MAGIC    `ON CONFLICT (document_id, chunk_index)` so re-runs are idempotent.
# MAGIC
# MAGIC **Write path = psycopg2, not Spark.** `spark.write.jdbc` is not used (and is
# MAGIC unsupported/unreliable against this Lakebase instance).
# MAGIC
# MAGIC Re-uses the SAME Lakebase secret (`database` / `lakebase-url`) as `lakebase.py`.
# MAGIC
# MAGIC **Prerequisite:** run `sql/05_setup_weather_documents.sql` and
# MAGIC `sql/06_setup_weather_embeddings.sql` in the Lakebase SQL Editor first, and
# MAGIC populate `weather_documents` via `POST /weather/sync`.

# COMMAND ----------

# DBTITLE 1,Install packages
# MAGIC %pip install -q 'databricks-sdk>=0.118.0' sentence-transformers psycopg2-binary

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Config (widgets)
dbutils.widgets.text("documents_table_name", "weather_documents", "Source table (weather docs)")
dbutils.widgets.text("embeddings_table_name", "weather_embeddings", "Destination table (vectors)")
dbutils.widgets.text("embedding_model", "sentence-transformers/all-MiniLM-L6-v2", "Embedding model")
dbutils.widgets.text("chunk_size", "800", "Chunk size (chars)")
dbutils.widgets.text("chunk_overlap", "100", "Chunk overlap (chars)")

DOCUMENTS_TABLE_NAME = dbutils.widgets.get("documents_table_name")
EMBEDDINGS_TABLE_NAME = dbutils.widgets.get("embeddings_table_name")
EMBEDDING_MODEL_NAME = dbutils.widgets.get("embedding_model")
CHUNK_SIZE = int(dbutils.widgets.get("chunk_size"))
CHUNK_OVERLAP = int(dbutils.widgets.get("chunk_overlap"))

# The pgvector column type VECTOR(N) must match the model's output dim exactly.
match EMBEDDING_MODEL_NAME:
    case "sentence-transformers/all-MiniLM-L6-v2":
        EMBEDDING_DIM = 384
    case "sentence-transformers/all-mpnet-base-v2":
        EMBEDDING_DIM = 768
    case _:
        raise ValueError(
            f"Unknown model {EMBEDDING_MODEL_NAME!r} - add its output dim above, "
            "and update VECTOR(...) in sql/06_setup_weather_embeddings.sql to match."
        )

print(f"Model {EMBEDDING_MODEL_NAME!r} -> {EMBEDDING_DIM}-dim vectors")

# COMMAND ----------

# DBTITLE 1,Read the Lakebase connection info from the shared secret (read-only)
# NOTE: this only READS the secret that app.py / lakebase.py already use.
# It does NOT set or hardcode any password - the credential lives only in the
# Databricks secret store (scope 'database', key 'lakebase-url').
import base64
from urllib.parse import urlparse

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()


def get_lakebase_url() -> str:
    secret = w.secrets.get_secret(scope="database", key="lakebase-url")
    return base64.b64decode(secret.value).decode("utf-8")


parsed = urlparse(get_lakebase_url())
db_host = parsed.hostname
db_port = parsed.port or 5432
db_name = parsed.path.lstrip("/")
db_user = parsed.username
db_password = parsed.password

print(f"Host: {db_host}:{db_port}  Database: {db_name}  User: {db_user}")

# COMMAND ----------

# DBTITLE 1,Helpers: connect, chunk, format vector
import psycopg2
from psycopg2.extras import RealDictCursor, execute_values


def connect():
    return psycopg2.connect(
        host=db_host, port=db_port, dbname=db_name, user=db_user,
        password=db_password, sslmode="require", cursor_factory=RealDictCursor,
    )


def chunk_text(text: str, size: int = CHUNK_SIZE, overlap: int = CHUNK_OVERLAP) -> list[str]:
    """Sliding window. Returns a single chunk when text fits in `size`."""
    text = (text or "").strip()
    if len(text) <= size:
        return [text] if text else []
    chunks = []
    for start in range(0, len(text), size - overlap):
        piece = text[start:start + size].strip()
        if piece:
            chunks.append(piece)
        if start + size >= len(text):
            break
    return chunks


def to_vector_literal(vec) -> str:
    """pgvector text format is '[0.1,0.2,...]' (brackets). Insert with %s::vector."""
    return "[" + ",".join(f"{float(x):.6f}" for x in vec) + "]"

# COMMAND ----------

# DBTITLE 1,Read documents that have not been embedded yet (left anti-join)
SELECT_UNEMBEDDED = f"""
    SELECT d.id, d.narrative_text
    FROM {DOCUMENTS_TABLE_NAME} d
    LEFT JOIN {EMBEDDINGS_TABLE_NAME} e ON e.document_id = d.id
    WHERE e.document_id IS NULL
      AND d.narrative_text IS NOT NULL
      AND d.narrative_text <> ''
"""

conn = connect()
try:
    with conn.cursor() as cur:
        cur.execute(SELECT_UNEMBEDDED)
        docs = cur.fetchall()   # list[dict]: {"id":..., "narrative_text":...}
finally:
    conn.close()

print(f"{len(docs)} documents to embed")

# COMMAND ----------

# DBTITLE 1,Embed each chunk
from sentence_transformers import SentenceTransformer

model = SentenceTransformer(EMBEDDING_MODEL_NAME)

rows = []  # (document_id, chunk_index, chunk_text, embedding_literal, model_name)
for d in docs:
    chunks = chunk_text(d["narrative_text"])
    if not chunks:
        continue
    vectors = model.encode(chunks, show_progress_bar=False)  # batch-encode this doc's chunks
    for i, (chunk, vec) in enumerate(zip(chunks, vectors)):
        rows.append((d["id"], i, chunk, to_vector_literal(vec.tolist()), EMBEDDING_MODEL_NAME))

print(f"Prepared {len(rows)} embedding rows")

# COMMAND ----------

# DBTITLE 1,Insert embeddings via psycopg2 execute_values (idempotent upsert)
# Write path is psycopg2 - NOT Spark JDBC. The %s::vector cast in the template
# writes directly into the pgvector VECTOR(384) column.
INSERT_SQL = f"""
    INSERT INTO {EMBEDDINGS_TABLE_NAME}
        (document_id, chunk_index, chunk_text, embedding, model_name)
    VALUES %s
    ON CONFLICT (document_id, chunk_index) DO UPDATE SET
        chunk_text = EXCLUDED.chunk_text,
        embedding  = EXCLUDED.embedding,
        model_name = EXCLUDED.model_name
"""

if rows:
    conn = connect()
    try:
        with conn.cursor() as cur:
            execute_values(
                cur, INSERT_SQL, rows,
                template="(%s, %s, %s, %s::vector, %s)",
                page_size=100,
            )
        conn.commit()
        print(f"Inserted/updated {len(rows)} embedding rows")
    finally:
        conn.close()
else:
    print("Nothing to insert.")

# COMMAND ----------

# DBTITLE 1,Verify (COUNT proof)
conn = connect()
try:
    with conn.cursor() as cur:
        cur.execute(f"SELECT count(*) AS n FROM {EMBEDDINGS_TABLE_NAME}")
        print("total embeddings:", cur.fetchone()["n"])
        cur.execute(f"SELECT count(*) AS n FROM {EMBEDDINGS_TABLE_NAME} WHERE embedding IS NULL")
        print("null embeddings (should be 0):", cur.fetchone()["n"])
        cur.execute(
            f"SELECT model_name, count(*) AS n FROM {EMBEDDINGS_TABLE_NAME} GROUP BY model_name"
        )
        for r in cur.fetchall():
            print("by model:", r["model_name"], r["n"])
finally:
    conn.close()
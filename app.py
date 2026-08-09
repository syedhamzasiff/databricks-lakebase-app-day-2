"""
Databricks App boilerplate:
- Serves a small Flask API
- Reads/writes to Lakebase (Databricks-managed Postgres) via lakebase.py
- Pulls data from the Massive API via massive_client.py and syncs it into Lakebase

Run locally:
    python app.py
Deploy as a Databricks App using app.yaml.

--------------------------------------------------------------------------
WEATHER HOMEWORK ADDITIONS (search for "# [WEATHER]" to see everything added):
  - import WeatherClient
  - weather config constants + lazy embedding-model loader
  - ensure_weather_documents_table()
  - POST /weather/sync  + _upsert_weather_batch()
  - POST /weather/search
Everything else is unchanged from the reference app.
--------------------------------------------------------------------------
"""

import logging
import os
import re

import requests
from databricks.sdk import WorkspaceClient
from flask import Flask, jsonify, render_template, request

import lakebase
from massive_client import MassiveClient
from weather_client import WeatherClient  # [WEATHER]

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("massive-app")

app = Flask(__name__)
_w = WorkspaceClient()

TABLE_NAME = os.environ.get("MASSIVE_TABLE_NAME", "massive_records")
WATCHLIST_TABLE_NAME = os.environ.get("WATCHLIST_TABLE_NAME", "watchlist")
NEWS_TABLE_NAME = os.environ.get("NEWS_TABLE_NAME", "ticker_news_documents")

# [WEATHER] weather config
WEATHER_DOCS_TABLE = os.environ.get("WEATHER_DOCS_TABLE", "weather_documents")
WEATHER_EMB_TABLE = os.environ.get("WEATHER_EMB_TABLE", "weather_embeddings")
WEATHER_EMBED_MODEL = os.environ.get(
    "WEATHER_EMBED_MODEL", "sentence-transformers/all-MiniLM-L6-v2"
)

# Tickers to fetch news for by default (comma-separated), e.g. "AAPL,MSFT,GOOGL"
DEFAULT_NEWS_TICKERS = [
    t.strip().upper()
    for t in os.environ.get("NEWS_TICKERS", "AAPL,MSFT,GOOGL,AMZN,TSLA").split(",")
    if t.strip()
]

# Basic stock ticker shape check: 1-10 uppercase letters, with an optional
# ".X" or ".XX" share-class suffix (e.g. "BRK.B").
_TICKER_RE = re.compile(r"^[A-Z]{1,10}(\.[A-Z]{1,2})?$")


# [WEATHER] Lazily load the sentence-transformers model ONCE (not per request).
# Loaded on first /weather/search call so app startup/import stays fast.
_search_model = None


def _get_search_model():
    global _search_model
    if _search_model is None:
        from sentence_transformers import SentenceTransformer  # imported lazily
        logger.info("Loading embedding model %s ...", WEATHER_EMBED_MODEL)
        _search_model = SentenceTransformer(WEATHER_EMBED_MODEL)
    return _search_model


def _to_vector_literal(vec) -> str:
    """pgvector text format '[0.1,0.2,...]' - pass with a %s::vector cast."""
    return "[" + ",".join(f"{float(x):.6f}" for x in vec) + "]"


def ensure_table():
    """Create the destination table in Lakebase if it doesn't exist yet."""
    lakebase.run_write(
        f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            id TEXT PRIMARY KEY,
            payload JSONB NOT NULL,
            synced_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )
        """
    )


def ensure_watchlist_table():
    """Create the watchlist table in Lakebase if it doesn't exist yet."""
    lakebase.run_write(
        f"""
        CREATE TABLE IF NOT EXISTS {WATCHLIST_TABLE_NAME} (
            symbol TEXT NOT NULL,
            email TEXT NOT NULL,
            latest_price NUMERIC,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            PRIMARY KEY (symbol, email)
        )
        """
    )


def ensure_news_table():
    """Create the raw ticker-news documents table in Lakebase if needed."""
    lakebase.run_write(
        f"""
        CREATE TABLE IF NOT EXISTS {NEWS_TABLE_NAME} (
            id TEXT PRIMARY KEY,
            ticker TEXT NOT NULL,
            title TEXT NOT NULL,
            description TEXT,
            author TEXT,
            article_url TEXT,
            publisher_name TEXT,
            keywords JSONB,
            sentiment TEXT,
            sentiment_reasoning TEXT,
            published_utc TIMESTAMPTZ,
            payload JSONB NOT NULL,
            synced_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )
        """
    )
    lakebase.run_write(
        f"CREATE INDEX IF NOT EXISTS idx_{NEWS_TABLE_NAME}_ticker "
        f"ON {NEWS_TABLE_NAME} (ticker)"
    )


def ensure_weather_documents_table():  # [WEATHER]
    """Create the raw weather documents table if it doesn't exist yet.

    Mirrors ensure_news_table(). Note: the weather_embeddings vector table is
    created via sql/06_setup_weather_embeddings.sql (it needs CREATE EXTENSION
    vector), same manual-SQL pattern the course uses for the news vectors.
    """
    lakebase.run_write(
        f"""
        CREATE TABLE IF NOT EXISTS {WEATHER_DOCS_TABLE} (
            id             TEXT PRIMARY KEY,
            location       TEXT NOT NULL,
            source_type    TEXT NOT NULL,
            headline       TEXT,
            narrative_text TEXT NOT NULL,
            issued_at      TIMESTAMPTZ,
            payload        JSONB NOT NULL,
            synced_at      TIMESTAMPTZ NOT NULL DEFAULT now()
        )
        """
    )
    lakebase.run_write(
        f"CREATE INDEX IF NOT EXISTS idx_{WEATHER_DOCS_TABLE}_location "
        f"ON {WEATHER_DOCS_TABLE} (location)"
    )


def _current_user_email() -> str:
    """Resolve the current user's email (Databricks Apps inject X-Forwarded-Email)."""
    header_email = request.headers.get("X-Forwarded-Email")
    if header_email:
        return header_email
    return _w.current_user.me().user_name


@app.route("/healthz")
def healthz():
    return jsonify({"status": "ok"})


@app.errorhandler(Exception)
def handle_exception(err):
    """Ensure all unhandled errors return JSON (not an HTML error page)."""
    logger.exception("Unhandled exception while processing request")
    status_code = getattr(err, "code", 500)
    if not isinstance(status_code, int):
        status_code = 500
    return jsonify({"error": str(err)}), status_code


@app.route("/")
def index():
    """Simple UI to submit a list of stock symbols to sync from Massive."""
    return render_template("index.html")


@app.route("/records")
def list_records():
    """Read records already synced into Lakebase."""
    limit = int(request.args.get("limit", 100))
    rows = lakebase.run_query(
        f"SELECT id, payload, synced_at FROM {TABLE_NAME} ORDER BY synced_at DESC LIMIT %s",
        (limit,),
    )
    return jsonify(rows)


@app.route("/sync", methods=["POST"])
def sync_from_massive():
    """Pull data from the Massive API (paginated) and upsert into Lakebase."""
    ensure_table()
    client = MassiveClient()

    path = request.json.get("path", "/records") if request.is_json else "/records"
    batch_size = int(request.args.get("batch_size", 500))

    batch = []
    total = 0
    for item in client.paginated_get(path):
        batch.append(item)
        if len(batch) >= batch_size:
            total += _upsert_batch(batch)
            batch = []

    if batch:
        total += _upsert_batch(batch)

    return jsonify({"synced": total})


@app.route("/news/sync", methods=["POST"])
def sync_news_from_massive():
    """Pull recent news for a set of tickers and upsert into ticker_news_documents."""
    ensure_news_table()
    client = MassiveClient()

    body = request.json if request.is_json else {}
    tickers = body.get("tickers") or DEFAULT_NEWS_TICKERS
    tickers = [t.strip().upper() for t in tickers if isinstance(t, str) and t.strip()]
    limit = int(body.get("limit", 50))

    total = 0
    for ticker in tickers:
        if not _TICKER_RE.match(ticker):
            continue
        articles = client.get_news(ticker, limit=limit)
        total += _upsert_news_batch(ticker, articles)

    return jsonify({"synced": total, "tickers": tickers})


# ==========================================================================
# [WEATHER] Weather sync + semantic search
# ==========================================================================


@app.route("/weather/sync", methods=["POST"])
def sync_weather():
    """Harvest NWS alerts + forecasts for locations and upsert into weather_documents.

    Body (JSON): {"locations": ["Chicago, IL", "Austin, TX"], "limit": 50}
    """
    ensure_weather_documents_table()

    body = request.json if request.is_json else {}
    locations = body.get("locations") or []
    locations = [loc.strip() for loc in locations if isinstance(loc, str) and loc.strip()]
    if not locations:
        return jsonify({"error": "locations is required (list of 'City, ST' or 'lat,lon')"}), 400
    limit = int(body.get("limit", 50))

    client = WeatherClient()
    docs = client.harvest(locations, limit=limit)
    synced = _upsert_weather_batch(docs)

    return jsonify({"synced": synced, "locations": locations})


@app.route("/weather/search", methods=["POST"])
def search_weather():
    """Semantic search over weather_embeddings using pgvector cosine distance.

    Body (JSON): {"query": "flash flood risk this weekend", "top_k": 5}
    """
    body = request.json if request.is_json else {}
    query = (body.get("query") or "").strip()
    if not query:
        return jsonify({"error": "query is required"}), 400

    try:
        top_k = int(body.get("top_k", 5))
    except (TypeError, ValueError):
        top_k = 5
    top_k = max(1, min(20, top_k))  # clamp to 1..20

    model = _get_search_model()
    query_literal = _to_vector_literal(model.encode(query).tolist())

    rows = lakebase.run_query(
        f"""
        SELECT d.id, d.location, d.headline, d.narrative_text, e.chunk_text,
               1 - (e.embedding <=> %s::vector) AS similarity
        FROM {WEATHER_EMB_TABLE} e
        JOIN {WEATHER_DOCS_TABLE} d ON d.id = e.document_id
        ORDER BY e.embedding <=> %s::vector
        LIMIT %s
        """,
        (query_literal, query_literal, top_k),
    )
    # Empty weather_embeddings (nothing synced/embedded yet) -> rows == [] -> results: []
    return jsonify({"query": query, "top_k": top_k, "results": rows})


@app.route("/watchlist", methods=["GET"])
def get_watchlist():
    """Return the current user's watchlist symbols, with their last known price."""
    ensure_watchlist_table()
    email = _current_user_email()
    rows = lakebase.run_query(
        f"SELECT symbol, email, latest_price, updated_at FROM {WATCHLIST_TABLE_NAME} "
        f"WHERE email = %s ORDER BY symbol ASC",
        (email,),
    )
    return jsonify(rows)


@app.route("/watchlist", methods=["POST"])
def add_to_watchlist():
    """Fetch latest price for a symbol (ONE Massive call) and add to the watchlist."""
    ensure_watchlist_table()

    if request.is_json:
        symbol = request.json.get("symbol", "")
    else:
        symbol = request.form.get("symbol", "")

    symbol = symbol.strip().upper() if isinstance(symbol, str) else ""

    if not symbol or not _TICKER_RE.match(symbol):
        return jsonify({"error": f"Invalid ticker symbol: {symbol!r}"}), 400

    client = MassiveClient()
    try:
        data = client.get_latest_price(symbol)
    except requests.HTTPError:
        return jsonify({"error": f"Unknown ticker symbol: {symbol}"}), 400

    price = _extract_latest_price(data)
    if price is None:
        return jsonify({"error": f"No price data available for ticker: {symbol}"}), 400

    email = _current_user_email()

    lakebase.run_write(
        f"""
        INSERT INTO {WATCHLIST_TABLE_NAME} (symbol, email, latest_price, updated_at)
        VALUES (%s, %s, %s, now())
        ON CONFLICT (symbol, email) DO UPDATE
            SET latest_price = EXCLUDED.latest_price,
                updated_at = EXCLUDED.updated_at
        """,
        (symbol, email, price),
    )

    return jsonify({"symbol": symbol, "email": email, "latest_price": price})


@app.route("/watchlist/<symbol>", methods=["DELETE"])
def delete_from_watchlist(symbol: str):
    """Remove a single symbol from the current user's watchlist."""
    ensure_watchlist_table()

    symbol = symbol.strip().upper() if isinstance(symbol, str) else ""
    if not symbol or not _TICKER_RE.match(symbol):
        return jsonify({"error": f"Invalid ticker symbol: {symbol!r}"}), 400

    email = _current_user_email()
    deleted = lakebase.run_write(
        f"DELETE FROM {WATCHLIST_TABLE_NAME} WHERE symbol = %s AND email = %s",
        (symbol, email),
    )

    if not deleted:
        return jsonify({"error": f"{symbol} is not on your watchlist"}), 404

    return jsonify({"symbol": symbol, "email": email, "deleted": True})


def _extract_latest_price(data: dict) -> float | None:
    """Pull the trade price out of the Massive 'previous close' response shape."""
    if not isinstance(data, dict):
        return None
    if data.get("status") not in (None, "OK") or data.get("resultsCount") == 0:
        return None
    results = data.get("results", data)
    if isinstance(results, list):
        results = results[0] if results else None
    if isinstance(results, dict):
        for key in ("c", "p", "price", "last_price", "vw"):
            if key in results:
                return results[key]
    return None


def _upsert_batch(items: list[dict]) -> int:
    """Upsert a batch of Massive API items into Lakebase, one statement per row."""
    import json as _json

    count = 0
    with lakebase.get_connection() as conn:
        with conn.cursor() as cur:
            for item in items:
                cur.execute(
                    f"""
                    INSERT INTO {TABLE_NAME} (id, payload, synced_at)
                    VALUES (%s, %s, now())
                    ON CONFLICT (id) DO UPDATE
                        SET payload = EXCLUDED.payload,
                            synced_at = EXCLUDED.synced_at
                    """,
                    (str(item.get("id")), _json.dumps(item)),
                )
                count += 1
            conn.commit()
    return count


def _upsert_news_batch(ticker: str, articles: list[dict]) -> int:
    """Upsert news articles for a single ticker into the news documents table."""
    import json as _json

    count = 0
    with lakebase.get_connection() as conn:
        with conn.cursor() as cur:
            for article in articles:
                sentiment = None
                sentiment_reasoning = None
                for insight in article.get("insights", []) or []:
                    if insight.get("ticker") == ticker:
                        sentiment = insight.get("sentiment")
                        sentiment_reasoning = insight.get("sentiment_reasoning")
                        break

                publisher = article.get("publisher") or {}
                cur.execute(
                    f"""
                    INSERT INTO {NEWS_TABLE_NAME} (
                        id, ticker, title, description, author, article_url,
                        publisher_name, keywords, sentiment, sentiment_reasoning,
                        published_utc, payload, synced_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now())
                    ON CONFLICT (id) DO UPDATE
                        SET ticker = EXCLUDED.ticker,
                            title = EXCLUDED.title,
                            description = EXCLUDED.description,
                            author = EXCLUDED.author,
                            article_url = EXCLUDED.article_url,
                            publisher_name = EXCLUDED.publisher_name,
                            keywords = EXCLUDED.keywords,
                            sentiment = EXCLUDED.sentiment,
                            sentiment_reasoning = EXCLUDED.sentiment_reasoning,
                            published_utc = EXCLUDED.published_utc,
                            payload = EXCLUDED.payload,
                            synced_at = EXCLUDED.synced_at
                    """,
                    (
                        str(article.get("id")),
                        ticker,
                        article.get("title", ""),
                        article.get("description"),
                        article.get("author"),
                        article.get("article_url"),
                        publisher.get("name"),
                        _json.dumps(article.get("keywords", [])),
                        sentiment,
                        sentiment_reasoning,
                        article.get("published_utc"),
                        _json.dumps(article),
                    ),
                )
                count += 1
            conn.commit()
    return count


def _upsert_weather_batch(docs: list[dict]) -> int:  # [WEATHER]
    """Upsert normalized weather documents into weather_documents.

    Follows the same get_connection() + manual commit pattern as
    _upsert_news_batch above.
    """
    import json as _json

    if not docs:
        return 0

    count = 0
    with lakebase.get_connection() as conn:
        with conn.cursor() as cur:
            for d in docs:
                cur.execute(
                    f"""
                    INSERT INTO {WEATHER_DOCS_TABLE} (
                        id, location, source_type, headline, narrative_text,
                        issued_at, payload, synced_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, now())
                    ON CONFLICT (id) DO UPDATE
                        SET location       = EXCLUDED.location,
                            source_type    = EXCLUDED.source_type,
                            headline       = EXCLUDED.headline,
                            narrative_text = EXCLUDED.narrative_text,
                            issued_at      = EXCLUDED.issued_at,
                            payload        = EXCLUDED.payload,
                            synced_at      = EXCLUDED.synced_at
                    """,
                    (
                        d["id"],
                        d["location"],
                        d["source_type"],
                        d.get("headline"),
                        d["narrative_text"],
                        d.get("issued_at"),
                        _json.dumps(d.get("payload", {})),
                    ),
                )
                count += 1
            conn.commit()
    return count


if __name__ == '__main__':
    host = os.getenv('FLASK_RUN_HOST', '0.0.0.0')
    port = int(os.getenv('FLASK_RUN_PORT', 8000))
    app.run(debug=True, host=host, port=port)
    print(f"Flask app running on http://{host}:{port}")
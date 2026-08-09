"""
Client for the National Weather Service API (api.weather.gov).

Mirrors the shape of massive_client.py, but the NWS API needs NO API key and
NO secret scope - it only requires a descriptive User-Agent header. So this
client is simpler: no Databricks SDK, no auth plumbing.

It resolves each location to an NWS grid point, then fetches:
  - active weather alerts (rich free-text: description + instruction)
  - the multi-day forecast (narrative detailedForecast per period)
and normalizes both into a common `weather_documents` record shape.
"""

import hashlib
from datetime import datetime, timezone
from typing import Any

import requests

_DEFAULT_TIMEOUT = 30

# NWS BLOCKS requests without a descriptive User-Agent that includes a contact.
# Put YOUR real email here before running.
_USER_AGENT = "(databricks-weather-homework, your-email@example.com)"

# NWS works off lat/lon, not city names. For a homework-sized set a small map
# is enough. (For arbitrary cities, resolve via the free US Census geocoder -
# https://geocoding.geo.census.gov - but that is optional polish, not required.)
CITY_COORDS: dict[str, tuple[float, float]] = {
    "Chicago, IL": (41.8781, -87.6298),
    "Austin, TX": (30.2672, -97.7431),
    "Miami, FL": (25.7617, -80.1918),
    "Seattle, WA": (47.6062, -122.3321),
    "New Orleans, LA": (29.9511, -90.0715),
}


def _stable_id(*parts: Any) -> str:
    """Deterministic dedup key so re-syncing upserts instead of duplicating."""
    joined = "|".join(str(p) for p in parts)
    return hashlib.sha256(joined.encode("utf-8")).hexdigest()[:40]


class WeatherClient:
    """Thin wrapper around the NWS API with a shared session + User-Agent."""

    def __init__(self, base_url: str = "https://api.weather.gov", timeout: int = _DEFAULT_TIMEOUT):
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self._session = requests.Session()
        self._session.headers.update(
            {"User-Agent": _USER_AGENT, "Accept": "application/geo+json"}
        )

    def _get(self, path_or_url: str, params: dict[str, Any] | None = None) -> Any:
        url = path_or_url if path_or_url.startswith("http") else f"{self.base_url}{path_or_url}"
        resp = self._session.get(url, params=params, timeout=self.timeout)
        resp.raise_for_status()
        return resp.json()

    # -- raw NWS calls -----------------------------------------------------

    def resolve_point(self, location: str) -> dict[str, Any]:
        """Resolve 'City, ST' (in CITY_COORDS) or a 'lat,lon' string to grid metadata."""
        if location in CITY_COORDS:
            lat, lon = CITY_COORDS[location]
        else:
            lat, lon = [float(x.strip()) for x in location.split(",")]

        props = self._get(f"/points/{lat},{lon}")["properties"]
        rel = props["relativeLocation"]["properties"]
        return {
            "location_label": location,
            "state": rel.get("state"),
            "forecast_url": props["forecast"],
        }

    def get_active_alerts(self, state: str) -> list[dict]:
        """Active alerts for a US state code, e.g. 'IL'. Returns GeoJSON features."""
        data = self._get("/alerts/active", params={"area": state})
        return data.get("features", [])

    def get_forecast_periods(self, forecast_url: str) -> list[dict]:
        """Multi-day forecast periods for a grid point."""
        return self._get(forecast_url)["properties"]["periods"]

    # -- normalization into weather_documents shape ------------------------

    @staticmethod
    def normalize_alert(location_label: str, feature: dict) -> dict:
        p = feature.get("properties", {})
        narrative = " ".join(x for x in [p.get("description"), p.get("instruction")] if x).strip()
        return {
            "id": p.get("id")
            or feature.get("id")
            or _stable_id(location_label, "alert", p.get("event"), p.get("effective")),
            "location": location_label,
            "source_type": "alert",
            "headline": p.get("event") or p.get("headline"),
            "narrative_text": narrative,
            "issued_at": p.get("effective") or p.get("onset"),
            "payload": feature,
        }

    @staticmethod
    def normalize_forecast(location_label: str, period: dict) -> dict:
        return {
            "id": _stable_id(location_label, "forecast", period.get("number"), period.get("startTime")),
            "location": location_label,
            "source_type": "forecast",
            "headline": period.get("name"),
            "narrative_text": (period.get("detailedForecast") or "").strip(),
            "issued_at": period.get("startTime"),
            "payload": period,
        }

    def harvest(self, locations: list[str], limit: int = 50) -> list[dict]:
        """Fetch + normalize alerts and forecast periods for each location.

        Returns a list of normalized document dicts (capped at `limit`).
        Locations that fail to resolve are skipped, not fatal.
        """
        docs: list[dict] = []
        for loc in locations:
            try:
                point = self.resolve_point(loc)
            except Exception as exc:  # noqa: BLE001 - skip a bad location, keep going
                print(f"skip {loc!r}: {exc}")
                continue

            if point.get("state"):
                for feature in self.get_active_alerts(point["state"]):
                    doc = self.normalize_alert(loc, feature)
                    if doc["narrative_text"]:
                        docs.append(doc)

            try:
                for period in self.get_forecast_periods(point["forecast_url"]):
                    doc = self.normalize_forecast(loc, period)
                    if doc["narrative_text"]:
                        docs.append(doc)
            except Exception as exc:  # noqa: BLE001
                print(f"forecast failed for {loc!r}: {exc}")

            if len(docs) >= limit:
                break

        return docs[:limit]
from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import urlopen


BASE_URL = "https://api.odds-api.io/v3"
logger = logging.getLogger(__name__)


@dataclass(slots=True)
class FootballSelectionQuote:
    selection_key: str
    selection_name: str
    odds: float


@dataclass(slots=True)
class FootballMarketQuote:
    bookmaker: str
    market_key: str
    market_name: str
    selections: list[FootballSelectionQuote]
    updated_at: datetime | None


@dataclass(slots=True)
class FootballEventOdds:
    event_id: str
    home: str
    away: str
    sport: str
    league: str
    starts_at: datetime
    status: str
    bookmakers: dict[str, list[FootballMarketQuote]]


class OddsApiClient:
    def __init__(self, api_key: str | None = None, timeout: int = 20) -> None:
        self.api_key = api_key or os.getenv("ODDS_API_KEY", "")
        self.timeout = timeout

    def get_events(
        self,
        *,
        sport: str = "football",
        limit: int = 10,
        bookmaker: str | None = None,
        status: str | None = "pending,live",
    ) -> list[dict[str, Any]]:
        params = {"sport": sport, "limit": str(limit)}
        if bookmaker:
            params["bookmaker"] = bookmaker
        if status:
            params["status"] = status
        payload = self._request("/events", params)
        if not isinstance(payload, list):
            return []
        return payload

    def get_selected_bookmakers(self) -> list[str]:
        payload = self._request("/bookmakers/selected", {})
        if isinstance(payload, dict):
            values = payload.get("bookmakers") or payload.get("selected") or payload.get("data") or []
            if isinstance(values, list):
                return [str(item["name"]) if isinstance(item, dict) and "name" in item else str(item) for item in values]
        if isinstance(payload, list):
            return [str(item["name"]) if isinstance(item, dict) and "name" in item else str(item) for item in payload]
        return []

    def get_bookmakers(self) -> list[str]:
        payload = self._request("/bookmakers", {})
        if isinstance(payload, list):
            return [
                str(item["name"])
                for item in payload
                if isinstance(item, dict) and item.get("active") and "name" in item
            ]
        return []

    def get_odds_multi(self, event_ids: list[str], bookmakers: list[str]) -> list[FootballEventOdds]:
        if not event_ids:
            return []
        payload = self._request(
            "/odds/multi",
            {"eventIds": ",".join(event_ids), "bookmakers": ",".join(bookmakers)},
        )
        if isinstance(payload, list) and payload:
            return [self._parse_event(item) for item in payload]

        # Some API keys / regions return empty results for /odds/multi even when /odds works.
        # Fall back to per-event calls to keep the engine functional.
        events: list[FootballEventOdds] = []
        for event_id in event_ids:
            event = self.get_odds(event_id, bookmakers=bookmakers)
            if event is not None:
                events.append(event)
        return events

    def get_odds(self, event_id: str, *, bookmakers: list[str]) -> FootballEventOdds | None:
        payload = self._request(
            "/odds",
            {"eventId": str(event_id), "bookmakers": ",".join(bookmakers)},
        )
        if not isinstance(payload, dict):
            return None
        try:
            return self._parse_event(payload)
        except KeyError:
            return None

    def _request(self, path: str, params: dict[str, str]) -> Any:
        if not self.api_key:
            raise RuntimeError("ODDS_API_KEY is not set")
        query = urlencode({"apiKey": self.api_key, **params})
        url = f"{BASE_URL}{path}?{query}"
        logger.debug("Odds API request path=%s params=%s", path, params)
        try:
            with urlopen(url, timeout=self.timeout) as response:
                payload = json.loads(response.read().decode("utf-8"))
                logger.debug("Odds API response path=%s received", path)
                return payload
        except HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace").strip()
            message = body or exc.reason or f"HTTP {exc.code}"
            logger.error("Odds API error path=%s code=%s message=%s", path, exc.code, message)
            raise RuntimeError(f"Odds API request failed for {path}: {exc.code} {message}") from exc

    def _parse_event(self, payload: dict[str, Any]) -> FootballEventOdds:
        bookmakers: dict[str, list[FootballMarketQuote]] = {}
        for bookmaker, markets in payload.get("bookmakers", {}).items():
            parsed_markets: list[FootballMarketQuote] = []
            for market in markets:
                parsed_market = _parse_market(bookmaker, market)
                if parsed_market is not None:
                    parsed_markets.append(parsed_market)
            if parsed_markets:
                bookmakers[bookmaker] = parsed_markets

        return FootballEventOdds(
            event_id=str(payload["id"]),
            home=payload["home"],
            away=payload["away"],
            sport=payload.get("sport", {}).get("slug", "football"),
            league=payload.get("league", {}).get("name", "Unknown League"),
            starts_at=datetime.fromisoformat(payload["date"].replace("Z", "+00:00")),
            status=payload.get("status", "unknown"),
            bookmakers=bookmakers,
        )


def _parse_market(bookmaker: str, payload: dict[str, Any]) -> FootballMarketQuote | None:
    name = payload.get("name")
    odds_rows = payload.get("odds") or []
    if not odds_rows:
        return None
    first = odds_rows[0]

    if name == "ML":
        selections = [
            FootballSelectionQuote("home", "Home", float(first["home"])),
            FootballSelectionQuote("draw", "Draw", float(first["draw"])),
            FootballSelectionQuote("away", "Away", float(first["away"])),
        ]
        market_key = "1x2"
        market_name = "Match Winner"
    elif name in {"Over/Under", "Totals"} and "max" in first:
        line = first["max"]
        if float(line) != 2.5:
            return None
        selections = [
            FootballSelectionQuote("over", f"Over {line}", float(first["over"])),
            FootballSelectionQuote("under", f"Under {line}", float(first["under"])),
        ]
        market_key = "totals_2_5"
        market_name = f"Total Over/Under {line}"
    elif name == "Both Teams to Score":
        selections = [
            FootballSelectionQuote("yes", "BTTS Yes", float(first["yes"])),
            FootballSelectionQuote("no", "BTTS No", float(first["no"])),
        ]
        market_key = "btts"
        market_name = "Both Teams To Score"
    else:
        return None

    updated_at_raw = payload.get("updatedAt")
    updated_at = None
    if updated_at_raw:
        updated_at = datetime.fromisoformat(updated_at_raw.replace("Z", "+00:00")).astimezone(UTC)
    return FootballMarketQuote(
        bookmaker=bookmaker,
        market_key=market_key,
        market_name=market_name,
        selections=selections,
        updated_at=updated_at,
    )

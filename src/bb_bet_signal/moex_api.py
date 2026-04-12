from __future__ import annotations

import html
import json
import logging
import re
from datetime import UTC, date, datetime
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import urlopen

from .models import MoexCandle, MoexQuote


logger = logging.getLogger(__name__)
BASE_URL = "https://iss.moex.com/iss"


class MoexApiClient:
    def __init__(self, timeout: int = 20) -> None:
        self.timeout = timeout

    def get_daily_candles(
        self,
        symbol: str,
        *,
        from_date: date,
        till_date: date,
        board: str = "TQBR",
    ) -> list[MoexCandle]:
        payload = self._request(
            f"/engines/stock/markets/shares/boards/{board}/securities/{symbol}/candles.json",
            {
                "from": from_date.isoformat(),
                "till": till_date.isoformat(),
                "interval": "24",
            },
        )
        rows = _table_rows(payload, "candles")
        candles: list[MoexCandle] = []
        for row in rows:
            try:
                candles.append(
                    MoexCandle(
                        open=float(row["open"]),
                        close=float(row["close"]),
                        high=float(row["high"]),
                        low=float(row["low"]),
                        value=float(row["value"]),
                        volume=float(row["volume"]),
                        begin=_parse_datetime(row["begin"]),
                        end=_parse_datetime(row["end"]),
                    )
                )
            except (KeyError, TypeError, ValueError):
                continue
        return candles

    def get_quote(self, symbol: str, board: str = "TQBR") -> MoexQuote | None:
        payload = self._request(
            f"/engines/stock/markets/shares/securities/{symbol}.json",
            {},
        )
        market_rows = _table_rows(payload, "marketdata")
        security_rows = _table_rows(payload, "securities")
        market = next((row for row in market_rows if row.get("BOARDID") == board), None)
        security = next((row for row in security_rows if row.get("BOARDID") == board), None)
        if market is None or security is None:
            return None

        last = _as_float(market.get("LAST")) or _as_float(market.get("LCURRENTPRICE")) or _as_float(security.get("PREVPRICE"))
        prev = _as_float(security.get("PREVPRICE")) or last
        if last is None or prev is None:
            return None

        change_pct = ((last - prev) / prev) if prev else 0.0
        update_time = str(market.get("SYSTIME") or "")
        updated_at = _parse_datetime(update_time) if update_time else datetime.now(UTC)
        return MoexQuote(
            symbol=symbol,
            board=board,
            last=last,
            prev_price=prev,
            change_pct=change_pct,
            trades=int(_as_float(market.get("NUMTRADES")) or 0),
            volume=float(_as_float(market.get("VOLTODAY")) or 0.0),
            turnover=float(_as_float(market.get("VALTODAY")) or 0.0),
            updated_at=updated_at,
        )

    def get_sitenews(self, *, limit: int = 120, start: int = 0) -> list[dict[str, Any]]:
        payload = self._request(
            "/sitenews.json",
            {
                "limit": str(limit),
                "start": str(start),
            },
        )
        return _table_rows(payload, "sitenews")

    def get_sitenews_content(self, news_id: int) -> str:
        payload = self._request(f"/sitenews/{news_id}.json", {})
        rows = _table_rows(payload, "content")
        if not rows:
            return ""
        return _clean_html(rows[0].get("body") or "")

    def _request(self, path: str, params: dict[str, str]) -> dict[str, Any]:
        query = urlencode(params)
        url = f"{BASE_URL}{path}"
        if query:
            url = f"{url}?{query}"
        logger.debug("MOEX request path=%s params=%s", path, params)
        try:
            with urlopen(url, timeout=self.timeout) as response:
                return json.loads(response.read().decode("utf-8"))
        except HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace").strip()
            message = body or exc.reason or f"HTTP {exc.code}"
            logger.error("MOEX request failed path=%s code=%s message=%s", path, exc.code, message)
            raise RuntimeError(f"MOEX ISS request failed for {path}: {exc.code} {message}") from exc
        except URLError as exc:
            logger.error("MOEX request failed path=%s error=%s", path, exc)
            raise RuntimeError(f"MOEX ISS network error for {path}: {exc}") from exc


def _table_rows(payload: dict[str, Any], key: str) -> list[dict[str, Any]]:
    table = payload.get(key)
    if not isinstance(table, dict):
        return []
    columns = table.get("columns")
    data = table.get("data")
    if not isinstance(columns, list) or not isinstance(data, list):
        return []
    rows: list[dict[str, Any]] = []
    for values in data:
        if not isinstance(values, list):
            continue
        rows.append({str(column): values[index] if index < len(values) else None for index, column in enumerate(columns)})
    return rows


def _parse_datetime(value: str) -> datetime:
    raw = value.strip()
    if len(raw) == 10:
        return datetime.strptime(raw, "%Y-%m-%d").replace(tzinfo=UTC)
    return datetime.strptime(raw, "%Y-%m-%d %H:%M:%S").replace(tzinfo=UTC)


def _as_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _clean_html(raw: str) -> str:
    without_tags = re.sub(r"<[^>]+>", " ", raw)
    normalized = re.sub(r"\s+", " ", html.unescape(without_tags))
    return normalized.strip()

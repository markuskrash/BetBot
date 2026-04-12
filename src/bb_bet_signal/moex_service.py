from __future__ import annotations

import asyncio
import logging
import re
from datetime import UTC, date, datetime, timedelta
from threading import Lock

from .moex_api import MoexApiClient
from .moex_engine import MoexSignalEngine, sentiment_score
from .models import MoexEvent, MoexSignal
from .storage import MoexSignalRepository
from .telegram import TelegramNotifier


logger = logging.getLogger(__name__)


class MoexStockService:
    def __init__(
        self,
        client: MoexApiClient,
        engine: MoexSignalEngine,
        *,
        symbols: list[str],
        repository: MoexSignalRepository | None = None,
        notifier: TelegramNotifier | None = None,
        poll_seconds: int = 120,
        history_days: int = 180,
        news_limit: int = 150,
        news_window_hours: int = 72,
    ) -> None:
        self.client = client
        self.engine = engine
        self.symbols = [item.upper() for item in symbols]
        self.repository = repository
        self.notifier = notifier
        self.poll_seconds = poll_seconds
        self.history_days = history_days
        self.news_limit = news_limit
        self.news_window_hours = news_window_hours
        self._latest: list[MoexSignal] = []
        self._lock = Lock()
        self._news_body_cache: dict[int, str] = {}
        self._sent_signal_keys: set[str] = set()

    async def poll_once(self) -> list[MoexSignal]:
        logger.info("Polling MOEX symbols=%s", ",".join(self.symbols))
        events_by_symbol = self._fetch_events()
        until = date.today()
        since = until - timedelta(days=self.history_days)

        signals: list[MoexSignal] = []
        for symbol in self.symbols:
            quote = self.client.get_quote(symbol)
            if quote is None:
                logger.warning("No quote data for %s", symbol)
                continue
            candles = self.client.get_daily_candles(symbol, from_date=since, till_date=until)
            signal = self.engine.build_signal(symbol, quote, candles, events_by_symbol.get(symbol, []))
            signals.append(signal)

        signals.sort(key=lambda item: (item.action != "BUY", -item.score))
        if self.repository is not None:
            self.repository.persist_signals(signals)
        if self.notifier is not None:
            sent = self._notify_signals(signals)
            logger.info("MOEX telegram notifications sent=%s", sent)

        with self._lock:
            self._latest = signals
            return list(self._latest)

    async def run_forever(self) -> None:
        while True:
            await self.poll_once()
            logger.info("MOEX sleep for %s seconds", self.poll_seconds)
            await asyncio.sleep(self.poll_seconds)

    def latest(self) -> list[MoexSignal]:
        with self._lock:
            return list(self._latest)

    def _fetch_events(self) -> dict[str, list[MoexEvent]]:
        raw_news = self.client.get_sitenews(limit=self.news_limit)
        threshold = datetime.now(UTC) - timedelta(hours=self.news_window_hours)
        symbol_patterns = {
            symbol: re.compile(rf"(?:\b{re.escape(symbol)}\b|\({re.escape(symbol)}\))", re.IGNORECASE)
            for symbol in self.symbols
        }

        events_by_symbol = {symbol: [] for symbol in self.symbols}
        for item in raw_news:
            news_id = int(item.get("id") or 0)
            if not news_id:
                continue
            title = str(item.get("title") or "")
            published_raw = str(item.get("published_at") or "")
            if not published_raw:
                continue
            published_at = datetime.strptime(published_raw, "%Y-%m-%d %H:%M:%S").replace(tzinfo=UTC)
            if published_at < threshold:
                continue

            mentions = [symbol for symbol, pattern in symbol_patterns.items() if pattern.search(title)]
            if not mentions:
                continue

            body = self._news_body_cache.get(news_id)
            if body is None:
                body = self.client.get_sitenews_content(news_id)
                self._news_body_cache[news_id] = body
            text = f"{title} {body}".strip()
            score = sentiment_score(text)
            event = MoexEvent(
                news_id=news_id,
                published_at=published_at,
                title=title,
                body=body,
                symbols=mentions,
                sentiment_score=score,
            )
            for symbol in mentions:
                events_by_symbol[symbol].append(event)
        for symbol in self.symbols:
            events_by_symbol[symbol].sort(key=lambda item: item.published_at, reverse=True)
        return events_by_symbol

    def _notify_signals(self, signals: list[MoexSignal]) -> int:
        assert self.notifier is not None
        sent = 0
        for signal in signals:
            if signal.action not in {"BUY", "SELL"}:
                continue
            signature = f"{signal.symbol}|{signal.action}|{signal.last_price:.2f}|{signal.score:.3f}"
            if signature in self._sent_signal_keys:
                continue
            self.notifier.send_message(_format_stock_signal(signal))
            self._sent_signal_keys.add(signature)
            sent += 1
        return sent


def _format_stock_signal(signal: MoexSignal) -> str:
    return "\n".join(
        [
            f"MOEX {signal.symbol}: {signal.action}",
            f"Price: {signal.last_price:.2f} RUB",
            f"Score: {signal.score:.2f} (tech {signal.technical_score:.2f}, events {signal.event_score:.2f})",
            f"Confidence: {signal.confidence:.0%}",
            f"Expected move: {signal.expected_move_pct:.2%}",
            f"Position: {signal.position_share:.2%}",
            f"Events used: {signal.event_count}",
        ]
    )

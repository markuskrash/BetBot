from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime
from threading import Lock

from .football_api import OddsApiClient
from .football_engine import FootballConsensusEngine
from .models import Recommendation
from .storage import SnapshotRepository
from .telegram import TelegramNotifier


logger = logging.getLogger(__name__)


class FootballPollingService:
    def __init__(
        self,
        client: OddsApiClient,
        engine: FootballConsensusEngine,
        repository: SnapshotRepository | None = None,
        *,
        target_bookmaker: str,
        comparison_bookmakers: list[str],
        notifier: TelegramNotifier | None = None,
        poll_seconds: int = 300,
        event_limit: int = 10,
    ) -> None:
        self.client = client
        self.engine = engine
        self.repository = repository
        self.target_bookmaker = target_bookmaker
        self.comparison_bookmakers = comparison_bookmakers
        self.notifier = notifier
        self.poll_seconds = poll_seconds
        self.event_limit = event_limit
        self._latest: list[Recommendation] = []
        self._lock = Lock()

    async def poll_once(self) -> list[Recommendation]:
        bookmaker_set = self._resolve_bookmakers()
        logger.info("Polling football odds bookmakers=%s limit=%s", ",".join(bookmaker_set), self.event_limit)
        # Request only events that have odds for the target bookmaker;
        # otherwise /odds/multi may legitimately return an empty list.
        events = self.client.get_events(limit=self.event_limit, bookmaker=self.target_bookmaker)
        event_ids = [str(item["id"]) for item in events if "id" in item]
        logger.info("Fetched events count=%s", len(event_ids))
        event_odds = self.client.get_odds_multi(event_ids, bookmaker_set)
        recommendations: list[Recommendation] = []
        captured_at = datetime.now(UTC)
        for event in event_odds:
            if self.repository is not None:
                self.repository.persist_event(event, captured_at)
            recommendations.extend(self.engine.evaluate(event, self.target_bookmaker))
        recommendations.sort(key=lambda item: (item.expected_value, item.edge), reverse=True)
        logger.info("Evaluated odds events=%s recommendations=%s", len(event_odds), len(recommendations))
        if self.notifier is not None:
            sent = self.notifier.notify_recommendations(recommendations)
            logger.info("Telegram notifications sent=%s", sent)
        with self._lock:
            self._latest = recommendations[:25]
            return list(self._latest)

    async def run_forever(self) -> None:
        while True:
            try:
                await self.poll_once()
            except Exception:
                logger.exception("Football poll failed; retrying after backoff")
                await asyncio.sleep(min(60, max(10, self.poll_seconds)))
                continue
            logger.info("Sleeping for %s seconds before next poll", self.poll_seconds)
            await asyncio.sleep(self.poll_seconds)

    def latest(self) -> list[Recommendation]:
        with self._lock:
            return list(self._latest)

    def _resolve_bookmakers(self) -> list[str]:
        valid = self.client.get_bookmakers()
        if valid and self.target_bookmaker not in valid:
            preview = ", ".join(valid[:10])
            raise RuntimeError(
                f"Target bookmaker '{self.target_bookmaker}' is not a valid bookmaker for Odds API. "
                f"Valid examples: {preview}"
            )

        selected = self.client.get_selected_bookmakers()
        if selected and self.target_bookmaker not in selected:
            preview = ", ".join(selected[:10])
            raise RuntimeError(
                f"Target bookmaker '{self.target_bookmaker}' is unavailable for this API key. "
                f"Available: {preview}"
            )

        pool = selected or valid
        if not pool:
            return [self.target_bookmaker, *self.comparison_bookmakers]

        comparison = [
            name for name in self.comparison_bookmakers
            if name in pool and name != self.target_bookmaker
        ]
        if not comparison:
            comparison = [name for name in pool if name != self.target_bookmaker][:3]
        if not comparison:
            raise RuntimeError("Need at least one comparison bookmaker for consensus odds")
        logger.info("Resolved bookmakers target=%s comparison=%s", self.target_bookmaker, ",".join(comparison))
        return [self.target_bookmaker, *comparison]

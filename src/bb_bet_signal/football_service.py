from __future__ import annotations

import asyncio
import logging
from collections import defaultdict
from datetime import UTC, datetime
from threading import Lock

from .football_api import OddsApiClient
from .football_engine import FootballConsensusEngine
from .football_risk import FootballRiskGovernor, FootballRiskState
from .models import ExpressRecommendation, Recommendation
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
        risk_governor: FootballRiskGovernor,
        enable_btts: bool = False,
        express_mode: str = "two-leg",
        notifier: TelegramNotifier | None = None,
        poll_seconds: int = 300,
        event_limit: int = 10,
    ) -> None:
        self.client = client
        self.engine = engine
        self.repository = repository
        self.target_bookmaker = target_bookmaker
        self.comparison_bookmakers = comparison_bookmakers
        self.risk_governor = risk_governor
        self.enable_btts = enable_btts
        self.express_mode = express_mode
        self.notifier = notifier
        self.poll_seconds = poll_seconds
        self.event_limit = event_limit
        self._latest: list[Recommendation] = []
        self._latest_expresses: list[ExpressRecommendation] = []
        self._risk_state: FootballRiskState | None = None
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
        captured_at = datetime.now(UTC)
        risk_state = self.risk_governor.refresh(event_odds, captured_at)

        allowed_markets = {"1x2", "totals_2_5"}
        if self.enable_btts:
            allowed_markets.add("btts")
        recommendations: list[Recommendation] = []
        for event in event_odds:
            if self.repository is not None:
                self.repository.persist_event(event, captured_at)
            recommendations.extend(
                self.engine.evaluate(
                    event,
                    self.target_bookmaker,
                    allowed_markets=allowed_markets,
                    min_edge=risk_state.current_min_edge,
                    min_ev=risk_state.current_min_ev,
                )
            )
        recommendations.sort(key=lambda item: (item.expected_value, item.edge), reverse=True)
        recommendations = _apply_league_cap(recommendations, per_league_cap=2)
        max_legs = 2 if self.express_mode == "two-leg" else 3
        expresses = self.engine.build_expresses(recommendations, max_legs=max_legs)
        recommendations, expresses = self.risk_governor.apply_and_place(
            recommendations,
            expresses,
            risk_state,
            captured_at,
        )
        logger.info("Evaluated odds events=%s recommendations=%s", len(event_odds), len(recommendations))
        if self.notifier is not None:
            singles_sent = self.notifier.notify_recommendations(recommendations)
            expresses_sent = self.notifier.notify_expresses(expresses)
            logger.info("Telegram notifications sent singles=%s expresses=%s", singles_sent, expresses_sent)
        with self._lock:
            self._latest = recommendations[:25]
            self._latest_expresses = expresses[:10]
            self._risk_state = risk_state
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

    def latest_expresses(self) -> list[ExpressRecommendation]:
        with self._lock:
            return list(self._latest_expresses)

    def risk_state(self) -> dict:
        with self._lock:
            if self._risk_state is None:
                return {}
            return self._risk_state.to_dict()

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


def _apply_league_cap(recommendations: list[Recommendation], per_league_cap: int) -> list[Recommendation]:
    if per_league_cap <= 0:
        return []
    counts: dict[str, int] = defaultdict(int)
    filtered: list[Recommendation] = []
    for item in sorted(recommendations, key=lambda rec: (rec.expected_value, rec.edge), reverse=True):
        if counts[item.league] >= per_league_cap:
            continue
        filtered.append(item)
        counts[item.league] += 1
    return filtered

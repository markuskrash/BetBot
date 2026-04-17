from __future__ import annotations

import asyncio
import logging
from collections import defaultdict, deque
from datetime import UTC, date, datetime
from threading import Lock
from typing import Any

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
        target_bets_per_day: int = 4,
        min_minutes_to_start: int = 45,
        max_minutes_to_start: int = 240,
        stale_market_minutes: int = 15,
        realert_odds_delta: float = 0.03,
        realert_ev_delta: float = 0.01,
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
        self.target_bets_per_day = target_bets_per_day
        self.min_minutes_to_start = min_minutes_to_start
        self.max_minutes_to_start = max_minutes_to_start
        self.stale_market_minutes = stale_market_minutes
        self.realert_odds_delta = realert_odds_delta
        self.realert_ev_delta = realert_ev_delta
        self._latest: list[Recommendation] = []
        self._latest_expresses: list[ExpressRecommendation] = []
        self._risk_state: FootballRiskState | None = None
        self._lock = Lock()
        self._cycle_index = 0
        self._signal_history: deque[int] = deque(maxlen=12)
        self._edge_tuning_offset = 0.0
        self._ev_tuning_offset = 0.0
        self._latest_projection = 0.0
        if self.notifier is not None:
            self.notifier.realert_odds_delta = realert_odds_delta
            self.notifier.realert_ev_delta = realert_ev_delta

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

        effective_min_edge = _clamp(risk_state.current_min_edge + self._edge_tuning_offset, 0.008, 0.040)
        effective_min_ev = _clamp(risk_state.current_min_ev + self._ev_tuning_offset, 0.012, 0.035)
        allowed_markets = {"1x2", "totals_2_5"}
        if self.enable_btts:
            allowed_markets.add("btts")

        recommendations: list[Recommendation] = []
        decision_entries: list[dict[str, Any]] = []
        for event in event_odds:
            if self.repository is not None:
                self.repository.persist_event(event, captured_at)
            event_recommendations, event_decisions = self.engine.evaluate_with_trace(
                event,
                self.target_bookmaker,
                allowed_markets=allowed_markets,
                min_edge=effective_min_edge,
                min_ev=effective_min_ev,
                min_minutes_to_start=self.min_minutes_to_start,
                max_minutes_to_start=self.max_minutes_to_start,
                stale_market_minutes=self.stale_market_minutes,
                now=captured_at,
            )
            recommendations.extend(event_recommendations)
            decision_entries.extend(event_decisions)

        recommendations = _rerank_cycle_recommendations(recommendations)
        recommendations = _apply_league_cap(recommendations, per_league_cap=2)
        recommendations, gate_entries = self._apply_league_sample_gate(recommendations, captured_at.date())
        decision_entries.extend(gate_entries)
        max_legs = 2 if self.express_mode == "two-leg" else 3
        expresses = self.engine.build_expresses(recommendations, max_legs=max_legs)
        recommendations, expresses = self.risk_governor.apply_and_place(
            recommendations,
            expresses,
            risk_state,
            captured_at,
        )
        for recommendation in recommendations:
            if recommendation.blocked_by_risk:
                decision_entries.append(_decision_entry_from_recommendation(recommendation, "rejected", "risk_block"))
            else:
                decision_entries.append(_decision_entry_from_recommendation(recommendation, "accepted", "accepted"))
        if self.repository is not None:
            self.repository.log_decisions(decision_entries, captured_at)
        self._log_decision_summary(decision_entries, len(event_odds))
        self._update_frequency_controller(recommendations)
        logger.info(
            "Evaluated odds events=%s recommendations=%s thresholds edge=%.4f ev=%.4f projected_day=%.2f",
            len(event_odds),
            len(recommendations),
            effective_min_edge,
            effective_min_ev,
            self._latest_projection,
        )
        if self.notifier is not None:
            singles_sent = self.notifier.notify_recommendations(recommendations, limit=3)
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

    def performance(self) -> dict:
        if self.repository is None:
            return {}
        payload = self.repository.performance_snapshot()
        payload["target_bets_per_day"] = self.target_bets_per_day
        payload["latest_projection_per_day"] = self._latest_projection
        return payload

    def _update_frequency_controller(self, recommendations: list[Recommendation]) -> None:
        actionable = [
            item
            for item in recommendations
            if not item.blocked_by_risk and item.tier in {"A", "B"}
        ]
        self._signal_history.append(len(actionable))
        self._cycle_index += 1
        if self._cycle_index % 12 != 0 or len(self._signal_history) < 12:
            return

        average_per_cycle = sum(self._signal_history) / len(self._signal_history)
        cycles_per_day = max(1.0, 86400.0 / max(float(self.poll_seconds), 1.0))
        projected = average_per_cycle * cycles_per_day
        self._latest_projection = projected
        lower_target = max(1.0, float(self.target_bets_per_day - 1))
        upper_target = float(self.target_bets_per_day + 2)
        if projected < lower_target:
            self._edge_tuning_offset -= 0.004
            self._ev_tuning_offset -= 0.003
        elif projected > upper_target:
            self._edge_tuning_offset += 0.004
            self._ev_tuning_offset += 0.003
        self._edge_tuning_offset = _clamp(self._edge_tuning_offset, -0.02, 0.02)
        self._ev_tuning_offset = _clamp(self._ev_tuning_offset, -0.02, 0.02)

    def _apply_league_sample_gate(
        self,
        recommendations: list[Recommendation],
        stat_date: date,
    ) -> tuple[list[Recommendation], list[dict[str, Any]]]:
        if self.repository is None or not recommendations:
            return recommendations, []
        gates = self.repository.league_sample_gate(30, stat_date, sample_gate=30)
        if not gates:
            return recommendations, []

        filtered: list[Recommendation] = []
        entries: list[dict[str, Any]] = []
        for recommendation in recommendations:
            gate = gates.get(recommendation.league)
            if gate is None:
                filtered.append(recommendation)
                continue
            action = str(gate.get("action") or "")
            if action == "ban":
                entries.append(_decision_entry_from_recommendation(recommendation, "rejected", "league_ban"))
                continue
            if action == "penalty":
                recommendation.priority_score = round(recommendation.priority_score - 0.25, 6)
                recommendation.recommended_stake = round(recommendation.recommended_stake * 0.8, 2)
                recommendation.decision_tags.append("league_penalty_30d")
                entries.append(_decision_entry_from_recommendation(recommendation, "accepted", "league_penalty"))
            filtered.append(recommendation)
        filtered.sort(
            key=lambda item: (item.priority_score, item.expected_value, item.edge),
            reverse=True,
        )
        return filtered, entries

    def _log_decision_summary(self, entries: list[dict[str, Any]], event_count: int) -> None:
        rejected_by_reason: dict[str, int] = defaultdict(int)
        accepted = 0
        rejected = 0
        for item in entries:
            decision = str(item.get("decision") or "")
            if decision == "accepted":
                accepted += 1
                continue
            rejected += 1
            reason = str(item.get("reason") or "unknown")
            rejected_by_reason[reason] += 1
        reason_preview = ",".join(f"{key}:{value}" for key, value in sorted(rejected_by_reason.items()))
        logger.info(
            "Decision summary events=%s accepted=%s rejected=%s reasons=%s",
            event_count,
            accepted,
            rejected,
            reason_preview,
        )

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
    for item in sorted(
        recommendations,
        key=lambda recommendation: (recommendation.priority_score, recommendation.expected_value, recommendation.edge),
        reverse=True,
    ):
        if counts[item.league] >= per_league_cap:
            continue
        filtered.append(item)
        counts[item.league] += 1
    return filtered


def _rerank_cycle_recommendations(recommendations: list[Recommendation]) -> list[Recommendation]:
    if not recommendations:
        return []
    ev_values = [item.expected_value for item in recommendations]
    edge_values = [item.edge for item in recommendations]
    price_values = [item.price_advantage for item in recommendations]
    ev_mean, ev_std = _mean_std(ev_values)
    edge_mean, edge_std = _mean_std(edge_values)
    price_mean, price_std = _mean_std(price_values)
    for recommendation in recommendations:
        disagreement_penalty = _disagreement_penalty_from_tags(recommendation.decision_tags)
        z_ev = _zscore(recommendation.expected_value, ev_mean, ev_std)
        z_edge = _zscore(recommendation.edge, edge_mean, edge_std)
        z_price = _zscore(recommendation.price_advantage, price_mean, price_std)
        recommendation.priority_score = round(
            0.45 * z_ev + 0.30 * z_edge + 0.25 * z_price - 0.20 * disagreement_penalty,
            6,
        )

    ranked = sorted(
        recommendations,
        key=lambda recommendation: (recommendation.priority_score, recommendation.expected_value, recommendation.edge),
        reverse=True,
    )
    size = len(ranked)
    tier_a = max(1, (size + 2) // 3)
    tier_b = max(1, (size - tier_a + 1) // 2) if size > tier_a else 0
    for index, recommendation in enumerate(ranked):
        if index < tier_a:
            recommendation.tier = "A"
        elif index < tier_a + tier_b:
            recommendation.tier = "B"
        else:
            recommendation.tier = "C"
    return ranked


def _disagreement_penalty_from_tags(tags: list[str]) -> float:
    for tag in tags:
        if not tag.startswith("disagreement_penalty="):
            continue
        value = tag.split("=", 1)[1]
        try:
            return float(value)
        except ValueError:
            return 0.0
    return 0.0


def _mean_std(values: list[float]) -> tuple[float, float]:
    if not values:
        return 0.0, 0.0
    avg = sum(values) / len(values)
    if len(values) < 2:
        return avg, 0.0
    variance = sum((value - avg) ** 2 for value in values) / len(values)
    return avg, variance ** 0.5


def _zscore(value: float, avg: float, std: float) -> float:
    if std <= 1e-9:
        return 0.0
    return (value - avg) / std


def _decision_entry_from_recommendation(
    recommendation: Recommendation,
    decision: str,
    reason: str,
) -> dict[str, Any]:
    return {
        "event_id": recommendation.event_id,
        "league": recommendation.league,
        "market_key": recommendation.market_key,
        "selection_key": recommendation.selection_key,
        "bookmaker": recommendation.bookmaker,
        "odds": recommendation.odds,
        "minutes_to_start": recommendation.minutes_to_start,
        "price_advantage": recommendation.price_advantage,
        "edge": recommendation.edge,
        "expected_value": recommendation.expected_value,
        "priority_score": recommendation.priority_score,
        "tier": recommendation.tier,
        "decision": decision,
        "reason": reason,
    }


def _clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))

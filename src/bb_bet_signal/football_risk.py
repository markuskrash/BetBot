from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime

from .config import FootballRiskConfig
from .football_api import FootballEventOdds
from .models import ExpressRecommendation, Recommendation
from .storage import SnapshotRepository


@dataclass(slots=True)
class FootballRiskState:
    stat_date: str
    bankroll: float
    daily_pnl: float
    daily_roi: float
    rolling_7d_roi: float
    rolling_7d_hit_rate: float
    rolling_7d_clv_proxy: float
    open_bets: int
    bets_today: int
    blocked: bool
    block_reason: str
    adaptive_edge_boost: float
    adaptive_ev_boost: float
    adaptive_stake_multiplier: float
    current_min_edge: float
    current_min_ev: float

    def to_dict(self) -> dict[str, float | int | str | bool]:
        return asdict(self)


class FootballRiskGovernor:
    def __init__(
        self,
        repository: SnapshotRepository,
        *,
        bankroll: float,
        risk_config: FootballRiskConfig,
        base_min_edge: float,
        base_min_ev: float,
    ) -> None:
        self.repository = repository
        self.bankroll = bankroll
        self.risk_config = risk_config
        self.base_min_edge = base_min_edge
        self.base_min_ev = base_min_ev

    def refresh(self, event_odds: list[FootballEventOdds], now: datetime | None = None) -> FootballRiskState:
        timestamp = now or datetime.now(UTC)
        for event in event_odds:
            self.repository.upsert_event_result(event, timestamp)
        self.repository.settle_open_bets(timestamp)

        stat_date = timestamp.date()
        daily = self.repository.daily_closed_metrics(stat_date)
        rolling = self.repository.rolling_closed_metrics(7, stat_date)
        open_count = self.repository.open_bets_count()
        placed_count = self.repository.daily_placed_count(stat_date)

        adaptive_edge_boost = _clamp(-rolling["roi"] * 0.5, 0.0, 0.02)
        adaptive_ev_boost = _clamp(-rolling["roi"] * 0.35, 0.0, 0.015)
        adaptive_stake_multiplier = _clamp(1.0 + rolling["roi"] * 2.0, 0.55, 1.0)

        blocked = daily["pnl"] <= -(self.bankroll * self.risk_config.daily_drawdown_limit)
        reason = (
            f"daily drawdown reached {daily['pnl']:.2f} <= -{self.bankroll * self.risk_config.daily_drawdown_limit:.2f}"
            if blocked
            else ""
        )

        self.repository.upsert_daily_stats(
            stat_date,
            placed_count=placed_count,
            settled_count=int(daily["settled_count"]),
            open_count=open_count,
            pnl=daily["pnl"],
            turnover=daily["turnover"],
            roi=daily["roi"],
            hit_rate=daily["hit_rate"],
            clv_proxy=daily["clv_proxy"],
            stop_triggered=blocked,
            updated_at=timestamp,
        )
        self.repository.set_stop_status(stat_date, blocked, reason, timestamp)

        return FootballRiskState(
            stat_date=stat_date.isoformat(),
            bankroll=self.bankroll,
            daily_pnl=daily["pnl"],
            daily_roi=daily["roi"],
            rolling_7d_roi=rolling["roi"],
            rolling_7d_hit_rate=rolling["hit_rate"],
            rolling_7d_clv_proxy=rolling["clv_proxy"],
            open_bets=open_count,
            bets_today=placed_count,
            blocked=blocked,
            block_reason=reason,
            adaptive_edge_boost=adaptive_edge_boost,
            adaptive_ev_boost=adaptive_ev_boost,
            adaptive_stake_multiplier=adaptive_stake_multiplier,
            current_min_edge=self.base_min_edge + adaptive_edge_boost,
            current_min_ev=self.base_min_ev + adaptive_ev_boost,
        )

    def apply_and_place(
        self,
        recommendations: list[Recommendation],
        expresses: list[ExpressRecommendation],
        risk_state: FootballRiskState,
        now: datetime | None = None,
    ) -> tuple[list[Recommendation], list[ExpressRecommendation]]:
        timestamp = now or datetime.now(UTC)
        if risk_state.blocked:
            return _block_recommendations(recommendations), _block_expresses(expresses)

        remaining_open_slots = max(0, self.risk_config.max_open_bets - risk_state.open_bets)
        remaining_day_slots = max(0, self.risk_config.max_bets_per_day - risk_state.bets_today)
        remaining_slots = min(remaining_open_slots, remaining_day_slots)
        if remaining_slots <= 0:
            return [], []

        accepted_singles: list[Recommendation] = []
        single_cap = self.bankroll * self.risk_config.max_single_share * risk_state.adaptive_stake_multiplier
        for recommendation in recommendations:
            if remaining_slots <= 0:
                break
            capped = max(0.0, min(recommendation.recommended_stake, single_cap))
            if capped < 1:
                continue
            recommendation.recommended_stake = round(capped, 2)
            if self.repository.place_single_bet(recommendation, timestamp):
                accepted_singles.append(recommendation)
                remaining_slots -= 1

        accepted_expresses: list[ExpressRecommendation] = []
        express_cap = self.bankroll * self.risk_config.max_express_share * risk_state.adaptive_stake_multiplier
        for express in expresses:
            if remaining_slots <= 0:
                break
            capped = max(0.0, min(express.recommended_stake, express_cap))
            if capped < 1:
                continue
            express.recommended_stake = round(capped, 2)
            if self.repository.place_express_bet(express, timestamp):
                accepted_expresses.append(express)
                remaining_slots -= 1

        return accepted_singles, accepted_expresses


def _block_recommendations(recommendations: list[Recommendation]) -> list[Recommendation]:
    for item in recommendations:
        item.blocked_by_risk = True
        item.recommended_stake = 0.0
    return recommendations


def _block_expresses(expresses: list[ExpressRecommendation]) -> list[ExpressRecommendation]:
    for item in expresses:
        item.blocked_by_risk = True
        item.recommended_stake = 0.0
    return expresses


def _clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))

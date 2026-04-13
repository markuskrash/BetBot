from __future__ import annotations

from collections import defaultdict
from datetime import UTC, datetime
from itertools import combinations
from statistics import mean, pstdev

from .config import EngineConfig
from .football_api import FootballEventOdds, FootballMarketQuote
from .models import ExpressLeg, ExpressRecommendation, Recommendation


class FootballConsensusEngine:
    def __init__(self, config: EngineConfig | None = None) -> None:
        self.config = config or EngineConfig()

    def evaluate(
        self,
        event: FootballEventOdds,
        target_bookmaker: str,
        *,
        allowed_markets: set[str] | None = None,
        min_edge: float | None = None,
        min_ev: float | None = None,
    ) -> list[Recommendation]:
        threshold_edge = min_edge if min_edge is not None else self.config.min_edge
        threshold_ev = min_ev if min_ev is not None else self.config.min_expected_value
        target_markets = {
            market.market_key: market
            for market in event.bookmakers.get(target_bookmaker, [])
            if allowed_markets is None or market.market_key in allowed_markets
        }
        if not target_markets:
            return []

        recommendations: list[Recommendation] = []
        for market_key, target_market in target_markets.items():
            consensus, avg_odds, comparison_count, disagreement = self._consensus(event, market_key, exclude=target_bookmaker)
            if not consensus:
                continue
            if comparison_count < 2:
                # Too fragile on a single external source: skip market entirely.
                continue
            for selection in target_market.selections:
                fair_probability = consensus.get(selection.selection_key)
                if fair_probability is None:
                    continue
                target_odds = selection.odds
                if target_odds > 4.5:
                    continue
                market_average_odds = avg_odds.get(selection.selection_key)
                if market_average_odds is None:
                    continue
                price_advantage = (target_odds / market_average_odds) - 1.0
                if price_advantage < 0.02:
                    continue

                implied_probability = 1.0 / selection.odds
                adjusted_probability = _blend_probability(
                    fair_probability,
                    implied_probability,
                    disagreement.get(selection.selection_key, 0.0),
                )
                edge = adjusted_probability - implied_probability
                expected_value = adjusted_probability * selection.odds - 1.0
                dynamic_edge, dynamic_ev = _dynamic_thresholds(
                    disagreement.get(selection.selection_key, 0.0),
                    base_edge=threshold_edge,
                    base_ev=threshold_ev,
                )
                if edge < dynamic_edge or expected_value < dynamic_ev:
                    continue
                stake = self._stake(selection.odds, adjusted_probability, comparison_count=comparison_count, disagreement=disagreement.get(selection.selection_key, 0.0))
                recommendations.append(
                    Recommendation(
                        event_id=event.event_id,
                        event_name=f"{event.home} vs {event.away}",
                        sport=event.sport,
                        league=event.league,
                        market_key=market_key,
                        market_name=target_market.market_name,
                        selection_key=selection.selection_key,
                        selection_name=selection.selection_name,
                        bookmaker=target_bookmaker,
                        odds=selection.odds,
                        implied_probability=implied_probability,
                        model_probability=adjusted_probability,
                        edge=edge,
                        expected_value=expected_value,
                        recommended_stake=stake,
                        generated_at=datetime.now(UTC),
                        starts_at=event.starts_at,
                        reasons=[
                            f"consensus {fair_probability:.2%}",
                            f"adjusted {adjusted_probability:.2%}",
                            f"target {implied_probability:.2%}",
                            f"price advantage {price_advantage:.2%}",
                            f"comparison books {comparison_count}",
                        ],
                    )
                )
        recommendations.sort(key=lambda item: (item.expected_value, item.edge), reverse=True)
        by_event: dict[str, Recommendation] = {}
        for recommendation in recommendations:
            current = by_event.get(recommendation.event_id)
            if current is None or recommendation.expected_value > current.expected_value:
                by_event[recommendation.event_id] = recommendation
        return sorted(by_event.values(), key=lambda item: (item.expected_value, item.edge), reverse=True)

    def build_expresses(
        self,
        recommendations: list[Recommendation],
        *,
        max_legs: int = 2,
        max_expresses: int = 5,
    ) -> list[ExpressRecommendation]:
        if len(recommendations) < 2:
            return []
        unique = _unique_by_event(recommendations)
        if len(unique) < 2:
            return []

        candidates = unique[:8]
        now = datetime.now(UTC)
        expresses: list[ExpressRecommendation] = []
        for legs_count in range(2, min(max_legs, len(candidates)) + 1):
            for combo in combinations(candidates, legs_count):
                if not _valid_express_combo(combo):
                    continue
                total_odds = 1.0
                model_prob = 1.0
                min_leg_stake = min(item.recommended_stake for item in combo)
                avg_leg_ev = mean(item.expected_value for item in combo)
                for leg in combo:
                    total_odds *= leg.odds
                    model_prob *= leg.model_probability
                # Penalize for correlation risk between legs.
                model_prob *= 0.92 ** (legs_count - 1)
                implied_probability = 1.0 / total_odds
                edge = model_prob - implied_probability
                expected_value = model_prob * total_odds - 1.0
                if total_odds < 1.8 or total_odds > 9.0:
                    continue
                if edge < max(0.01, self.config.min_edge * 0.6):
                    continue
                if expected_value < max(0.03, self.config.min_expected_value * 1.2, avg_leg_ev + 0.015):
                    continue

                express_stake = min_leg_stake * 0.28
                express_stake = min(express_stake, self.config.bankroll * 0.004)
                if express_stake < 1:
                    continue

                legs = [
                    ExpressLeg(
                        event_id=item.event_id,
                        event_name=item.event_name,
                        market_name=item.market_name,
                        selection_key=item.selection_key,
                        selection_name=item.selection_name,
                        odds=item.odds,
                        model_probability=item.model_probability,
                    )
                    for item in combo
                ]
                express_id = "+".join(item.event_id for item in combo)
                expresses.append(
                    ExpressRecommendation(
                        express_id=express_id,
                        legs=legs,
                        total_odds=round(total_odds, 2),
                        implied_probability=implied_probability,
                        model_probability=model_prob,
                        edge=edge,
                        expected_value=expected_value,
                        recommended_stake=round(express_stake, 2),
                        generated_at=now,
                        reasons=[
                            f"legs {legs_count}",
                            f"odds {total_odds:.2f}",
                            f"ev {expected_value:.2%}",
                        ],
                    )
                )
        expresses.sort(key=lambda item: (item.expected_value, item.edge), reverse=True)
        return expresses[:max_expresses]

    def _consensus(
        self,
        event: FootballEventOdds,
        market_key: str,
        exclude: str,
    ) -> tuple[dict[str, float], dict[str, float], int, dict[str, float]]:
        collected: dict[str, list[float]] = defaultdict(list)
        collected_odds: dict[str, list[float]] = defaultdict(list)
        comparison_count = 0
        for bookmaker, markets in event.bookmakers.items():
            if bookmaker == exclude:
                continue
            market = next((item for item in markets if item.market_key == market_key), None)
            if market is None:
                continue
            comparison_count += 1
            normalized = _normalize_market(market)
            for selection_key, probability in normalized.items():
                collected[selection_key].append(probability)
            for selection in market.selections:
                collected_odds[selection.selection_key].append(selection.odds)
        if len(collected) < 2:
            return {}, {}, 0, {}
        averaged = {key: sum(values) / len(values) for key, values in collected.items() if values}
        total = sum(averaged.values())
        if total <= 0:
            return {}, {}, 0, {}
        normalized_consensus = {key: value / total for key, value in averaged.items()}
        avg_odds = {key: mean(values) for key, values in collected_odds.items() if values}
        disagreement = {key: pstdev(values) if len(values) > 1 else 0.0 for key, values in collected.items() if values}
        return normalized_consensus, avg_odds, comparison_count, disagreement

    def _coverage(self, event: FootballEventOdds, market_key: str) -> int:
        return sum(1 for markets in event.bookmakers.values() if any(item.market_key == market_key for item in markets))

    def _stake(
        self,
        odds: float,
        probability: float,
        *,
        comparison_count: int,
        disagreement: float,
    ) -> float:
        numerator = ((odds - 1.0) * probability) - (1.0 - probability)
        denominator = max(odds - 1.0, 1e-9)
        raw_fraction = numerator / denominator
        confidence_discount = min(1.0, comparison_count / 4) * max(0.45, 1.0 - disagreement * 2.8)
        conservative_fraction = raw_fraction * confidence_discount * 0.55
        fraction = max(0.0, min(conservative_fraction, self.config.max_fractional_kelly))
        stake = min(
            self.config.bankroll * fraction,
            self.config.bankroll * min(self.config.max_stake_share, 0.02),
        )
        return round(stake, 2)


def _normalize_market(market: FootballMarketQuote) -> dict[str, float]:
    implied = {selection.selection_key: 1.0 / selection.odds for selection in market.selections}
    total = sum(implied.values())
    return {key: value / total for key, value in implied.items()}


def _blend_probability(consensus: float, implied: float, disagreement: float) -> float:
    # Shrink toward market-implied when bookmaker disagreement is high.
    shrink = min(0.45, max(0.12, disagreement * 3.0))
    return (1.0 - shrink) * consensus + shrink * implied


def _unique_by_event(recommendations: list[Recommendation]) -> list[Recommendation]:
    by_event: dict[str, Recommendation] = {}
    for item in recommendations:
        current = by_event.get(item.event_id)
        if current is None or item.expected_value > current.expected_value:
            by_event[item.event_id] = item
    return sorted(by_event.values(), key=lambda item: (item.expected_value, item.edge), reverse=True)


def _dynamic_thresholds(disagreement: float, *, base_edge: float, base_ev: float) -> tuple[float, float]:
    if disagreement >= 0.06:
        return base_edge + 0.02, base_ev + 0.015
    if disagreement >= 0.04:
        return base_edge + 0.012, base_ev + 0.01
    if disagreement >= 0.025:
        return base_edge + 0.006, base_ev + 0.005
    return base_edge, base_ev


def _valid_express_combo(combo: tuple[Recommendation, ...]) -> bool:
    event_ids = {item.event_id for item in combo}
    if len(event_ids) != len(combo):
        return False
    leagues = {item.league for item in combo}
    if len(leagues) != len(combo):
        return False

    starts = [item.starts_at for item in combo]
    if any(start is None for start in starts):
        return False
    starts_sorted = sorted(starts)
    for idx in range(1, len(starts_sorted)):
        delta_minutes = abs((starts_sorted[idx] - starts_sorted[idx - 1]).total_seconds()) / 60
        if delta_minutes < 60:
            return False
    return True

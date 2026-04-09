from __future__ import annotations

from collections import defaultdict
from datetime import UTC, datetime

from .config import EngineConfig
from .football_api import FootballEventOdds, FootballMarketQuote
from .models import Recommendation


class FootballConsensusEngine:
    def __init__(self, config: EngineConfig | None = None) -> None:
        self.config = config or EngineConfig()

    def evaluate(self, event: FootballEventOdds, target_bookmaker: str) -> list[Recommendation]:
        target_markets = {market.market_key: market for market in event.bookmakers.get(target_bookmaker, [])}
        if not target_markets:
            return []

        recommendations: list[Recommendation] = []
        for market_key, target_market in target_markets.items():
            consensus = self._consensus(event, market_key, exclude=target_bookmaker)
            if not consensus:
                continue
            for selection in target_market.selections:
                fair_probability = consensus.get(selection.selection_key)
                if fair_probability is None:
                    continue
                implied_probability = 1.0 / selection.odds
                edge = fair_probability - implied_probability
                expected_value = fair_probability * selection.odds - 1.0
                if edge < self.config.min_edge or expected_value < self.config.min_expected_value:
                    continue
                stake = self._stake(selection.odds, fair_probability)
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
                        model_probability=fair_probability,
                        edge=edge,
                        expected_value=expected_value,
                        recommended_stake=stake,
                        generated_at=datetime.now(UTC),
                        reasons=[
                            f"consensus {fair_probability:.2%}",
                            f"target {implied_probability:.2%}",
                            f"market coverage {self._coverage(event, market_key)} bookmakers",
                        ],
                    )
                )
        recommendations.sort(key=lambda item: (item.expected_value, item.edge), reverse=True)
        return recommendations

    def _consensus(self, event: FootballEventOdds, market_key: str, exclude: str) -> dict[str, float]:
        collected: dict[str, list[float]] = defaultdict(list)
        for bookmaker, markets in event.bookmakers.items():
            if bookmaker == exclude:
                continue
            market = next((item for item in markets if item.market_key == market_key), None)
            if market is None:
                continue
            normalized = _normalize_market(market)
            for selection_key, probability in normalized.items():
                collected[selection_key].append(probability)
        if len(collected) < 2:
            return {}
        averaged = {key: sum(values) / len(values) for key, values in collected.items() if values}
        total = sum(averaged.values())
        if total <= 0:
            return {}
        return {key: value / total for key, value in averaged.items()}

    def _coverage(self, event: FootballEventOdds, market_key: str) -> int:
        return sum(1 for markets in event.bookmakers.values() if any(item.market_key == market_key for item in markets))

    def _stake(self, odds: float, probability: float) -> float:
        numerator = ((odds - 1.0) * probability) - (1.0 - probability)
        denominator = max(odds - 1.0, 1e-9)
        fraction = max(0.0, min(numerator / denominator, self.config.max_fractional_kelly))
        stake = min(
            self.config.bankroll * fraction,
            self.config.bankroll * self.config.max_stake_share,
        )
        return round(stake, 2)


def _normalize_market(market: FootballMarketQuote) -> dict[str, float]:
    implied = {selection.selection_key: 1.0 / selection.odds for selection in market.selections}
    total = sum(implied.values())
    return {key: value / total for key, value in implied.items()}

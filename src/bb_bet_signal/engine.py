from __future__ import annotations

import math
from datetime import UTC, datetime

from .config import EngineConfig
from .models import Market, MarketSnapshot, Recommendation, Selection


class ProbabilityModel:
    def __init__(self, config: EngineConfig | None = None) -> None:
        self.config = config or EngineConfig()

    def evaluate(self, snapshot: MarketSnapshot) -> list[Recommendation]:
        recommendations: list[Recommendation] = []
        for market in snapshot.markets:
            probabilities = self._estimate_probabilities(snapshot, market)
            for selection, probability in zip(market.selections, probabilities, strict=True):
                rec = self._build_recommendation(snapshot, market, selection, probability)
                if rec is not None:
                    recommendations.append(rec)
        recommendations.sort(key=lambda item: (item.expected_value, item.edge), reverse=True)
        return recommendations

    def _estimate_probabilities(self, snapshot: MarketSnapshot, market: Market) -> list[float]:
        market_prior = _normalize_implied_probabilities(market.selections)
        context = snapshot.context
        rating_delta = (context.home.rating - context.away.rating) / 400
        form_delta = context.home.form - context.away.form
        time_factor = _time_factor(context.starts_at)

        if len(market.selections) == 2:
            base = 0.5 + rating_delta * 0.14 + form_delta * 0.21 * time_factor
            if market.key.startswith("totals"):
                base = 0.5 + form_delta * 0.18 * time_factor
            model_home = _sigmoid(base)
            model_probs = [model_home, 1.0 - model_home]
        else:
            draw_bias = 0.24 - abs(rating_delta) * 0.05
            home_score = math.exp(rating_delta + form_delta * 0.8)
            draw_score = math.exp(draw_bias)
            away_score = math.exp(-(rating_delta + form_delta * 0.8))
            total = home_score + draw_score + away_score
            model_probs = [home_score / total, draw_score / total, away_score / total]

        blended = [
            self.config.market_prior_weight * prior + self.config.rating_weight * estimate
            for prior, estimate in zip(market_prior, model_probs, strict=True)
        ]
        total = sum(blended)
        return [value / total for value in blended]

    def _build_recommendation(
        self,
        snapshot: MarketSnapshot,
        market: Market,
        selection: Selection,
        model_probability: float,
    ) -> Recommendation | None:
        implied_probability = 1.0 / selection.odds
        edge = model_probability - implied_probability
        expected_value = (model_probability * selection.odds) - 1.0
        if edge < self.config.min_edge or expected_value < self.config.min_expected_value:
            return None

        kelly_fraction = ((selection.odds - 1.0) * model_probability - (1.0 - model_probability)) / (selection.odds - 1.0)
        kelly_fraction = max(0.0, min(kelly_fraction, self.config.max_fractional_kelly))
        recommended_stake = min(
            self.config.bankroll * kelly_fraction,
            self.config.bankroll * self.config.max_stake_share,
        )

        event_name = f"{snapshot.context.home.name} vs {snapshot.context.away.name}"
        reasons = [
            f"edge {edge:.2%}",
            f"ev {expected_value:.2%}",
            f"model {model_probability:.2%} vs market {implied_probability:.2%}",
        ]
        return Recommendation(
            event_id=snapshot.context.event_id,
            event_name=event_name,
            sport=snapshot.context.sport,
            league=snapshot.context.league,
            market_key=market.key,
            market_name=market.name,
            selection_key=selection.key,
            selection_name=selection.name,
            odds=selection.odds,
            implied_probability=implied_probability,
            model_probability=model_probability,
            edge=edge,
            expected_value=expected_value,
            recommended_stake=round(recommended_stake, 2),
            generated_at=datetime.now(UTC),
            reasons=reasons,
        )


def _normalize_implied_probabilities(selections: list[Selection]) -> list[float]:
    implied = [1.0 / item.odds for item in selections]
    total = sum(implied)
    return [item / total for item in implied]


def _time_factor(starts_at: datetime) -> float:
    minutes_left = max((starts_at - datetime.now(UTC)).total_seconds() / 60, 1)
    return min(1.15, max(0.75, 60 / (minutes_left + 20)))


def _sigmoid(x: float) -> float:
    return 1.0 / (1.0 + math.exp(-x))

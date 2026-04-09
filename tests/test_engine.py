from __future__ import annotations

from datetime import UTC, datetime, timedelta
import unittest

from bb_bet_signal.config import EngineConfig
from bb_bet_signal.engine import ProbabilityModel
from bb_bet_signal.models import Market, MarketSnapshot, MatchContext, Selection, TeamState


def build_snapshot() -> MarketSnapshot:
    context = MatchContext(
        event_id="demo-1",
        sport="football",
        league="Premier League",
        home=TeamState(name="Home", rating=1600, form=0.74),
        away=TeamState(name="Away", rating=1490, form=0.48),
        starts_at=datetime.now(UTC) + timedelta(minutes=35),
    )
    markets = [
        Market(
            key="1x2",
            name="Match Winner",
            selections=[
                Selection(key="home", name="Home", odds=2.35),
                Selection(key="draw", name="Draw", odds=3.7),
                Selection(key="away", name="Away", odds=3.4),
            ],
        ),
        Market(
            key="totals_2_5",
            name="Total Over/Under 2.5",
            selections=[
                Selection(key="over", name="Over 2.5", odds=2.25),
                Selection(key="under", name="Under 2.5", odds=1.72),
            ],
        ),
    ]
    return MarketSnapshot(context=context, markets=markets, timestamp=datetime.now(UTC))


class ProbabilityModelTest(unittest.TestCase):
    def test_engine_returns_positive_value_recommendations(self) -> None:
        model = ProbabilityModel(EngineConfig(bankroll=5000, min_edge=0.01, min_expected_value=0.01))
        recommendations = model.evaluate(build_snapshot())
        self.assertTrue(recommendations)
        self.assertTrue(all(item.expected_value > 0 for item in recommendations))
        self.assertTrue(all(item.recommended_stake <= 250 for item in recommendations))


if __name__ == "__main__":
    unittest.main()

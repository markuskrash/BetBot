from __future__ import annotations

import unittest
from datetime import UTC, datetime

from bb_bet_signal.config import EngineConfig
from bb_bet_signal.football_api import (
    FootballEventOdds,
    FootballMarketQuote,
    FootballSelectionQuote,
)
from bb_bet_signal.football_engine import FootballConsensusEngine


def build_event() -> FootballEventOdds:
    return FootballEventOdds(
        event_id="42",
        home="Roma",
        away="Lazio",
        sport="football",
        league="Serie A",
        starts_at=datetime.now(UTC),
        status="pending",
        result_key=None,
        home_score=None,
        away_score=None,
        is_final=False,
        bookmakers={
            "Bet365": [
                FootballMarketQuote(
                    bookmaker="Bet365",
                    market_key="1x2",
                    market_name="Match Winner",
                    selections=[
                        FootballSelectionQuote("home", "Home", 2.95),
                        FootballSelectionQuote("draw", "Draw", 3.25),
                        FootballSelectionQuote("away", "Away", 2.8),
                    ],
                    updated_at=datetime.now(UTC),
                )
            ],
            "Unibet": [
                FootballMarketQuote(
                    bookmaker="Unibet",
                    market_key="1x2",
                    market_name="Match Winner",
                    selections=[
                        FootballSelectionQuote("home", "Home", 2.42),
                        FootballSelectionQuote("draw", "Draw", 3.1),
                        FootballSelectionQuote("away", "Away", 2.75),
                    ],
                    updated_at=datetime.now(UTC),
                )
            ],
            "Pinnacle": [
                FootballMarketQuote(
                    bookmaker="Pinnacle",
                    market_key="1x2",
                    market_name="Match Winner",
                    selections=[
                        FootballSelectionQuote("home", "Home", 2.38),
                        FootballSelectionQuote("draw", "Draw", 3.05),
                        FootballSelectionQuote("away", "Away", 2.72),
                    ],
                    updated_at=datetime.now(UTC),
                )
            ],
        },
    )


class FootballConsensusEngineTest(unittest.TestCase):
    def test_detects_value_against_market_consensus(self) -> None:
        engine = FootballConsensusEngine(EngineConfig(bankroll=5000, min_edge=0.01, min_expected_value=0.01))
        recommendations = engine.evaluate(build_event(), "Bet365")
        self.assertTrue(recommendations)
        self.assertEqual(recommendations[0].bookmaker, "Bet365")
        self.assertGreater(recommendations[0].expected_value, 0)


if __name__ == "__main__":
    unittest.main()

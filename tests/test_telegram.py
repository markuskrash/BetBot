from __future__ import annotations

import unittest
from datetime import UTC, datetime

from bb_bet_signal.models import Recommendation
from bb_bet_signal.telegram import format_recommendation


class TelegramFormattingTest(unittest.TestCase):
    def test_format_recommendation_contains_main_fields(self) -> None:
        recommendation = Recommendation(
            event_id="1",
            event_name="Roma vs Lazio",
            sport="football",
            league="Serie A",
            market_key="1x2",
            market_name="Match Winner",
            selection_key="home",
            selection_name="Roma",
            odds=2.55,
            implied_probability=0.39,
            model_probability=0.44,
            edge=0.05,
            expected_value=0.12,
            recommended_stake=150.0,
            generated_at=datetime.now(UTC),
            bookmaker="Bet365",
        )
        text = format_recommendation(recommendation)
        self.assertIn("Roma vs Lazio", text)
        self.assertIn("Bet365", text)
        self.assertIn("Edge: 5.00%", text)


if __name__ == "__main__":
    unittest.main()

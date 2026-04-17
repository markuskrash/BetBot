from __future__ import annotations

import unittest
from dataclasses import replace
from datetime import UTC, datetime

from bb_bet_signal.models import ExpressLeg, ExpressRecommendation, Recommendation
from bb_bet_signal.telegram import TelegramNotifier, format_express_recommendation, format_recommendation


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
        self.assertIn("Tier:", text)
        self.assertIn("Priority:", text)

    def test_format_express_contains_main_fields(self) -> None:
        express = ExpressRecommendation(
            express_id="e1+e2",
            legs=[
                ExpressLeg(
                    event_id="e1",
                    event_name="Roma vs Lazio",
                    market_name="Match Winner",
                    selection_key="home",
                    selection_name="Roma",
                    odds=2.1,
                    model_probability=0.52,
                ),
                ExpressLeg(
                    event_id="e2",
                    event_name="Milan vs Juve",
                    market_name="Match Winner",
                    selection_key="home",
                    selection_name="Milan",
                    odds=1.95,
                    model_probability=0.57,
                ),
            ],
            total_odds=4.10,
            implied_probability=0.24,
            model_probability=0.29,
            edge=0.05,
            expected_value=0.18,
            recommended_stake=25.0,
            generated_at=datetime.now(UTC),
        )
        text = format_express_recommendation(express)
        self.assertIn("Express (2 legs)", text)
        self.assertIn("Total odds: 4.10", text)
        self.assertIn("EV: 18.00%", text)

    def test_realert_requires_material_improvement(self) -> None:
        notifier = _DummyNotifier(token="token", chat_id="chat")
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
            tier="A",
            priority_score=1.2,
            minutes_to_start=90,
            price_advantage=0.03,
        )
        sent_first = notifier.notify_recommendations([recommendation], limit=3)
        self.assertEqual(sent_first, 1)

        stale = replace(recommendation, odds=2.56, expected_value=0.125)
        sent_stale = notifier.notify_recommendations([stale], limit=3)
        self.assertEqual(sent_stale, 0)

        improved = replace(recommendation, odds=2.60, expected_value=0.135)
        sent_improved = notifier.notify_recommendations([improved], limit=3)
        self.assertEqual(sent_improved, 1)


class _DummyNotifier(TelegramNotifier):
    def __init__(self, token: str, chat_id: str) -> None:
        super().__init__(token=token, chat_id=chat_id)
        self.messages: list[str] = []

    def send_message(self, text: str) -> None:  # type: ignore[override]
        self.messages.append(text)


if __name__ == "__main__":
    unittest.main()

from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass, field
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from .models import ExpressRecommendation, Recommendation


logger = logging.getLogger(__name__)


@dataclass(slots=True)
class TelegramNotifier:
    token: str
    chat_id: str
    disable_notification: bool = False
    realert_odds_delta: float = 0.03
    realert_ev_delta: float = 0.01
    _seen_express: set[str] = field(default_factory=set)
    _last_recommendation_state: dict[str, tuple[float, float]] = field(default_factory=dict)

    @classmethod
    def from_env(
        cls,
        *,
        realert_odds_delta: float = 0.03,
        realert_ev_delta: float = 0.01,
    ) -> TelegramNotifier | None:
        token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
        chat_id = os.getenv("TELEGRAM_CHAT_ID", "").strip()
        if not token or not chat_id:
            return None
        return cls(
            token=token,
            chat_id=chat_id,
            realert_odds_delta=realert_odds_delta,
            realert_ev_delta=realert_ev_delta,
        )

    def notify_recommendations(self, recommendations: list[Recommendation], limit: int = 3) -> int:
        sent = 0
        actionable = [
            item
            for item in recommendations
            if item.tier in {"A", "B"} and not item.blocked_by_risk
        ]
        actionable.sort(
            key=lambda item: (item.priority_score, item.expected_value, item.edge),
            reverse=True,
        )
        for recommendation in actionable[:limit]:
            signature = _signature_recommendation(recommendation)
            previous = self._last_recommendation_state.get(signature)
            if previous is not None:
                previous_odds, previous_ev = previous
                improved_odds = (recommendation.odds - previous_odds) >= self.realert_odds_delta
                improved_ev = (recommendation.expected_value - previous_ev) >= self.realert_ev_delta
                if not (improved_odds or improved_ev):
                    logger.debug("Skipping unchanged Telegram signal signature=%s", signature)
                    continue
            self.send_message(format_recommendation(recommendation))
            self._last_recommendation_state[signature] = (recommendation.odds, recommendation.expected_value)
            sent += 1
        return sent

    def notify_expresses(self, expresses: list[ExpressRecommendation], limit: int = 2) -> int:
        sent = 0
        for express in expresses[:limit]:
            signature = _signature_express(express)
            if signature in self._seen_express:
                logger.debug("Skipping duplicate Telegram express signature=%s", signature)
                continue
            self.send_message(format_express_recommendation(express))
            self._seen_express.add(signature)
            sent += 1
        return sent

    def send_message(self, text: str) -> None:
        payload = urlencode(
            {
                "chat_id": self.chat_id,
                "text": text,
                "disable_notification": "true" if self.disable_notification else "false",
            }
        ).encode("utf-8")
        request = Request(
            url=f"https://api.telegram.org/bot{self.token}/sendMessage",
            data=payload,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            method="POST",
        )
        with urlopen(request, timeout=20) as response:
            body = json.loads(response.read().decode("utf-8"))
        if not body.get("ok"):
            raise RuntimeError(f"Telegram API error: {body}")
        logger.info("Telegram message sent chat_id=%s message_id=%s", self.chat_id, body["result"]["message_id"])


def format_recommendation(recommendation: Recommendation) -> str:
    minutes_to_start = recommendation.minutes_to_start if recommendation.minutes_to_start is not None else -1
    return "\n".join(
        [
            f"{recommendation.event_name}",
            f"{recommendation.market_name}: {recommendation.selection_name}",
            f"Bookmaker: {recommendation.bookmaker or 'n/a'}",
            f"Tier: {recommendation.tier} | Priority: {recommendation.priority_score:.3f}",
            f"Odds: {recommendation.odds:.2f}",
            f"Edge: {recommendation.edge:.2%}",
            f"EV: {recommendation.expected_value:.2%}",
            f"Price advantage: {recommendation.price_advantage:.2%}",
            f"Minutes to start: {minutes_to_start}",
            f"Stake: {recommendation.recommended_stake:.2f}",
        ]
    )


def format_express_recommendation(express: ExpressRecommendation) -> str:
    legs_line = " + ".join(
        f"{leg.event_name}: {leg.selection_name} ({leg.odds:.2f})"
        for leg in express.legs
    )
    return "\n".join(
        [
            f"Express ({len(express.legs)} legs)",
            legs_line,
            f"Total odds: {express.total_odds:.2f}",
            f"Model prob: {express.model_probability:.2%}",
            f"Edge: {express.edge:.2%}",
            f"EV: {express.expected_value:.2%}",
            f"Stake: {express.recommended_stake:.2f}",
        ]
    )


def _signature_recommendation(recommendation: Recommendation) -> str:
    return "|".join(
        [
            recommendation.event_id,
            recommendation.market_key,
            recommendation.selection_key,
            recommendation.bookmaker,
        ]
    )


def _signature_express(express: ExpressRecommendation) -> str:
    legs_signature = ",".join(
        f"{leg.event_id}:{leg.selection_key}:{leg.odds:.2f}"
        for leg in express.legs
    )
    return "|".join([express.express_id, legs_signature, f"{express.total_odds:.2f}"])

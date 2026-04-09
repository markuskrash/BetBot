from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass, field
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from .models import Recommendation


logger = logging.getLogger(__name__)


@dataclass(slots=True)
class TelegramNotifier:
    token: str
    chat_id: str
    disable_notification: bool = False
    _seen: set[str] = field(default_factory=set)

    @classmethod
    def from_env(cls) -> TelegramNotifier | None:
        token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
        chat_id = os.getenv("TELEGRAM_CHAT_ID", "").strip()
        if not token or not chat_id:
            return None
        return cls(token=token, chat_id=chat_id)

    def notify_recommendations(self, recommendations: list[Recommendation], limit: int = 5) -> int:
        sent = 0
        for recommendation in recommendations[:limit]:
            signature = _signature(recommendation)
            if signature in self._seen:
                logger.debug("Skipping duplicate Telegram signal signature=%s", signature)
                continue
            self.send_message(format_recommendation(recommendation))
            self._seen.add(signature)
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
    return "\n".join(
        [
            f"{recommendation.event_name}",
            f"{recommendation.market_name}: {recommendation.selection_name}",
            f"Bookmaker: {recommendation.bookmaker or 'n/a'}",
            f"Odds: {recommendation.odds:.2f}",
            f"Edge: {recommendation.edge:.2%}",
            f"EV: {recommendation.expected_value:.2%}",
            f"Stake: {recommendation.recommended_stake:.2f}",
        ]
    )


def _signature(recommendation: Recommendation) -> str:
    return "|".join(
        [
            recommendation.event_id,
            recommendation.market_key,
            recommendation.selection_key,
            recommendation.bookmaker,
            f"{recommendation.odds:.2f}",
        ]
    )

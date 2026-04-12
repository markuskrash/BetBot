from __future__ import annotations

import math
from datetime import UTC, datetime
from statistics import StatisticsError, mean, stdev

from .models import MoexCandle, MoexEvent, MoexQuote, MoexSignal


class MoexSignalEngine:
    def __init__(
        self,
        *,
        buy_threshold: float = 0.25,
        sell_threshold: float = -0.25,
        max_position_share: float = 0.2,
        target_daily_volatility: float = 0.02,
    ) -> None:
        self.buy_threshold = buy_threshold
        self.sell_threshold = sell_threshold
        self.max_position_share = max_position_share
        self.target_daily_volatility = target_daily_volatility

    def build_signal(
        self,
        symbol: str,
        quote: MoexQuote,
        candles: list[MoexCandle],
        events: list[MoexEvent],
    ) -> MoexSignal:
        closes = [item.close for item in candles]
        volumes = [item.volume for item in candles]
        technical_score, technical_reasons = _technical_score(closes, volumes)
        event_score, event_reasons = _event_score(events)
        combined = technical_score * 0.7 + event_score * 0.3
        action = _action(combined, self.buy_threshold, self.sell_threshold)
        confidence = min(0.95, 0.5 + abs(combined) * 0.65)
        expected_move_pct = _clamp((combined * 0.04), -0.08, 0.08)

        daily_vol = _daily_volatility(closes)
        risk_scale = min(1.0, self.target_daily_volatility / max(daily_vol, 1e-6))
        base_share = confidence * 0.12 * risk_scale
        if action == "BUY":
            position_share = min(self.max_position_share, base_share)
        elif action == "SELL":
            position_share = min(self.max_position_share, base_share * 0.7)
        else:
            position_share = 0.0

        reasons = technical_reasons + event_reasons
        if not reasons:
            reasons = ["No strong technical or event deviations."]

        return MoexSignal(
            symbol=symbol,
            action=action,
            score=combined,
            confidence=confidence,
            last_price=quote.last,
            expected_move_pct=expected_move_pct,
            position_share=position_share,
            technical_score=technical_score,
            event_score=event_score,
            event_count=len(events),
            generated_at=datetime.now(UTC),
            reasons=reasons[:5],
        )


def sentiment_score(text: str) -> float:
    normalized = text.lower()
    positive_terms = {
        "дивиденд": 0.28,
        "байбэк": 0.35,
        "рост": 0.12,
        "рекорд": 0.15,
        "прибыл": 0.14,
        "возобновляет торги": 0.12,
        "сотруднич": 0.08,
        "допуск к торгам": 0.09,
    }
    negative_terms = {
        "санкц": -0.3,
        "убыт": -0.22,
        "снижен": -0.13,
        "приостанов": -0.18,
        "делистинг": -0.35,
        "дестабилизации цен": -0.45,
        "ограничени": -0.15,
        "дефолт": -0.5,
    }
    score = 0.0
    for term, weight in positive_terms.items():
        if term in normalized:
            score += weight
    for term, weight in negative_terms.items():
        if term in normalized:
            score += weight
    return _clamp(score, -1.0, 1.0)


def _technical_score(closes: list[float], volumes: list[float]) -> tuple[float, list[str]]:
    if len(closes) < 55:
        return 0.0, ["Insufficient candle history for stable trend signal."]

    latest = closes[-1]
    sma20 = mean(closes[-20:])
    sma50 = mean(closes[-50:])
    momentum5 = latest / closes[-6] - 1.0
    momentum20 = latest / closes[-21] - 1.0
    volume_ratio = (volumes[-1] / mean(volumes[-20:])) if mean(volumes[-20:]) > 0 else 1.0

    score = 0.0
    reasons: list[str] = []
    if latest > sma20:
        score += 0.23
        reasons.append("Price is above SMA20.")
    else:
        score -= 0.23
        reasons.append("Price is below SMA20.")
    if sma20 > sma50:
        score += 0.18
        reasons.append("SMA20 is above SMA50.")
    else:
        score -= 0.18
        reasons.append("SMA20 is below SMA50.")
    score += _clamp(momentum5 * 8, -0.2, 0.2)
    score += _clamp(momentum20 * 5, -0.25, 0.25)
    if volume_ratio > 1.2:
        score += 0.08
        reasons.append("Volume supports the move.")
    elif volume_ratio < 0.8:
        score -= 0.06
        reasons.append("Move has weak volume support.")

    return _clamp(score, -1.0, 1.0), reasons


def _event_score(events: list[MoexEvent]) -> tuple[float, list[str]]:
    if not events:
        return 0.0, []
    now = datetime.now(UTC)
    weighted_scores: list[float] = []
    reasons: list[str] = []
    for event in events:
        age_hours = max((now - event.published_at).total_seconds() / 3600, 0)
        decay = math.exp(-age_hours / 36)
        weighted = event.sentiment_score * decay
        weighted_scores.append(weighted)
        if abs(weighted) >= 0.08:
            direction = "positive" if weighted > 0 else "negative"
            reasons.append(f"Recent {direction} event: {event.title[:80]}")
    score = _clamp(sum(weighted_scores), -1.0, 1.0)
    return score, reasons


def _action(score: float, buy_threshold: float, sell_threshold: float) -> str:
    if score >= buy_threshold:
        return "BUY"
    if score <= sell_threshold:
        return "SELL"
    return "HOLD"


def _daily_volatility(closes: list[float]) -> float:
    if len(closes) < 25:
        return 0.02
    returns = [(closes[i] / closes[i - 1]) - 1.0 for i in range(1, len(closes)) if closes[i - 1] > 0]
    if len(returns) < 10:
        return 0.02
    try:
        return stdev(returns)
    except StatisticsError:
        return 0.02


def _clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))

from __future__ import annotations

import asyncio
import logging
import re
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from statistics import StatisticsError, mean, stdev

from .models import LongTermSignal, MoexCandle, MoexEvent, MoexQuote
from .moex_api import MoexApiClient
from .moex_engine import sentiment_score
from .storage import MoexSignalRepository
from .telegram import TelegramNotifier


logger = logging.getLogger(__name__)


@dataclass(slots=True)
class LongTermProfileConfig:
    name: str
    horizon_days_min: int
    horizon_days_max: int
    buy_threshold: float
    sell_threshold: float
    technical_weight: float
    event_weight: float
    max_position_share: float
    confirmation_target: int
    decay_hours: float


PROFILES: dict[str, LongTermProfileConfig] = {
    "swing": LongTermProfileConfig(
        name="swing",
        horizon_days_min=14,
        horizon_days_max=56,
        buy_threshold=0.20,
        sell_threshold=-0.20,
        technical_weight=0.62,
        event_weight=0.38,
        max_position_share=0.12,
        confirmation_target=2,
        decay_hours=60.0,
    ),
    "position": LongTermProfileConfig(
        name="position",
        horizon_days_min=60,
        horizon_days_max=180,
        buy_threshold=0.28,
        sell_threshold=-0.28,
        technical_weight=0.74,
        event_weight=0.26,
        max_position_share=0.18,
        confirmation_target=3,
        decay_hours=168.0,
    ),
}


class LongTermMoexEngine:
    def build_signal(
        self,
        profile: str,
        symbol: str,
        quote: MoexQuote,
        candles: list[MoexCandle],
        events: list[MoexEvent],
        *,
        confirmation_count: int,
        previous_action: str | None = None,
        previous_score: float | None = None,
    ) -> LongTermSignal:
        cfg = PROFILES[profile]
        closes = [item.close for item in candles]
        volumes = [item.volume for item in candles]
        technical_score, technical_reasons = _longterm_technical_score(closes, volumes, profile)
        event_score, event_reasons = _longterm_event_score(events, cfg.decay_hours)
        combined = _clamp(
            technical_score * cfg.technical_weight + event_score * cfg.event_weight,
            -1.0,
            1.0,
        )
        action = _action(combined, cfg.buy_threshold, cfg.sell_threshold)
        confidence = min(0.95, 0.45 + abs(combined) * 0.75)
        action, churn_note = _apply_churn_guard(
            action,
            combined,
            previous_action=previous_action,
            previous_score=previous_score,
        )
        if churn_note is not None:
            event_reasons.append(churn_note)

        daily_vol = _daily_volatility(closes)
        base_share = confidence * (0.10 if profile == "swing" else 0.12)
        risk_scale = min(1.0, 0.03 / max(daily_vol, 1e-6))
        if action in {"BUY", "SELL"}:
            position_share = min(cfg.max_position_share, base_share * risk_scale)
        else:
            position_share = 0.0
        stop_loss, take_profit = _longterm_risk_levels(
            action=action,
            last_price=quote.last,
            daily_vol=daily_vol,
            profile=profile,
        )
        reasons = (technical_reasons + event_reasons)[:6]
        if not reasons:
            reasons = ["No strong long-term divergence detected."]
        holding_stage = "entry" if action in {"BUY", "SELL"} else "watch"
        return LongTermSignal(
            symbol=symbol,
            profile=profile,
            action=action,
            score=combined,
            confidence=confidence,
            last_price=quote.last,
            horizon_days_min=cfg.horizon_days_min,
            horizon_days_max=cfg.horizon_days_max,
            position_share=position_share,
            technical_score=technical_score,
            event_score=event_score,
            event_count=len(events),
            confirmation_count=confirmation_count,
            holding_stage=holding_stage,
            generated_at=datetime.now(UTC),
            stop_loss=stop_loss,
            take_profit=take_profit,
            reasons=reasons,
        )


class MoexLongTermService:
    def __init__(
        self,
        client: MoexApiClient,
        engine: LongTermMoexEngine,
        *,
        symbols: list[str],
        profiles: list[str],
        repository: MoexSignalRepository | None = None,
        notifier: TelegramNotifier | None = None,
        poll_seconds: int = 86400,
        history_days: int = 365,
        news_limit: int = 250,
        news_window_hours: int = 336,
        max_open_positions: int = 5,
    ) -> None:
        self.client = client
        self.engine = engine
        self.symbols = [item.upper() for item in symbols]
        self.profiles = [item for item in profiles if item in PROFILES]
        self.repository = repository
        self.notifier = notifier
        self.poll_seconds = poll_seconds
        self.history_days = history_days
        self.news_limit = news_limit
        self.news_window_hours = news_window_hours
        self.max_open_positions = max_open_positions
        self._latest: list[LongTermSignal] = []
        self._news_body_cache: dict[int, str] = {}
        self._lock = asyncio.Lock()

    async def poll_once(self) -> list[LongTermSignal]:
        logger.info(
            "Polling MOEX long-term symbols=%s profiles=%s",
            ",".join(self.symbols),
            ",".join(self.profiles),
        )
        events_by_symbol = self._fetch_events()
        until = date.today()
        since = until - timedelta(days=self.history_days)
        candidates: list[LongTermSignal] = []
        for symbol in self.symbols:
            try:
                quote = self.client.get_quote(symbol)
                if quote is None:
                    continue
                candles = self.client.get_daily_candles(symbol, from_date=since, till_date=until)
                symbol_events = events_by_symbol.get(symbol, [])
                for profile in self.profiles:
                    previous = self.repository.get_longterm_notification_state(symbol, profile) if self.repository else None
                    previous_action = str(previous.get("action")) if previous else None
                    previous_score = float(previous.get("score")) if previous and previous.get("score") is not None else None
                    confirmation_count = _next_confirmation(previous, profile, previous_action_candidate=None)
                    signal = self.engine.build_signal(
                        profile,
                        symbol,
                        quote,
                        candles,
                        symbol_events,
                        confirmation_count=confirmation_count,
                        previous_action=previous_action,
                        previous_score=previous_score,
                    )
                    next_confirmation = _next_confirmation(previous, profile, signal.action)
                    if (
                        previous is None
                        and signal.action in {"BUY", "SELL"}
                        and abs(signal.score) >= (0.45 if profile == "swing" else 0.55)
                    ):
                        next_confirmation = PROFILES[profile].confirmation_target
                    signal.confirmation_count = next_confirmation
                    candidates.append(signal)
            except Exception:
                logger.exception("Failed to build long-term signal for %s", symbol)

        actionable = self._select_actionable(candidates)
        if self.repository is not None:
            self.repository.persist_longterm_signals(candidates)
            self.repository.refresh_longterm_positions(actionable)
        if self.notifier is not None:
            sent = self._notify_signals(actionable)
            logger.info("MOEX long-term telegram notifications sent=%s", sent)
        async with self._lock:
            self._latest = actionable
            return list(self._latest)

    async def run_forever(self) -> None:
        while True:
            try:
                await self.poll_once()
            except Exception:
                logger.exception("MOEX long-term poll failed; retrying after backoff")
                await asyncio.sleep(min(1800, max(300, self.poll_seconds // 4)))
                continue
            logger.info("MOEX long-term sleep for %s seconds", self.poll_seconds)
            await asyncio.sleep(self.poll_seconds)

    def latest(self) -> list[LongTermSignal]:
        return list(self._latest)

    def latest_longterm(self) -> list[LongTermSignal]:
        return list(self._latest)

    def longterm_performance(self) -> dict:
        if self.repository is None:
            return {}
        return self.repository.longterm_performance_snapshot()

    def _fetch_events(self) -> dict[str, list[MoexEvent]]:
        raw_news = self.client.get_sitenews(limit=self.news_limit)
        threshold = datetime.now(UTC) - timedelta(hours=self.news_window_hours)
        symbol_patterns = {
            symbol: re.compile(rf"(?:\b{re.escape(symbol)}\b|\({re.escape(symbol)}\))", re.IGNORECASE)
            for symbol in self.symbols
        }
        events_by_symbol = {symbol: [] for symbol in self.symbols}
        for item in raw_news:
            news_id = int(item.get("id") or 0)
            if not news_id:
                continue
            title = str(item.get("title") or "")
            published_raw = str(item.get("published_at") or "")
            if not published_raw:
                continue
            published_at = datetime.strptime(published_raw, "%Y-%m-%d %H:%M:%S").replace(tzinfo=UTC)
            if published_at < threshold:
                continue
            mentions = [symbol for symbol, pattern in symbol_patterns.items() if pattern.search(title)]
            if not mentions:
                continue
            body = self._news_body_cache.get(news_id)
            if body is None:
                body = self.client.get_sitenews_content(news_id)
                self._news_body_cache[news_id] = body
            event = MoexEvent(
                news_id=news_id,
                published_at=published_at,
                title=title,
                body=body,
                symbols=mentions,
                sentiment_score=sentiment_score(f"{title} {body}".strip()),
            )
            for symbol in mentions:
                events_by_symbol[symbol].append(event)
        for symbol in self.symbols:
            events_by_symbol[symbol].sort(key=lambda item: item.published_at, reverse=True)
        return events_by_symbol

    def _select_actionable(self, candidates: list[LongTermSignal]) -> list[LongTermSignal]:
        actionable = [
            item
            for item in candidates
            if item.action in {"BUY", "SELL"} and item.confirmation_count >= PROFILES[item.profile].confirmation_target
        ]
        actionable.sort(key=lambda item: (item.confidence, abs(item.score)), reverse=True)
        return actionable[: self.max_open_positions]

    def _notify_signals(self, signals: list[LongTermSignal]) -> int:
        assert self.notifier is not None
        sent = 0
        for signal in signals:
            previous = self.repository.get_longterm_notification_state(signal.symbol, signal.profile) if self.repository else None
            if previous is not None:
                prev_action = str(previous.get("action") or "")
                prev_score = float(previous.get("score") or 0.0)
                if prev_action == signal.action and abs(signal.score - prev_score) < 0.10:
                    continue
            self.notifier.send_message(_format_longterm_signal(signal))
            if self.repository is not None:
                self.repository.upsert_longterm_notification_state(signal)
            sent += 1
        return sent


def _next_confirmation(
    previous_state: dict[str, float | str | None] | None,
    profile: str,
    previous_action_candidate: str | None,
) -> int:
    if previous_action_candidate is None:
        return 1
    target = PROFILES[profile].confirmation_target
    if previous_state is None:
        return 1
    previous_action = str(previous_state.get("action") or "")
    previous_count = int(float(previous_state.get("confirmation_count") or 0))
    if previous_action != previous_action_candidate:
        return 1
    return min(target + 2, previous_count + 1)


def _format_longterm_signal(signal: LongTermSignal) -> str:
    stop_line = f"Stop: {signal.stop_loss:.2f}" if signal.stop_loss is not None else "Stop: n/a"
    take_line = f"Take: {signal.take_profit:.2f}" if signal.take_profit is not None else "Take: n/a"
    return "\n".join(
        [
            f"MOEX LONG {signal.symbol} [{signal.profile}] {signal.action}",
            f"Horizon: {signal.horizon_days_min}-{signal.horizon_days_max} days",
            f"Price: {signal.last_price:.2f} RUB",
            f"Score: {signal.score:.2f} (tech {signal.technical_score:.2f}, events {signal.event_score:.2f})",
            f"Confidence: {signal.confidence:.0%}",
            f"Confirmation: {signal.confirmation_count}/{PROFILES[signal.profile].confirmation_target}",
            f"Position: {signal.position_share:.2%}",
            stop_line,
            take_line,
        ]
    )


def _longterm_technical_score(closes: list[float], volumes: list[float], profile: str) -> tuple[float, list[str]]:
    min_len = 130 if profile == "position" else 90
    if len(closes) < min_len:
        return 0.0, ["Insufficient long-term candle history."]
    latest = closes[-1]
    sma50 = mean(closes[-50:])
    sma120 = mean(closes[-120:])
    momentum20 = latest / closes[-21] - 1.0
    momentum60 = latest / closes[-61] - 1.0
    volume_ratio = (volumes[-1] / mean(volumes[-60:])) if mean(volumes[-60:]) > 0 else 1.0
    score = 0.0
    reasons: list[str] = []
    if latest > sma50:
        score += 0.18
        reasons.append("Price above SMA50.")
    else:
        score -= 0.18
        reasons.append("Price below SMA50.")
    if sma50 > sma120:
        score += 0.22
        reasons.append("SMA50 above SMA120.")
    else:
        score -= 0.22
        reasons.append("SMA50 below SMA120.")
    score += _clamp(momentum20 * 5.5, -0.22, 0.22)
    score += _clamp(momentum60 * 4.5, -0.25, 0.25)
    if volume_ratio > 1.15:
        score += 0.06
        reasons.append("Volume confirms trend.")
    elif volume_ratio < 0.85:
        score -= 0.05
        reasons.append("Volume weak for trend.")
    if profile == "position":
        score *= 1.05
    return _clamp(score, -1.0, 1.0), reasons


def _longterm_event_score(events: list[MoexEvent], decay_hours: float) -> tuple[float, list[str]]:
    if not events:
        return 0.0, []
    now = datetime.now(UTC)
    weighted_scores: list[float] = []
    reasons: list[str] = []
    for item in events[:30]:
        age_hours = max((now - item.published_at).total_seconds() / 3600, 0.0)
        decay = pow(2.718281828, -age_hours / max(decay_hours, 1.0))
        weighted = item.sentiment_score * decay
        weighted_scores.append(weighted)
        if abs(weighted) >= 0.08:
            direction = "positive" if weighted > 0 else "negative"
            reasons.append(f"{direction.capitalize()} event: {item.title[:72]}")
    score = _clamp(sum(weighted_scores), -1.0, 1.0)
    return score, reasons


def _action(score: float, buy_threshold: float, sell_threshold: float) -> str:
    if score >= buy_threshold:
        return "BUY"
    if score <= sell_threshold:
        return "SELL"
    return "HOLD"


def _apply_churn_guard(
    action: str,
    score: float,
    *,
    previous_action: str | None,
    previous_score: float | None,
) -> tuple[str, str | None]:
    if previous_action not in {"BUY", "SELL"} or action not in {"BUY", "SELL"}:
        return action, None
    if previous_action == action:
        return action, None
    if previous_score is None:
        return action, None
    if abs(score - previous_score) < 0.15:
        return "HOLD", "Churn guard: weak reversal."
    return action, None


def _daily_volatility(closes: list[float]) -> float:
    if len(closes) < 40:
        return 0.02
    returns = [(closes[index] / closes[index - 1]) - 1.0 for index in range(1, len(closes)) if closes[index - 1] > 0]
    if len(returns) < 20:
        return 0.02
    try:
        return stdev(returns)
    except StatisticsError:
        return 0.02


def _longterm_risk_levels(
    *,
    action: str,
    last_price: float,
    daily_vol: float,
    profile: str,
) -> tuple[float | None, float | None]:
    if action not in {"BUY", "SELL"} or last_price <= 0:
        return None, None
    multiplier = 2.8 if profile == "position" else 2.2
    base_stop = _clamp(daily_vol * multiplier, 0.03, 0.14)
    take_multiplier = 2.4 if profile == "position" else 2.0
    take_pct = _clamp(base_stop * take_multiplier, 0.06, 0.30)
    if action == "BUY":
        stop_loss = last_price * (1.0 - base_stop)
        take_profit = last_price * (1.0 + take_pct)
    else:
        stop_loss = last_price * (1.0 + base_stop)
        take_profit = last_price * (1.0 - take_pct)
    return round(stop_loss, 2), round(take_profit, 2)


def _clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))

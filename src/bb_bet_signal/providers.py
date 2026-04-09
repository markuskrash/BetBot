from __future__ import annotations

import abc
import asyncio
import json
import math
import random
from datetime import UTC, datetime, timedelta
from pathlib import Path

from .config import FeedConfig
from .models import Market, MarketSnapshot, MatchContext, Selection, TeamState


class RealtimeFeed(abc.ABC):
    @abc.abstractmethod
    async def stream(self) -> abc.AsyncIterator[MarketSnapshot]:
        """Yield live market snapshots."""


class DemoRealtimeFeed(RealtimeFeed):
    def __init__(self, config: FeedConfig | None = None, seed: int = 7) -> None:
        self.config = config or FeedConfig()
        self.random = random.Random(seed)
        now = datetime.now(UTC)
        self.matches = [
            MatchContext(
                event_id="cs2-001",
                sport="esports",
                league="CS2 Major",
                home=TeamState(name="Aurora", rating=1610, form=0.72),
                away=TeamState(name="Spirit", rating=1695, form=0.81),
                starts_at=now + timedelta(minutes=18),
            ),
            MatchContext(
                event_id="fb-002",
                sport="football",
                league="Premier League",
                home=TeamState(name="Newcastle", rating=1540, form=0.66),
                away=TeamState(name="Brighton", rating=1502, form=0.61),
                starts_at=now + timedelta(minutes=44),
            ),
        ]
        self.tick = 0

    async def stream(self):
        while True:
            self.tick += 1
            for context in self.matches:
                yield self._build_snapshot(context)
            await asyncio.sleep(self.config.tick_interval_seconds)

    def _build_snapshot(self, context: MatchContext) -> MarketSnapshot:
        phase = self.tick / 4
        rating_delta = context.home.rating - context.away.rating
        form_delta = context.home.form - context.away.form
        home_strength = (rating_delta / 400) + form_delta * 1.2

        moneyline_shift = math.sin(phase + len(context.event_id)) * self.config.jitter
        total_shift = math.cos(phase + len(context.league)) * self.config.jitter

        home_prob = _clamp(0.48 + home_strength * 0.09 + moneyline_shift, 0.12, 0.82)
        if context.sport == "football":
            draw_prob = _clamp(0.24 - abs(home_strength) * 0.04 + self.random.uniform(-0.03, 0.03), 0.12, 0.32)
            away_prob = _clamp(1.0 - home_prob - draw_prob, 0.1, 0.72)
            total = home_prob + draw_prob + away_prob
            probs = [home_prob / total, draw_prob / total, away_prob / total]
            moneyline = Market(
                key="1x2",
                name="Match Winner",
                selections=[
                    Selection(key="home", name=context.home.name, odds=_price_from_probability(probs[0], 1.07)),
                    Selection(key="draw", name="Draw", odds=_price_from_probability(probs[1], 1.07)),
                    Selection(key="away", name=context.away.name, odds=_price_from_probability(probs[2], 1.07)),
                ],
            )
        else:
            away_prob = 1.0 - home_prob
            moneyline = Market(
                key="moneyline",
                name="Match Winner",
                selections=[
                    Selection(key="home", name=context.home.name, odds=_price_from_probability(home_prob, 1.05)),
                    Selection(key="away", name=context.away.name, odds=_price_from_probability(away_prob, 1.05)),
                ],
            )

        base_total = 2.5 if context.sport == "football" else 2.5
        over_prob = _clamp(0.51 + form_delta * 0.08 + total_shift, 0.18, 0.82)
        totals = Market(
            key="totals_2_5",
            name=f"Total Over/Under {base_total}",
            selections=[
                Selection(key="over", name=f"Over {base_total}", odds=_price_from_probability(over_prob, 1.06)),
                Selection(key="under", name=f"Under {base_total}", odds=_price_from_probability(1.0 - over_prob, 1.06)),
            ],
        )

        return MarketSnapshot(
            context=context,
            markets=[moneyline, totals],
            timestamp=datetime.now(UTC),
        )


class JsonlRealtimeFeed(RealtimeFeed):
    def __init__(self, path: str | Path, config: FeedConfig | None = None) -> None:
        self.path = Path(path)
        self.config = config or FeedConfig()

    async def stream(self):
        while True:
            if not self.path.exists():
                await asyncio.sleep(self.config.tick_interval_seconds)
                continue
            for line in self.path.read_text(encoding="utf-8").splitlines():
                if not line.strip():
                    continue
                yield self._parse_snapshot(json.loads(line))
            await asyncio.sleep(self.config.tick_interval_seconds)

    def _parse_snapshot(self, payload: dict) -> MarketSnapshot:
        context = MatchContext(
            event_id=payload["event_id"],
            sport=payload["sport"],
            league=payload["league"],
            home=TeamState(**payload["home"]),
            away=TeamState(**payload["away"]),
            starts_at=datetime.fromisoformat(payload["starts_at"]),
        )
        markets = [
            Market(
                key=market["key"],
                name=market["name"],
                selections=[Selection(**selection) for selection in market["selections"]],
            )
            for market in payload["markets"]
        ]
        return MarketSnapshot(
            context=context,
            markets=markets,
            timestamp=datetime.fromisoformat(payload["timestamp"]),
        )


def _price_from_probability(probability: float, margin: float) -> float:
    return round(max(1.01, 1.0 / (probability * margin)), 2)


def _clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))

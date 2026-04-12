from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime
from typing import Any


@dataclass(slots=True)
class TeamState:
    name: str
    rating: float
    form: float


@dataclass(slots=True)
class Selection:
    key: str
    name: str
    odds: float


@dataclass(slots=True)
class Market:
    key: str
    name: str
    selections: list[Selection]


@dataclass(slots=True)
class MatchContext:
    event_id: str
    sport: str
    league: str
    home: TeamState
    away: TeamState
    starts_at: datetime


@dataclass(slots=True)
class MarketSnapshot:
    context: MatchContext
    markets: list[Market]
    timestamp: datetime


@dataclass(slots=True)
class Recommendation:
    event_id: str
    event_name: str
    sport: str
    league: str
    market_key: str
    market_name: str
    selection_key: str
    selection_name: str
    odds: float
    implied_probability: float
    model_probability: float
    edge: float
    expected_value: float
    recommended_stake: float
    generated_at: datetime
    bookmaker: str = ""
    reasons: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["generated_at"] = self.generated_at.isoformat()
        return payload


@dataclass(slots=True)
class MoexCandle:
    open: float
    close: float
    high: float
    low: float
    value: float
    volume: float
    begin: datetime
    end: datetime


@dataclass(slots=True)
class MoexQuote:
    symbol: str
    board: str
    last: float
    prev_price: float
    change_pct: float
    trades: int
    volume: float
    turnover: float
    updated_at: datetime


@dataclass(slots=True)
class MoexEvent:
    news_id: int
    published_at: datetime
    title: str
    body: str
    symbols: list[str]
    sentiment_score: float


@dataclass(slots=True)
class MoexSignal:
    symbol: str
    action: str
    score: float
    confidence: float
    last_price: float
    expected_move_pct: float
    position_share: float
    technical_score: float
    event_score: float
    event_count: int
    generated_at: datetime
    stop_loss: float | None = None
    take_profit: float | None = None
    reasons: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["generated_at"] = self.generated_at.isoformat()
        return payload

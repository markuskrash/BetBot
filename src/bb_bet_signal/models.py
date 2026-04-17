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
    starts_at: datetime | None = None
    blocked_by_risk: bool = False
    bookmaker: str = ""
    priority_score: float = 0.0
    tier: str = "C"
    minutes_to_start: int | None = None
    price_advantage: float = 0.0
    decision_tags: list[str] = field(default_factory=list)
    reasons: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["generated_at"] = self.generated_at.isoformat()
        if self.starts_at is not None:
            payload["starts_at"] = self.starts_at.isoformat()
        return payload


@dataclass(slots=True)
class ExpressLeg:
    event_id: str
    event_name: str
    market_name: str
    selection_key: str
    selection_name: str
    odds: float
    model_probability: float


@dataclass(slots=True)
class ExpressRecommendation:
    express_id: str
    legs: list[ExpressLeg]
    total_odds: float
    implied_probability: float
    model_probability: float
    edge: float
    expected_value: float
    recommended_stake: float
    generated_at: datetime
    blocked_by_risk: bool = False
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

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

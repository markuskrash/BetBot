from __future__ import annotations

from dataclasses import dataclass


@dataclass(slots=True)
class EngineConfig:
    bankroll: float = 100.0
    min_edge: float = 0.03
    min_expected_value: float = 0.02
    max_fractional_kelly: float = 0.25
    max_stake_share: float = 0.05
    market_prior_weight: float = 0.65
    rating_weight: float = 0.35


@dataclass(slots=True)
class FeedConfig:
    tick_interval_seconds: float = 1.5
    jitter: float = 0.12

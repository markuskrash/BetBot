from __future__ import annotations

from dataclasses import dataclass

# Use these in argparse defaults — never `EngineConfig.min_edge` on a slotted dataclass
# (that is a member_descriptor, not a float).
DEFAULT_MIN_EDGE: float = 0.03
DEFAULT_MIN_EV: float = 0.02


@dataclass(slots=True)
class EngineConfig:
    bankroll: float = 100.0
    min_edge: float = DEFAULT_MIN_EDGE
    min_expected_value: float = DEFAULT_MIN_EV
    max_fractional_kelly: float = 0.25
    max_stake_share: float = 0.05
    market_prior_weight: float = 0.65
    rating_weight: float = 0.35


@dataclass(slots=True)
class FeedConfig:
    tick_interval_seconds: float = 1.5
    jitter: float = 0.12

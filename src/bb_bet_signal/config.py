from __future__ import annotations

from dataclasses import dataclass

# Use these in argparse defaults — never `EngineConfig.min_edge` on a slotted dataclass
# (that is a member_descriptor, not a float).
DEFAULT_MIN_EDGE: float = 0.035
DEFAULT_MIN_EV: float = 0.025
DEFAULT_DAILY_DD_LIMIT: float = 0.05
DEFAULT_MAX_SINGLE_SHARE: float = 0.01
DEFAULT_MAX_EXPRESS_SHARE: float = 0.004
DEFAULT_MAX_OPEN_BETS: int = 6
DEFAULT_MAX_BETS_PER_DAY: int = 20


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


@dataclass(slots=True)
class FootballRiskConfig:
    daily_drawdown_limit: float = DEFAULT_DAILY_DD_LIMIT
    max_single_share: float = DEFAULT_MAX_SINGLE_SHARE
    max_express_share: float = DEFAULT_MAX_EXPRESS_SHARE
    max_open_bets: int = DEFAULT_MAX_OPEN_BETS
    max_bets_per_day: int = DEFAULT_MAX_BETS_PER_DAY

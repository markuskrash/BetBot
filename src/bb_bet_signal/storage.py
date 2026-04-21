from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any, Iterator

from .football_api import FootballEventOdds
from .models import ExpressRecommendation, LongTermSignal, MoexSignal, Recommendation


class SnapshotRepository:
    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    @contextmanager
    def _connect(self) -> Iterator[sqlite3.Connection]:
        connection = sqlite3.connect(self.path)
        try:
            yield connection
            connection.commit()
        finally:
            connection.close()

    def _init_db(self) -> None:
        with self._connect() as connection:
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS odds_snapshots (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_id TEXT NOT NULL,
                    home TEXT NOT NULL,
                    away TEXT NOT NULL,
                    league TEXT NOT NULL,
                    market_key TEXT NOT NULL,
                    bookmaker TEXT NOT NULL,
                    selection_key TEXT NOT NULL,
                    odds REAL NOT NULL,
                    captured_at TEXT NOT NULL
                )
                """
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS football_open_bets (
                    bet_key TEXT PRIMARY KEY,
                    bet_type TEXT NOT NULL,
                    event_id TEXT NOT NULL,
                    league TEXT NOT NULL,
                    market_key TEXT NOT NULL,
                    selection_key TEXT NOT NULL,
                    odds REAL NOT NULL,
                    stake REAL NOT NULL,
                    potential_payout REAL NOT NULL,
                    model_probability REAL NOT NULL,
                    implied_probability REAL NOT NULL,
                    edge REAL NOT NULL,
                    expected_value REAL NOT NULL,
                    legs_json TEXT,
                    priority_score REAL,
                    tier TEXT,
                    price_advantage REAL,
                    minutes_to_start INTEGER,
                    decision_tags TEXT,
                    placed_at TEXT NOT NULL
                )
                """
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS football_closed_bets (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    bet_key TEXT UNIQUE NOT NULL,
                    bet_type TEXT NOT NULL,
                    event_id TEXT NOT NULL,
                    league TEXT NOT NULL,
                    market_key TEXT NOT NULL,
                    selection_key TEXT NOT NULL,
                    odds REAL NOT NULL,
                    stake REAL NOT NULL,
                    payout REAL NOT NULL,
                    pnl REAL NOT NULL,
                    outcome TEXT NOT NULL,
                    model_probability REAL NOT NULL,
                    implied_probability REAL NOT NULL,
                    edge REAL NOT NULL,
                    expected_value REAL NOT NULL,
                    priority_score REAL,
                    tier TEXT,
                    price_advantage REAL,
                    minutes_to_start INTEGER,
                    decision_tags TEXT,
                    placed_at TEXT NOT NULL,
                    settled_at TEXT NOT NULL
                )
                """
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS football_event_results (
                    event_id TEXT PRIMARY KEY,
                    result_key TEXT,
                    status TEXT NOT NULL,
                    home_score REAL,
                    away_score REAL,
                    is_final INTEGER NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS football_daily_stats (
                    stat_date TEXT PRIMARY KEY,
                    placed_count INTEGER NOT NULL DEFAULT 0,
                    settled_count INTEGER NOT NULL DEFAULT 0,
                    open_count INTEGER NOT NULL DEFAULT 0,
                    pnl REAL NOT NULL DEFAULT 0,
                    turnover REAL NOT NULL DEFAULT 0,
                    roi REAL NOT NULL DEFAULT 0,
                    hit_rate REAL NOT NULL DEFAULT 0,
                    clv_proxy REAL NOT NULL DEFAULT 0,
                    stop_triggered INTEGER NOT NULL DEFAULT 0,
                    updated_at TEXT NOT NULL
                )
                """
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS football_stop_status (
                    stat_date TEXT PRIMARY KEY,
                    is_blocked INTEGER NOT NULL,
                    reason TEXT NOT NULL,
                    triggered_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS football_decisions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    captured_at TEXT NOT NULL,
                    event_id TEXT NOT NULL,
                    league TEXT NOT NULL,
                    market_key TEXT NOT NULL,
                    selection_key TEXT NOT NULL,
                    bookmaker TEXT NOT NULL,
                    odds REAL NOT NULL,
                    minutes_to_start INTEGER,
                    price_advantage REAL,
                    edge REAL,
                    expected_value REAL,
                    priority_score REAL,
                    tier TEXT NOT NULL,
                    decision TEXT NOT NULL,
                    reason TEXT NOT NULL
                )
                """
            )
            open_columns = {row[1] for row in connection.execute("PRAGMA table_info(football_open_bets)")}
            if "priority_score" not in open_columns:
                connection.execute("ALTER TABLE football_open_bets ADD COLUMN priority_score REAL")
            if "tier" not in open_columns:
                connection.execute("ALTER TABLE football_open_bets ADD COLUMN tier TEXT")
            if "price_advantage" not in open_columns:
                connection.execute("ALTER TABLE football_open_bets ADD COLUMN price_advantage REAL")
            if "minutes_to_start" not in open_columns:
                connection.execute("ALTER TABLE football_open_bets ADD COLUMN minutes_to_start INTEGER")
            if "decision_tags" not in open_columns:
                connection.execute("ALTER TABLE football_open_bets ADD COLUMN decision_tags TEXT")

            closed_columns = {row[1] for row in connection.execute("PRAGMA table_info(football_closed_bets)")}
            if "priority_score" not in closed_columns:
                connection.execute("ALTER TABLE football_closed_bets ADD COLUMN priority_score REAL")
            if "tier" not in closed_columns:
                connection.execute("ALTER TABLE football_closed_bets ADD COLUMN tier TEXT")
            if "price_advantage" not in closed_columns:
                connection.execute("ALTER TABLE football_closed_bets ADD COLUMN price_advantage REAL")
            if "minutes_to_start" not in closed_columns:
                connection.execute("ALTER TABLE football_closed_bets ADD COLUMN minutes_to_start INTEGER")
            if "decision_tags" not in closed_columns:
                connection.execute("ALTER TABLE football_closed_bets ADD COLUMN decision_tags TEXT")

    def persist_event(self, event: FootballEventOdds, captured_at: datetime | None = None) -> None:
        timestamp = (captured_at or datetime.now(UTC)).isoformat()
        with self._connect() as connection:
            for bookmaker, markets in event.bookmakers.items():
                for market in markets:
                    for selection in market.selections:
                        connection.execute(
                            """
                            INSERT INTO odds_snapshots (
                                event_id, home, away, league, market_key,
                                bookmaker, selection_key, odds, captured_at
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                            """,
                            (
                                event.event_id,
                                event.home,
                                event.away,
                                event.league,
                                market.market_key,
                                bookmaker,
                                selection.selection_key,
                                selection.odds,
                                timestamp,
                            ),
                        )

    def upsert_event_result(self, event: FootballEventOdds, captured_at: datetime | None = None) -> None:
        timestamp = (captured_at or datetime.now(UTC)).isoformat()
        with self._connect() as connection:
            connection.execute(
                """
                INSERT INTO football_event_results (
                    event_id, result_key, status, home_score, away_score, is_final, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(event_id) DO UPDATE SET
                    result_key=excluded.result_key,
                    status=excluded.status,
                    home_score=excluded.home_score,
                    away_score=excluded.away_score,
                    is_final=excluded.is_final,
                    updated_at=excluded.updated_at
                """,
                (
                    event.event_id,
                    event.result_key,
                    event.status,
                    event.home_score,
                    event.away_score,
                    1 if event.is_final else 0,
                    timestamp,
                ),
            )

    def place_single_bet(self, recommendation: Recommendation, placed_at: datetime) -> bool:
        bet_key = _single_bet_key(recommendation)
        with self._connect() as connection:
            exists = connection.execute(
                "SELECT 1 FROM football_open_bets WHERE bet_key = ?",
                (bet_key,),
            ).fetchone()
            if exists is not None:
                return False
            connection.execute(
                """
                INSERT INTO football_open_bets (
                    bet_key, bet_type, event_id, league, market_key, selection_key,
                    odds, stake, potential_payout, model_probability, implied_probability,
                    edge, expected_value, legs_json, priority_score, tier,
                    price_advantage, minutes_to_start, decision_tags, placed_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    bet_key,
                    "single",
                    recommendation.event_id,
                    recommendation.league,
                    recommendation.market_key,
                    recommendation.selection_key,
                    recommendation.odds,
                    recommendation.recommended_stake,
                    recommendation.recommended_stake * recommendation.odds,
                    recommendation.model_probability,
                    recommendation.implied_probability,
                    recommendation.edge,
                    recommendation.expected_value,
                    None,
                    recommendation.priority_score,
                    recommendation.tier,
                    recommendation.price_advantage,
                    recommendation.minutes_to_start,
                    json.dumps(recommendation.decision_tags, ensure_ascii=False),
                    placed_at.isoformat(),
                ),
            )
            return True

    def place_express_bet(self, express: ExpressRecommendation, placed_at: datetime) -> bool:
        bet_key = _express_bet_key(express)
        legs = [
            {
                "event_id": leg.event_id,
                "selection_key": getattr(leg, "selection_key", _selection_key_from_leg_name(leg.selection_name)),
                "odds": leg.odds,
            }
            for leg in express.legs
        ]
        with self._connect() as connection:
            exists = connection.execute(
                "SELECT 1 FROM football_open_bets WHERE bet_key = ?",
                (bet_key,),
            ).fetchone()
            if exists is not None:
                return False
            connection.execute(
                """
                INSERT INTO football_open_bets (
                    bet_key, bet_type, event_id, league, market_key, selection_key,
                    odds, stake, potential_payout, model_probability, implied_probability,
                    edge, expected_value, legs_json, priority_score, tier,
                    price_advantage, minutes_to_start, decision_tags, placed_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    bet_key,
                    "express",
                    express.express_id,
                    "MULTI",
                    "express",
                    "express",
                    express.total_odds,
                    express.recommended_stake,
                    express.recommended_stake * express.total_odds,
                    express.model_probability,
                    express.implied_probability,
                    express.edge,
                    express.expected_value,
                    json.dumps(legs, ensure_ascii=False),
                    None,
                    None,
                    None,
                    None,
                    None,
                    placed_at.isoformat(),
                ),
            )
            return True

    def open_bets_count(self) -> int:
        with self._connect() as connection:
            row = connection.execute("SELECT COUNT(*) FROM football_open_bets").fetchone()
        return int(row[0]) if row else 0

    def daily_placed_count(self, stat_date: date) -> int:
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT COUNT(*)
                FROM football_open_bets
                WHERE date(placed_at) = ?
                """,
                (stat_date.isoformat(),),
            ).fetchone()
        return int(row[0]) if row else 0

    def settle_open_bets(self, settled_at: datetime) -> dict[str, float]:
        settled_count = 0
        pnl_sum = 0.0
        turnover_sum = 0.0
        win_count = 0
        clv_sum = 0.0

        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT bet_key, bet_type, event_id, league, market_key, selection_key,
                       odds, stake, model_probability, implied_probability, edge, expected_value,
                       legs_json, priority_score, tier, price_advantage, minutes_to_start,
                       decision_tags, placed_at
                FROM football_open_bets
                """
            ).fetchall()

            for row in rows:
                (
                    bet_key,
                    bet_type,
                    event_id,
                    league,
                    market_key,
                    selection_key,
                    odds,
                    stake,
                    model_probability,
                    implied_probability,
                    edge,
                    expected_value,
                    legs_json,
                    priority_score,
                    tier,
                    price_advantage,
                    minutes_to_start,
                    decision_tags,
                    placed_at,
                ) = row
                outcome = _resolve_outcome(connection, bet_type, event_id, selection_key, legs_json)
                if outcome is None:
                    continue

                payout = 0.0
                if outcome == "win":
                    payout = float(stake) * float(odds)
                    win_count += 1
                elif outcome == "push":
                    payout = float(stake)
                pnl = payout - float(stake)

                settled_count += 1
                pnl_sum += pnl
                turnover_sum += float(stake)
                clv_sum += float(edge)

                connection.execute(
                    """
                    INSERT OR REPLACE INTO football_closed_bets (
                        bet_key, bet_type, event_id, league, market_key, selection_key,
                        odds, stake, payout, pnl, outcome,
                        model_probability, implied_probability, edge, expected_value,
                        priority_score, tier, price_advantage, minutes_to_start,
                        decision_tags, placed_at, settled_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        bet_key,
                        bet_type,
                        event_id,
                        league,
                        market_key,
                        selection_key,
                        odds,
                        stake,
                        payout,
                        pnl,
                        outcome,
                        model_probability,
                        implied_probability,
                        edge,
                        expected_value,
                        priority_score,
                        tier,
                        price_advantage,
                        minutes_to_start,
                        decision_tags,
                        placed_at,
                        settled_at.isoformat(),
                    ),
                )
                connection.execute(
                    "DELETE FROM football_open_bets WHERE bet_key = ?",
                    (bet_key,),
                )

        hit_rate = (win_count / settled_count) if settled_count else 0.0
        clv_proxy = (clv_sum / settled_count) if settled_count else 0.0
        return {
            "settled_count": float(settled_count),
            "pnl": pnl_sum,
            "turnover": turnover_sum,
            "hit_rate": hit_rate,
            "clv_proxy": clv_proxy,
        }

    def daily_closed_metrics(self, stat_date: date) -> dict[str, float]:
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT
                    COUNT(*),
                    COALESCE(SUM(stake), 0),
                    COALESCE(SUM(pnl), 0),
                    COALESCE(AVG(CASE WHEN outcome = 'win' THEN 1.0 ELSE 0.0 END), 0),
                    COALESCE(AVG(edge), 0)
                FROM football_closed_bets
                WHERE date(settled_at) = ?
                """,
                (stat_date.isoformat(),),
            ).fetchone()

        settled_count = int(row[0]) if row else 0
        turnover = float(row[1]) if row else 0.0
        pnl = float(row[2]) if row else 0.0
        hit_rate = float(row[3]) if row else 0.0
        clv_proxy = float(row[4]) if row else 0.0
        roi = (pnl / turnover) if turnover > 0 else 0.0
        return {
            "settled_count": float(settled_count),
            "turnover": turnover,
            "pnl": pnl,
            "hit_rate": hit_rate,
            "clv_proxy": clv_proxy,
            "roi": roi,
        }

    def rolling_closed_metrics(self, days: int, until: date) -> dict[str, float]:
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT
                    COUNT(*),
                    COALESCE(SUM(stake), 0),
                    COALESCE(SUM(pnl), 0),
                    COALESCE(AVG(CASE WHEN outcome = 'win' THEN 1.0 ELSE 0.0 END), 0),
                    COALESCE(AVG(edge), 0)
                FROM football_closed_bets
                WHERE date(settled_at) >= date(?, ?)
                  AND date(settled_at) <= date(?)
                """,
                (until.isoformat(), f"-{days - 1} day", until.isoformat()),
            ).fetchone()

        count = float(row[0]) if row else 0.0
        turnover = float(row[1]) if row else 0.0
        pnl = float(row[2]) if row else 0.0
        hit_rate = float(row[3]) if row else 0.0
        clv_proxy = float(row[4]) if row else 0.0
        roi = (pnl / turnover) if turnover > 0 else 0.0
        return {
            "count": count,
            "turnover": turnover,
            "pnl": pnl,
            "roi": roi,
            "hit_rate": hit_rate,
            "clv_proxy": clv_proxy,
        }

    def upsert_daily_stats(
        self,
        stat_date: date,
        *,
        placed_count: int,
        settled_count: int,
        open_count: int,
        pnl: float,
        turnover: float,
        roi: float,
        hit_rate: float,
        clv_proxy: float,
        stop_triggered: bool,
        updated_at: datetime,
    ) -> None:
        with self._connect() as connection:
            connection.execute(
                """
                INSERT INTO football_daily_stats (
                    stat_date, placed_count, settled_count, open_count, pnl,
                    turnover, roi, hit_rate, clv_proxy, stop_triggered, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(stat_date) DO UPDATE SET
                    placed_count=excluded.placed_count,
                    settled_count=excluded.settled_count,
                    open_count=excluded.open_count,
                    pnl=excluded.pnl,
                    turnover=excluded.turnover,
                    roi=excluded.roi,
                    hit_rate=excluded.hit_rate,
                    clv_proxy=excluded.clv_proxy,
                    stop_triggered=excluded.stop_triggered,
                    updated_at=excluded.updated_at
                """,
                (
                    stat_date.isoformat(),
                    placed_count,
                    settled_count,
                    open_count,
                    pnl,
                    turnover,
                    roi,
                    hit_rate,
                    clv_proxy,
                    1 if stop_triggered else 0,
                    updated_at.isoformat(),
                ),
            )

    def set_stop_status(self, stat_date: date, is_blocked: bool, reason: str, now: datetime) -> None:
        with self._connect() as connection:
            connection.execute(
                """
                INSERT INTO football_stop_status (
                    stat_date, is_blocked, reason, triggered_at, updated_at
                ) VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(stat_date) DO UPDATE SET
                    is_blocked=excluded.is_blocked,
                    reason=excluded.reason,
                    updated_at=excluded.updated_at
                """,
                (
                    stat_date.isoformat(),
                    1 if is_blocked else 0,
                    reason,
                    now.isoformat(),
                    now.isoformat(),
                ),
            )

    def get_stop_status(self, stat_date: date) -> dict[str, str | int] | None:
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT is_blocked, reason, triggered_at, updated_at
                FROM football_stop_status
                WHERE stat_date = ?
                """,
                (stat_date.isoformat(),),
            ).fetchone()
        if row is None:
            return None
        return {
            "is_blocked": int(row[0]),
            "reason": str(row[1]),
            "triggered_at": str(row[2]),
            "updated_at": str(row[3]),
        }

    def log_decisions(self, entries: list[dict[str, Any]], captured_at: datetime) -> None:
        if not entries:
            return
        timestamp = captured_at.isoformat()
        with self._connect() as connection:
            for item in entries:
                connection.execute(
                    """
                    INSERT INTO football_decisions (
                        captured_at, event_id, league, market_key, selection_key,
                        bookmaker, odds, minutes_to_start, price_advantage, edge,
                        expected_value, priority_score, tier, decision, reason
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        timestamp,
                        str(item.get("event_id") or ""),
                        str(item.get("league") or ""),
                        str(item.get("market_key") or ""),
                        str(item.get("selection_key") or ""),
                        str(item.get("bookmaker") or ""),
                        float(item.get("odds") or 0.0),
                        int(item["minutes_to_start"]) if item.get("minutes_to_start") is not None else None,
                        float(item["price_advantage"]) if item.get("price_advantage") is not None else None,
                        float(item["edge"]) if item.get("edge") is not None else None,
                        float(item["expected_value"]) if item.get("expected_value") is not None else None,
                        float(item["priority_score"]) if item.get("priority_score") is not None else None,
                        str(item.get("tier") or "C"),
                        str(item.get("decision") or "rejected"),
                        str(item.get("reason") or "unknown"),
                    ),
                )

    def rolling_league_performance(self, days: int, until: date, *, limit: int = 5) -> list[dict[str, float | str]]:
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT
                    league,
                    COUNT(*) AS bets_count,
                    COALESCE(AVG(expected_value), 0) AS avg_ev,
                    COALESCE(AVG(CASE WHEN outcome = 'win' THEN 1.0 ELSE 0.0 END), 0) AS hit_rate,
                    COALESCE(SUM(pnl), 0) AS pnl_sum,
                    COALESCE(SUM(stake), 0) AS turnover,
                    COALESCE(AVG(edge), 0) AS clv_proxy
                FROM football_closed_bets
                WHERE date(settled_at) >= date(?, ?)
                  AND date(settled_at) <= date(?)
                GROUP BY league
                ORDER BY bets_count DESC, avg_ev DESC
                LIMIT ?
                """,
                (until.isoformat(), f"-{days - 1} day", until.isoformat(), limit),
            ).fetchall()
        payload: list[dict[str, float | str]] = []
        for row in rows:
            turnover = float(row[5] or 0.0)
            payload.append(
                {
                    "league": str(row[0]),
                    "bets_count": int(row[1] or 0),
                    "avg_ev": float(row[2] or 0.0),
                    "hit_rate": float(row[3] or 0.0),
                    "roi": (float(row[4] or 0.0) / turnover) if turnover > 0 else 0.0,
                    "clv_proxy": float(row[6] or 0.0),
                }
            )
        return payload

    def rolling_market_performance(self, days: int, until: date, *, limit: int = 5) -> list[dict[str, float | str]]:
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT
                    market_key,
                    COUNT(*) AS bets_count,
                    COALESCE(AVG(expected_value), 0) AS avg_ev,
                    COALESCE(AVG(CASE WHEN outcome = 'win' THEN 1.0 ELSE 0.0 END), 0) AS hit_rate,
                    COALESCE(SUM(pnl), 0) AS pnl_sum,
                    COALESCE(SUM(stake), 0) AS turnover,
                    COALESCE(AVG(edge), 0) AS clv_proxy
                FROM football_closed_bets
                WHERE date(settled_at) >= date(?, ?)
                  AND date(settled_at) <= date(?)
                GROUP BY market_key
                ORDER BY bets_count DESC, avg_ev DESC
                LIMIT ?
                """,
                (until.isoformat(), f"-{days - 1} day", until.isoformat(), limit),
            ).fetchall()
        payload: list[dict[str, float | str]] = []
        for row in rows:
            turnover = float(row[5] or 0.0)
            payload.append(
                {
                    "market_key": str(row[0]),
                    "bets_count": int(row[1] or 0),
                    "avg_ev": float(row[2] or 0.0),
                    "hit_rate": float(row[3] or 0.0),
                    "roi": (float(row[4] or 0.0) / turnover) if turnover > 0 else 0.0,
                    "clv_proxy": float(row[6] or 0.0),
                }
            )
        return payload

    def rolling_window_performance(self, days: int, until: date) -> list[dict[str, float | str]]:
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT
                    CASE
                        WHEN minutes_to_start < 60 THEN '0-59'
                        WHEN minutes_to_start <= 120 THEN '60-120'
                        WHEN minutes_to_start <= 240 THEN '121-240'
                        ELSE '241+'
                    END AS window_bucket,
                    COUNT(*) AS signals_count,
                    COALESCE(AVG(expected_value), 0) AS avg_ev,
                    COALESCE(AVG(edge), 0) AS avg_edge,
                    COALESCE(AVG(price_advantage), 0) AS avg_price_advantage
                FROM football_decisions
                WHERE decision = 'accepted'
                  AND minutes_to_start IS NOT NULL
                  AND date(captured_at) >= date(?, ?)
                  AND date(captured_at) <= date(?)
                GROUP BY window_bucket
                ORDER BY
                    CASE window_bucket
                        WHEN '0-59' THEN 1
                        WHEN '60-120' THEN 2
                        WHEN '121-240' THEN 3
                        ELSE 4
                    END
                """,
                (until.isoformat(), f"-{days - 1} day", until.isoformat()),
            ).fetchall()
        return [
            {
                "window_bucket": str(row[0]),
                "signals_count": int(row[1] or 0),
                "avg_ev": float(row[2] or 0.0),
                "avg_edge": float(row[3] or 0.0),
                "avg_price_advantage": float(row[4] or 0.0),
            }
            for row in rows
        ]

    def league_sample_gate(self, days: int, until: date, *, sample_gate: int = 30) -> dict[str, dict[str, float | str]]:
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT
                    league,
                    COUNT(*) AS settled_count,
                    COALESCE(SUM(pnl), 0) AS pnl_sum,
                    COALESCE(SUM(stake), 0) AS turnover
                FROM football_closed_bets
                WHERE date(settled_at) >= date(?, ?)
                  AND date(settled_at) <= date(?)
                GROUP BY league
                """,
                (until.isoformat(), f"-{days - 1} day", until.isoformat()),
            ).fetchall()
        gates: dict[str, dict[str, float | str]] = {}
        for league, settled_count, pnl_sum, turnover in rows:
            count = int(settled_count or 0)
            total_turnover = float(turnover or 0.0)
            roi = (float(pnl_sum or 0.0) / total_turnover) if total_turnover > 0 else 0.0
            if count < sample_gate:
                continue
            if roi <= -0.10:
                gates[str(league)] = {"action": "ban", "roi": roi, "count": count}
            elif roi <= -0.03:
                gates[str(league)] = {"action": "penalty", "roi": roi, "count": count}
        return gates

    def performance_snapshot(self, now: datetime | None = None) -> dict[str, Any]:
        timestamp = now or datetime.now(UTC)
        stat_date = timestamp.date()
        rolling_7d = self.rolling_closed_metrics(7, stat_date)
        rolling_30d = self.rolling_closed_metrics(30, stat_date)
        return {
            "rolling": {
                "7d": rolling_7d,
                "30d": rolling_30d,
            },
            "top_leagues": self.rolling_league_performance(30, stat_date, limit=5),
            "top_markets": self.rolling_market_performance(30, stat_date, limit=5),
            "window_minutes": self.rolling_window_performance(30, stat_date),
        }


class MoexSignalRepository:
    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    @contextmanager
    def _connect(self) -> Iterator[sqlite3.Connection]:
        connection = sqlite3.connect(self.path)
        try:
            yield connection
            connection.commit()
        finally:
            connection.close()

    def _init_db(self) -> None:
        with self._connect() as connection:
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS moex_signals (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    symbol TEXT NOT NULL,
                    action TEXT NOT NULL,
                    score REAL NOT NULL,
                    confidence REAL NOT NULL,
                    last_price REAL NOT NULL,
                    expected_move_pct REAL NOT NULL,
                    position_share REAL NOT NULL,
                    stop_loss REAL,
                    take_profit REAL,
                    technical_score REAL NOT NULL,
                    event_score REAL NOT NULL,
                    event_count INTEGER NOT NULL,
                    reasons TEXT NOT NULL,
                    generated_at TEXT NOT NULL
                )
                """
            )
            columns = {row[1] for row in connection.execute("PRAGMA table_info(moex_signals)")}
            if "stop_loss" not in columns:
                connection.execute("ALTER TABLE moex_signals ADD COLUMN stop_loss REAL")
            if "take_profit" not in columns:
                connection.execute("ALTER TABLE moex_signals ADD COLUMN take_profit REAL")
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS moex_notification_state (
                    symbol TEXT PRIMARY KEY,
                    action TEXT NOT NULL,
                    last_price REAL NOT NULL,
                    score REAL NOT NULL,
                    confidence REAL NOT NULL,
                    stop_loss REAL,
                    take_profit REAL,
                    sent_at TEXT NOT NULL
                )
                """
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS moex_longterm_signals (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    symbol TEXT NOT NULL,
                    profile TEXT NOT NULL,
                    action TEXT NOT NULL,
                    score REAL NOT NULL,
                    confidence REAL NOT NULL,
                    last_price REAL NOT NULL,
                    horizon_days_min INTEGER NOT NULL,
                    horizon_days_max INTEGER NOT NULL,
                    position_share REAL NOT NULL,
                    technical_score REAL NOT NULL,
                    event_score REAL NOT NULL,
                    event_count INTEGER NOT NULL,
                    confirmation_count INTEGER NOT NULL,
                    holding_stage TEXT NOT NULL,
                    stop_loss REAL,
                    take_profit REAL,
                    reasons TEXT NOT NULL,
                    generated_at TEXT NOT NULL
                )
                """
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS moex_longterm_notification_state (
                    symbol TEXT NOT NULL,
                    profile TEXT NOT NULL,
                    action TEXT NOT NULL,
                    score REAL NOT NULL,
                    confidence REAL NOT NULL,
                    confirmation_count INTEGER NOT NULL,
                    last_price REAL NOT NULL,
                    stop_loss REAL,
                    take_profit REAL,
                    sent_at TEXT NOT NULL,
                    PRIMARY KEY (symbol, profile)
                )
                """
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS moex_longterm_positions (
                    symbol TEXT NOT NULL,
                    profile TEXT NOT NULL,
                    action TEXT NOT NULL,
                    score REAL NOT NULL,
                    confidence REAL NOT NULL,
                    confirmation_count INTEGER NOT NULL,
                    last_price REAL NOT NULL,
                    stop_loss REAL,
                    take_profit REAL,
                    opened_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    is_active INTEGER NOT NULL DEFAULT 1,
                    PRIMARY KEY (symbol, profile)
                )
                """
            )

    def persist_signals(self, signals: list[MoexSignal]) -> None:
        with self._connect() as connection:
            for signal in signals:
                connection.execute(
                    """
                    INSERT INTO moex_signals (
                        symbol, action, score, confidence, last_price, expected_move_pct,
                        position_share, stop_loss, take_profit, technical_score,
                        event_score, event_count, reasons, generated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        signal.symbol,
                        signal.action,
                        signal.score,
                        signal.confidence,
                        signal.last_price,
                        signal.expected_move_pct,
                        signal.position_share,
                        signal.stop_loss,
                        signal.take_profit,
                        signal.technical_score,
                        signal.event_score,
                        signal.event_count,
                        " | ".join(signal.reasons),
                        signal.generated_at.isoformat(),
                    ),
                )

    def persist_longterm_signals(self, signals: list[LongTermSignal]) -> None:
        with self._connect() as connection:
            for signal in signals:
                connection.execute(
                    """
                    INSERT INTO moex_longterm_signals (
                        symbol, profile, action, score, confidence, last_price,
                        horizon_days_min, horizon_days_max, position_share,
                        technical_score, event_score, event_count,
                        confirmation_count, holding_stage, stop_loss, take_profit,
                        reasons, generated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        signal.symbol,
                        signal.profile,
                        signal.action,
                        signal.score,
                        signal.confidence,
                        signal.last_price,
                        signal.horizon_days_min,
                        signal.horizon_days_max,
                        signal.position_share,
                        signal.technical_score,
                        signal.event_score,
                        signal.event_count,
                        signal.confirmation_count,
                        signal.holding_stage,
                        signal.stop_loss,
                        signal.take_profit,
                        " | ".join(signal.reasons),
                        signal.generated_at.isoformat(),
                    ),
                )

    def get_notification_state(self, symbol: str) -> dict[str, float | str | None] | None:
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT action, last_price, score, confidence, stop_loss, take_profit, sent_at
                FROM moex_notification_state
                WHERE symbol = ?
                """,
                (symbol,),
            ).fetchone()
        if row is None:
            return None
        return {
            "action": row[0],
            "last_price": row[1],
            "score": row[2],
            "confidence": row[3],
            "stop_loss": row[4],
            "take_profit": row[5],
            "sent_at": row[6],
        }

    def upsert_notification_state(self, signal: MoexSignal) -> None:
        with self._connect() as connection:
            connection.execute(
                """
                INSERT INTO moex_notification_state (
                    symbol, action, last_price, score, confidence, stop_loss, take_profit, sent_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(symbol) DO UPDATE SET
                    action=excluded.action,
                    last_price=excluded.last_price,
                    score=excluded.score,
                    confidence=excluded.confidence,
                    stop_loss=excluded.stop_loss,
                    take_profit=excluded.take_profit,
                    sent_at=excluded.sent_at
                """,
                (
                    signal.symbol,
                    signal.action,
                    signal.last_price,
                    signal.score,
                    signal.confidence,
                    signal.stop_loss,
                    signal.take_profit,
                    signal.generated_at.isoformat(),
                ),
            )

    def get_longterm_notification_state(self, symbol: str, profile: str) -> dict[str, float | str | None] | None:
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT action, score, confidence, confirmation_count, last_price, stop_loss, take_profit, sent_at
                FROM moex_longterm_notification_state
                WHERE symbol = ? AND profile = ?
                """,
                (symbol, profile),
            ).fetchone()
        if row is None:
            return None
        return {
            "action": row[0],
            "score": row[1],
            "confidence": row[2],
            "confirmation_count": row[3],
            "last_price": row[4],
            "stop_loss": row[5],
            "take_profit": row[6],
            "sent_at": row[7],
        }

    def upsert_longterm_notification_state(self, signal: LongTermSignal) -> None:
        with self._connect() as connection:
            connection.execute(
                """
                INSERT INTO moex_longterm_notification_state (
                    symbol, profile, action, score, confidence, confirmation_count,
                    last_price, stop_loss, take_profit, sent_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(symbol, profile) DO UPDATE SET
                    action=excluded.action,
                    score=excluded.score,
                    confidence=excluded.confidence,
                    confirmation_count=excluded.confirmation_count,
                    last_price=excluded.last_price,
                    stop_loss=excluded.stop_loss,
                    take_profit=excluded.take_profit,
                    sent_at=excluded.sent_at
                """,
                (
                    signal.symbol,
                    signal.profile,
                    signal.action,
                    signal.score,
                    signal.confidence,
                    signal.confirmation_count,
                    signal.last_price,
                    signal.stop_loss,
                    signal.take_profit,
                    signal.generated_at.isoformat(),
                ),
            )

    def refresh_longterm_positions(self, actionable: list[LongTermSignal]) -> None:
        timestamp = datetime.now(UTC).isoformat()
        active_keys = {(item.symbol, item.profile) for item in actionable}
        with self._connect() as connection:
            for signal in actionable:
                row = connection.execute(
                    """
                    SELECT opened_at FROM moex_longterm_positions
                    WHERE symbol = ? AND profile = ?
                    """,
                    (signal.symbol, signal.profile),
                ).fetchone()
                opened_at = str(row[0]) if row else timestamp
                connection.execute(
                    """
                    INSERT INTO moex_longterm_positions (
                        symbol, profile, action, score, confidence, confirmation_count,
                        last_price, stop_loss, take_profit, opened_at, updated_at, is_active
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)
                    ON CONFLICT(symbol, profile) DO UPDATE SET
                        action=excluded.action,
                        score=excluded.score,
                        confidence=excluded.confidence,
                        confirmation_count=excluded.confirmation_count,
                        last_price=excluded.last_price,
                        stop_loss=excluded.stop_loss,
                        take_profit=excluded.take_profit,
                        updated_at=excluded.updated_at,
                        is_active=1
                    """,
                    (
                        signal.symbol,
                        signal.profile,
                        signal.action,
                        signal.score,
                        signal.confidence,
                        signal.confirmation_count,
                        signal.last_price,
                        signal.stop_loss,
                        signal.take_profit,
                        opened_at,
                        timestamp,
                    ),
                )
            rows = connection.execute(
                "SELECT symbol, profile FROM moex_longterm_positions WHERE is_active = 1"
            ).fetchall()
            for symbol, profile in rows:
                if (str(symbol), str(profile)) in active_keys:
                    continue
                connection.execute(
                    """
                    UPDATE moex_longterm_positions
                    SET is_active = 0, updated_at = ?
                    WHERE symbol = ? AND profile = ?
                    """,
                    (timestamp, symbol, profile),
                )

    def longterm_performance_snapshot(self) -> dict[str, Any]:
        with self._connect() as connection:
            rows_profile = connection.execute(
                """
                SELECT
                    profile,
                    COUNT(*) AS signals_count,
                    COALESCE(AVG(score), 0) AS avg_score,
                    COALESCE(AVG(confidence), 0) AS avg_confidence,
                    COALESCE(AVG(CASE WHEN action IN ('BUY','SELL') THEN 1.0 ELSE 0.0 END), 0) AS actionable_rate
                FROM moex_longterm_signals
                WHERE date(generated_at) >= date('now', '-30 day')
                GROUP BY profile
                ORDER BY profile
                """
            ).fetchall()
            rows_actions = connection.execute(
                """
                SELECT action, COUNT(*)
                FROM moex_longterm_signals
                WHERE date(generated_at) >= date('now', '-30 day')
                GROUP BY action
                ORDER BY COUNT(*) DESC
                """
            ).fetchall()
            rows_positions = connection.execute(
                """
                SELECT symbol, profile, action, score, confidence, updated_at
                FROM moex_longterm_positions
                WHERE is_active = 1
                ORDER BY confidence DESC, ABS(score) DESC
                """
            ).fetchall()
        return {
            "profiles_30d": [
                {
                    "profile": str(row[0]),
                    "signals_count": int(row[1]),
                    "avg_score": float(row[2]),
                    "avg_confidence": float(row[3]),
                    "actionable_rate": float(row[4]),
                }
                for row in rows_profile
            ],
            "actions_30d": [
                {"action": str(row[0]), "count": int(row[1])}
                for row in rows_actions
            ],
            "active_positions": [
                {
                    "symbol": str(row[0]),
                    "profile": str(row[1]),
                    "action": str(row[2]),
                    "score": float(row[3]),
                    "confidence": float(row[4]),
                    "updated_at": str(row[5]),
                }
                for row in rows_positions
            ],
            "active_positions_count": len(rows_positions),
        }


def _single_bet_key(recommendation: Recommendation) -> str:
    return "|".join(
        [
            "single",
            recommendation.event_id,
            recommendation.market_key,
            recommendation.selection_key,
            recommendation.bookmaker or "book",
        ]
    )


def _express_bet_key(express: ExpressRecommendation) -> str:
    legs = sorted(
        f"{leg.event_id}:{getattr(leg, 'selection_key', _selection_key_from_leg_name(leg.selection_name))}"
        for leg in express.legs
    )
    return "|".join(["express", ",".join(legs)])


def _selection_key_from_leg_name(value: str) -> str:
    lowered = value.lower()
    if lowered == "draw":
        return "draw"
    if "under" in lowered:
        return "under"
    if "over" in lowered:
        return "over"
    if lowered in {"yes", "btts yes"}:
        return "yes"
    if lowered in {"no", "btts no"}:
        return "no"
    return lowered


def _resolve_outcome(
    connection: sqlite3.Connection,
    bet_type: str,
    event_id: str,
    selection_key: str,
    legs_json: str | None,
) -> str | None:
    if bet_type == "single":
        row = connection.execute(
            "SELECT result_key, is_final FROM football_event_results WHERE event_id = ?",
            (event_id,),
        ).fetchone()
        if row is None or int(row[1]) != 1:
            return None
        result_key = str(row[0] or "")
        return "win" if result_key == selection_key else "lose"

    if bet_type == "express":
        if not legs_json:
            return "lose"
        try:
            legs = json.loads(legs_json)
        except json.JSONDecodeError:
            return "lose"
        if not isinstance(legs, list) or not legs:
            return "lose"
        for leg in legs:
            if not isinstance(leg, dict):
                return "lose"
            leg_event = str(leg.get("event_id") or "")
            leg_selection = str(leg.get("selection_key") or "")
            row = connection.execute(
                "SELECT result_key, is_final FROM football_event_results WHERE event_id = ?",
                (leg_event,),
            ).fetchone()
            if row is None or int(row[1]) != 1:
                return None
            result_key = str(row[0] or "")
            if result_key != leg_selection:
                return "lose"
        return "win"

    return None

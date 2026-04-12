from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Iterator

from .football_api import FootballEventOdds
from .models import MoexSignal


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

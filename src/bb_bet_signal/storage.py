from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Iterator

from .football_api import FootballEventOdds


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

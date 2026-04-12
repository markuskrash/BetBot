from __future__ import annotations

import asyncio
import tempfile
import unittest
from pathlib import Path

from bb_bet_signal.config import EngineConfig
from bb_bet_signal.football_engine import FootballConsensusEngine
from bb_bet_signal.football_service import FootballPollingService
from bb_bet_signal.storage import SnapshotRepository
from test_football_engine import build_event


class FakeOddsApiClient:
    def get_bookmakers(self):
        return ["Bet365", "Unibet", "Pinnacle"]

    def get_selected_bookmakers(self):
        return ["Bet365", "Unibet", "Pinnacle"]

    def get_events(
        self,
        *,
        sport: str = "football",
        limit: int = 10,
        bookmaker: str | None = None,
        status: str | None = None,
    ):
        return [{"id": "42"}]

    def get_odds_multi(self, event_ids, bookmakers):
        return [build_event()]


class FootballPollingServiceTest(unittest.TestCase):
    def test_poll_once_updates_latest_and_persists(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            repository = SnapshotRepository(Path(tmp_dir) / "snapshots.sqlite3")
            service = FootballPollingService(
                FakeOddsApiClient(),
                FootballConsensusEngine(EngineConfig(bankroll=5000, min_edge=0.01, min_expected_value=0.01)),
                repository,
                target_bookmaker="Bet365",
                comparison_bookmakers=["Unibet", "Pinnacle"],
                poll_seconds=300,
                event_limit=10,
            )
            recommendations = asyncio.run(service.poll_once())
            self.assertTrue(recommendations)
            self.assertTrue(service.latest())


if __name__ == "__main__":
    unittest.main()

from __future__ import annotations

import asyncio
import tempfile
import unittest
from pathlib import Path

from bb_bet_signal.config import EngineConfig, FootballRiskConfig
from bb_bet_signal.football_engine import FootballConsensusEngine
from bb_bet_signal.football_risk import FootballRiskGovernor
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


class FakeMultiOddsApiClient(FakeOddsApiClient):
    def get_events(
        self,
        *,
        sport: str = "football",
        limit: int = 10,
        bookmaker: str | None = None,
        status: str | None = None,
    ):
        return [{"id": "1"}, {"id": "2"}, {"id": "3"}]

    def get_odds_multi(self, event_ids, bookmakers):
        events = []
        for idx, home_odds in enumerate([2.75, 2.95, 3.15], start=1):
            event = build_event()
            event.event_id = str(idx)
            event.home = f"Home {idx}"
            event.away = f"Away {idx}"
            event.league = f"League {idx}"
            event.bookmakers["Bet365"][0].selections[0].odds = home_odds
            events.append(event)
        return events


class FootballPollingServiceTest(unittest.TestCase):
    def test_poll_once_updates_latest_and_persists(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            repository = SnapshotRepository(Path(tmp_dir) / "snapshots.sqlite3")
            risk_governor = FootballRiskGovernor(
                repository,
                bankroll=5000.0,
                risk_config=FootballRiskConfig(),
                base_min_edge=0.01,
                base_min_ev=0.01,
            )
            service = FootballPollingService(
                FakeOddsApiClient(),
                FootballConsensusEngine(EngineConfig(bankroll=5000, min_edge=0.01, min_expected_value=0.01)),
                repository,
                target_bookmaker="Bet365",
                comparison_bookmakers=["Unibet", "Pinnacle"],
                risk_governor=risk_governor,
                poll_seconds=300,
                event_limit=10,
            )
            recommendations = asyncio.run(service.poll_once())
            self.assertTrue(recommendations)
            self.assertTrue(service.latest())
            performance = service.performance()
            self.assertIn("rolling", performance)
            self.assertIn("top_leagues", performance)

    def test_cycle_ranking_assigns_tiers_across_candidates(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            repository = SnapshotRepository(Path(tmp_dir) / "snapshots.sqlite3")
            risk_governor = FootballRiskGovernor(
                repository,
                bankroll=5000.0,
                risk_config=FootballRiskConfig(),
                base_min_edge=0.01,
                base_min_ev=0.01,
            )
            service = FootballPollingService(
                FakeMultiOddsApiClient(),
                FootballConsensusEngine(EngineConfig(bankroll=5000, min_edge=0.01, min_expected_value=0.01)),
                repository,
                target_bookmaker="Bet365",
                comparison_bookmakers=["Unibet", "Pinnacle"],
                risk_governor=risk_governor,
                poll_seconds=300,
                event_limit=10,
            )
            recommendations = asyncio.run(service.poll_once())
            tiers = {item.tier for item in recommendations}
            self.assertIn("A", tiers)
            self.assertIn("B", tiers)
            self.assertIn("C", tiers)

    def test_frequency_controller_relaxes_thresholds_when_signals_are_too_rare(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            repository = SnapshotRepository(Path(tmp_dir) / "snapshots.sqlite3")
            risk_governor = FootballRiskGovernor(
                repository,
                bankroll=5000.0,
                risk_config=FootballRiskConfig(),
                base_min_edge=0.01,
                base_min_ev=0.01,
            )
            service = FootballPollingService(
                FakeOddsApiClient(),
                FootballConsensusEngine(EngineConfig(bankroll=5000, min_edge=0.01, min_expected_value=0.01)),
                repository,
                target_bookmaker="Bet365",
                comparison_bookmakers=["Unibet", "Pinnacle"],
                risk_governor=risk_governor,
                poll_seconds=120,
                event_limit=10,
                min_minutes_to_start=500,
                max_minutes_to_start=600,
            )
            for _ in range(12):
                asyncio.run(service.poll_once())
            self.assertLess(service._edge_tuning_offset, 0.0)
            self.assertLess(service._ev_tuning_offset, 0.0)


if __name__ == "__main__":
    unittest.main()

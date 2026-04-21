from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
from pathlib import Path
from typing import Sequence

from .config import (
    DEFAULT_DAILY_DD_LIMIT,
    DEFAULT_MAX_BETS_PER_DAY,
    DEFAULT_MAX_EXPRESS_SHARE,
    DEFAULT_MAX_OPEN_BETS,
    DEFAULT_MAX_SINGLE_SHARE,
    DEFAULT_MIN_EDGE,
    DEFAULT_MIN_EV,
    EngineConfig,
    FeedConfig,
    FootballRiskConfig,
)
from .engine import ProbabilityModel
from .football_api import OddsApiClient
from .football_engine import FootballConsensusEngine
from .football_risk import FootballRiskGovernor
from .football_service import FootballPollingService
from .logging_setup import setup_logging
from .moex_api import MoexApiClient
from .moex_engine import MoexSignalEngine
from .moex_longterm import LongTermMoexEngine, MoexLongTermService
from .moex_service import MoexStockService
from .providers import DemoRealtimeFeed, JsonlRealtimeFeed
from .storage import MoexSignalRepository, SnapshotRepository
from .service import ApiServer, SignalService
from .telegram import TelegramNotifier


def _default_http_port() -> int:
    raw = os.environ.get("PORT")
    if raw:
        try:
            return int(raw)
        except ValueError:
            pass
    return 8080


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Realtime betting signal engine")
    subparsers = parser.add_subparsers(dest="command", required=True)

    simulate = subparsers.add_parser("simulate", help="Run demo feed and print live recommendations")
    simulate.add_argument("--ticks", type=int, default=5, help="Number of snapshots per match to process")
    simulate.add_argument("--bankroll", type=float, default=100.0, help="Bankroll for stake sizing")
    simulate.add_argument("--feed-file", type=Path, help="Optional jsonl file with market snapshots")

    serve = subparsers.add_parser("serve", help="Run HTTP API")
    serve.add_argument("--host", default="127.0.0.1")
    serve.add_argument("--port", type=int, default=_default_http_port())
    serve.add_argument("--bankroll", type=float, default=100.0)
    serve.add_argument("--feed-file", type=Path, help="Optional jsonl file with market snapshots")

    football_scan = subparsers.add_parser("football-scan", help="Fetch football odds from free API and print recommendations")
    football_scan.add_argument("--bankroll", type=float, default=100.0)
    football_scan.add_argument("--min-edge", type=float, default=DEFAULT_MIN_EDGE)
    football_scan.add_argument("--min-ev", type=float, default=DEFAULT_MIN_EV)
    football_scan.add_argument("--target-bookmaker", default="Bet365")
    football_scan.add_argument("--bookmakers", default="Bet365,Unibet")
    football_scan.add_argument("--daily-dd-limit", type=float, default=DEFAULT_DAILY_DD_LIMIT)
    football_scan.add_argument("--max-single-share", type=float, default=DEFAULT_MAX_SINGLE_SHARE)
    football_scan.add_argument("--max-express-share", type=float, default=DEFAULT_MAX_EXPRESS_SHARE)
    football_scan.add_argument("--max-open-bets", type=int, default=DEFAULT_MAX_OPEN_BETS)
    football_scan.add_argument("--max-bets-per-day", type=int, default=DEFAULT_MAX_BETS_PER_DAY)
    football_scan.add_argument("--enable-btts", action="store_true")
    football_scan.add_argument("--express-mode", choices=["two-leg"], default="two-leg")
    football_scan.add_argument("--target-bets-per-day", type=int, default=4)
    football_scan.add_argument("--min-minutes-to-start", type=int, default=45)
    football_scan.add_argument("--max-minutes-to-start", type=int, default=240)
    football_scan.add_argument("--realert-odds-delta", type=float, default=0.03)
    football_scan.add_argument("--realert-ev-delta", type=float, default=0.01)
    football_scan.add_argument("--limit", type=int, default=10)
    football_scan.add_argument("--db-path", type=Path, default=Path("data/football_odds.sqlite3"))
    football_scan.add_argument("--notify-telegram", action="store_true")
    football_scan.add_argument("--log-file", type=Path, default=Path("logs/football-scan.log"))
    football_scan.add_argument("--log-level", default="INFO")

    football_serve = subparsers.add_parser("football-serve", help="Run football polling service and HTTP API")
    football_serve.add_argument("--host", default="127.0.0.1")
    football_serve.add_argument("--port", type=int, default=_default_http_port())
    football_serve.add_argument("--bankroll", type=float, default=3500.0)
    football_serve.add_argument("--min-edge", type=float, default=DEFAULT_MIN_EDGE)
    football_serve.add_argument("--min-ev", type=float, default=DEFAULT_MIN_EV)
    football_serve.add_argument("--target-bookmaker", default="Bet365")
    football_serve.add_argument("--bookmakers", default="Bet365,Unibet")
    football_serve.add_argument("--daily-dd-limit", type=float, default=DEFAULT_DAILY_DD_LIMIT)
    football_serve.add_argument("--max-single-share", type=float, default=DEFAULT_MAX_SINGLE_SHARE)
    football_serve.add_argument("--max-express-share", type=float, default=DEFAULT_MAX_EXPRESS_SHARE)
    football_serve.add_argument("--max-open-bets", type=int, default=DEFAULT_MAX_OPEN_BETS)
    football_serve.add_argument("--max-bets-per-day", type=int, default=DEFAULT_MAX_BETS_PER_DAY)
    football_serve.add_argument("--enable-btts", action="store_true")
    football_serve.add_argument("--express-mode", choices=["two-leg"], default="two-leg")
    football_serve.add_argument("--target-bets-per-day", type=int, default=4)
    football_serve.add_argument("--min-minutes-to-start", type=int, default=45)
    football_serve.add_argument("--max-minutes-to-start", type=int, default=240)
    football_serve.add_argument("--realert-odds-delta", type=float, default=0.03)
    football_serve.add_argument("--realert-ev-delta", type=float, default=0.01)
    football_serve.add_argument("--limit", type=int, default=10)
    football_serve.add_argument("--poll-seconds", type=int, default=300)
    football_serve.add_argument("--db-path", type=Path, default=Path("data/football_odds.sqlite3"))
    football_serve.add_argument("--notify-telegram", action="store_true")
    football_serve.add_argument("--log-file", type=Path, default=Path("logs/football-serve.log"))
    football_serve.add_argument("--log-level", default="INFO")

    moex_scan = subparsers.add_parser("moex-scan", help="Fetch MOEX stock signals and print recommendations")
    moex_scan.add_argument("--symbols", default="SBER,GAZP,LKOH,ROSN,NVTK,YDEX,T")
    moex_scan.add_argument("--history-days", type=int, default=180)
    moex_scan.add_argument("--news-limit", type=int, default=150)
    moex_scan.add_argument("--news-window-hours", type=int, default=72)
    moex_scan.add_argument("--poll-seconds", type=int, default=120)
    moex_scan.add_argument("--db-path", type=Path, default=Path("data/moex_signals.sqlite3"))
    moex_scan.add_argument("--notify-telegram", action="store_true")
    moex_scan.add_argument("--log-file", type=Path, default=Path("logs/moex-scan.log"))
    moex_scan.add_argument("--log-level", default="INFO")

    moex_serve = subparsers.add_parser("moex-serve", help="Run MOEX stock polling service and HTTP API")
    moex_serve.add_argument("--host", default="127.0.0.1")
    moex_serve.add_argument("--port", type=int, default=8082)
    moex_serve.add_argument("--symbols", default="SBER,GAZP,LKOH,ROSN,NVTK,YDEX,T")
    moex_serve.add_argument("--history-days", type=int, default=180)
    moex_serve.add_argument("--news-limit", type=int, default=150)
    moex_serve.add_argument("--news-window-hours", type=int, default=72)
    moex_serve.add_argument("--poll-seconds", type=int, default=120)
    moex_serve.add_argument("--db-path", type=Path, default=Path("data/moex_signals.sqlite3"))
    moex_serve.add_argument("--notify-telegram", action="store_true")
    moex_serve.add_argument("--log-file", type=Path, default=Path("logs/moex-serve.log"))
    moex_serve.add_argument("--log-level", default="INFO")

    moex_longterm_scan = subparsers.add_parser("moex-longterm-scan", help="Fetch MOEX long-term signals")
    moex_longterm_scan.add_argument("--symbols", default="SBER,GAZP,LKOH,ROSN,NVTK,YDEX,T")
    moex_longterm_scan.add_argument("--profiles", default="swing,position")
    moex_longterm_scan.add_argument("--history-days", type=int, default=365)
    moex_longterm_scan.add_argument("--news-limit", type=int, default=250)
    moex_longterm_scan.add_argument("--news-window-hours", type=int, default=336)
    moex_longterm_scan.add_argument("--poll-seconds", type=int, default=86400)
    moex_longterm_scan.add_argument("--max-open-positions", type=int, default=5)
    moex_longterm_scan.add_argument("--db-path", type=Path, default=Path("data/moex_signals.sqlite3"))
    moex_longterm_scan.add_argument("--notify-telegram", action="store_true")
    moex_longterm_scan.add_argument("--log-file", type=Path, default=Path("logs/moex-longterm-scan.log"))
    moex_longterm_scan.add_argument("--log-level", default="INFO")

    moex_longterm_serve = subparsers.add_parser("moex-longterm-serve", help="Run MOEX long-term polling service and HTTP API")
    moex_longterm_serve.add_argument("--host", default="127.0.0.1")
    moex_longterm_serve.add_argument("--port", type=int, default=8083)
    moex_longterm_serve.add_argument("--symbols", default="SBER,GAZP,LKOH,ROSN,NVTK,YDEX,T")
    moex_longterm_serve.add_argument("--profiles", default="swing,position")
    moex_longterm_serve.add_argument("--history-days", type=int, default=365)
    moex_longterm_serve.add_argument("--news-limit", type=int, default=250)
    moex_longterm_serve.add_argument("--news-window-hours", type=int, default=336)
    moex_longterm_serve.add_argument("--poll-seconds", type=int, default=86400)
    moex_longterm_serve.add_argument("--max-open-positions", type=int, default=5)
    moex_longterm_serve.add_argument("--db-path", type=Path, default=Path("data/moex_signals.sqlite3"))
    moex_longterm_serve.add_argument("--notify-telegram", action="store_true")
    moex_longterm_serve.add_argument("--log-file", type=Path, default=Path("logs/moex-longterm-serve.log"))
    moex_longterm_serve.add_argument("--log-level", default="INFO")

    return parser


def main(argv: Sequence[str] | None = None) -> None:
    _load_dotenv()
    args = build_parser().parse_args(argv)
    if args.command == "simulate":
        asyncio.run(_simulate(args.ticks, args.bankroll, args.feed_file))
        return
    if args.command == "serve":
        try:
            asyncio.run(_serve(args.host, args.port, args.bankroll, args.feed_file))
        except KeyboardInterrupt:
            pass
        return
    if args.command == "football-scan":
        setup_logging(args.log_level, args.log_file)
        try:
            asyncio.run(
                _football_scan(
                    bankroll=args.bankroll,
                    min_edge=args.min_edge,
                    min_ev=args.min_ev,
                    target_bookmaker=args.target_bookmaker,
                    bookmakers=_split_csv(args.bookmakers),
                    daily_dd_limit=args.daily_dd_limit,
                    max_single_share=args.max_single_share,
                    max_express_share=args.max_express_share,
                    max_open_bets=args.max_open_bets,
                    max_bets_per_day=args.max_bets_per_day,
                    enable_btts=args.enable_btts,
                    express_mode=args.express_mode,
                    target_bets_per_day=args.target_bets_per_day,
                    min_minutes_to_start=args.min_minutes_to_start,
                    max_minutes_to_start=args.max_minutes_to_start,
                    realert_odds_delta=args.realert_odds_delta,
                    realert_ev_delta=args.realert_ev_delta,
                    limit=args.limit,
                    db_path=args.db_path,
                    notify_telegram=args.notify_telegram,
                )
            )
        except RuntimeError as exc:
            raise SystemExit(str(exc)) from exc
        return
    if args.command == "football-serve":
        setup_logging(args.log_level, args.log_file)
        try:
            asyncio.run(
                _football_serve(
                    host=args.host,
                    port=args.port,
                    bankroll=args.bankroll,
                    min_edge=args.min_edge,
                    min_ev=args.min_ev,
                    target_bookmaker=args.target_bookmaker,
                    bookmakers=_split_csv(args.bookmakers),
                    daily_dd_limit=args.daily_dd_limit,
                    max_single_share=args.max_single_share,
                    max_express_share=args.max_express_share,
                    max_open_bets=args.max_open_bets,
                    max_bets_per_day=args.max_bets_per_day,
                    enable_btts=args.enable_btts,
                    express_mode=args.express_mode,
                    target_bets_per_day=args.target_bets_per_day,
                    min_minutes_to_start=args.min_minutes_to_start,
                    max_minutes_to_start=args.max_minutes_to_start,
                    realert_odds_delta=args.realert_odds_delta,
                    realert_ev_delta=args.realert_ev_delta,
                    limit=args.limit,
                    poll_seconds=args.poll_seconds,
                    db_path=args.db_path,
                    notify_telegram=args.notify_telegram,
                )
            )
        except KeyboardInterrupt:
            pass
        except RuntimeError as exc:
            raise SystemExit(str(exc)) from exc
        return
    if args.command == "moex-scan":
        setup_logging(args.log_level, args.log_file)
        try:
            asyncio.run(
                _moex_scan(
                    symbols=_split_csv(args.symbols),
                    history_days=args.history_days,
                    news_limit=args.news_limit,
                    news_window_hours=args.news_window_hours,
                    poll_seconds=args.poll_seconds,
                    db_path=args.db_path,
                    notify_telegram=args.notify_telegram,
                )
            )
        except RuntimeError as exc:
            raise SystemExit(str(exc)) from exc
        return
    if args.command == "moex-serve":
        setup_logging(args.log_level, args.log_file)
        try:
            asyncio.run(
                _moex_serve(
                    host=args.host,
                    port=args.port,
                    symbols=_split_csv(args.symbols),
                    history_days=args.history_days,
                    news_limit=args.news_limit,
                    news_window_hours=args.news_window_hours,
                    poll_seconds=args.poll_seconds,
                    db_path=args.db_path,
                    notify_telegram=args.notify_telegram,
                )
            )
        except KeyboardInterrupt:
            pass
        except RuntimeError as exc:
            raise SystemExit(str(exc)) from exc
        return
    if args.command == "moex-longterm-scan":
        setup_logging(args.log_level, args.log_file)
        try:
            asyncio.run(
                _moex_longterm_scan(
                    symbols=_split_csv(args.symbols),
                    profiles=_split_csv(args.profiles),
                    history_days=args.history_days,
                    news_limit=args.news_limit,
                    news_window_hours=args.news_window_hours,
                    poll_seconds=args.poll_seconds,
                    max_open_positions=args.max_open_positions,
                    db_path=args.db_path,
                    notify_telegram=args.notify_telegram,
                )
            )
        except RuntimeError as exc:
            raise SystemExit(str(exc)) from exc
        return
    if args.command == "moex-longterm-serve":
        setup_logging(args.log_level, args.log_file)
        try:
            asyncio.run(
                _moex_longterm_serve(
                    host=args.host,
                    port=args.port,
                    symbols=_split_csv(args.symbols),
                    profiles=_split_csv(args.profiles),
                    history_days=args.history_days,
                    news_limit=args.news_limit,
                    news_window_hours=args.news_window_hours,
                    poll_seconds=args.poll_seconds,
                    max_open_positions=args.max_open_positions,
                    db_path=args.db_path,
                    notify_telegram=args.notify_telegram,
                )
            )
        except KeyboardInterrupt:
            pass
        except RuntimeError as exc:
            raise SystemExit(str(exc)) from exc
        return
    raise SystemExit(f"Unknown command: {args.command}")


async def _simulate(ticks: int, bankroll: float, feed_file: Path | None) -> None:
    feed = _build_feed(feed_file)
    engine = ProbabilityModel(EngineConfig(bankroll=bankroll))
    stream = feed.stream()
    match_count = len(feed.matches) if isinstance(feed, DemoRealtimeFeed) else 2
    snapshots_to_consume = ticks * match_count
    for _ in range(snapshots_to_consume):
        snapshot = await anext(stream)
        recommendations = engine.evaluate(snapshot)
        payload = {
            "event": f"{snapshot.context.home.name} vs {snapshot.context.away.name}",
            "timestamp": snapshot.timestamp.isoformat(),
            "recommendations": [item.to_dict() for item in recommendations[:3]],
        }
        print(json.dumps(payload, ensure_ascii=False, indent=2))


async def _serve(host: str, port: int, bankroll: float, feed_file: Path | None) -> None:
    feed = _build_feed(feed_file)
    engine = ProbabilityModel(EngineConfig(bankroll=bankroll))
    service = SignalService(feed, engine)
    server = ApiServer(service, host, port)
    server.start()
    print(f"HTTP API listening on http://{host}:{port}")
    try:
        await service.run()
    finally:
        server.stop()


def _build_feed(feed_file: Path | None):
    if feed_file is not None:
        return JsonlRealtimeFeed(feed_file, FeedConfig())
    return DemoRealtimeFeed(FeedConfig())


async def _football_scan(
    *,
    bankroll: float,
    min_edge: float,
    min_ev: float,
    target_bookmaker: str,
    bookmakers: list[str],
    daily_dd_limit: float,
    max_single_share: float,
    max_express_share: float,
    max_open_bets: int,
    max_bets_per_day: int,
    enable_btts: bool,
    express_mode: str,
    target_bets_per_day: int,
    min_minutes_to_start: int,
    max_minutes_to_start: int,
    realert_odds_delta: float,
    realert_ev_delta: float,
    limit: int,
    db_path: Path,
    notify_telegram: bool,
) -> None:
    logging.getLogger(__name__).info(
        "Starting football scan target=%s bookmakers=%s limit=%s notify_telegram=%s",
        target_bookmaker,
        ",".join(bookmakers),
        limit,
        notify_telegram,
    )
    service = _build_football_service(
        bankroll,
        min_edge,
        min_ev,
        target_bookmaker,
        bookmakers,
        daily_dd_limit,
        max_single_share,
        max_express_share,
        max_open_bets,
        max_bets_per_day,
        enable_btts,
        express_mode,
        target_bets_per_day,
        min_minutes_to_start,
        max_minutes_to_start,
        realert_odds_delta,
        realert_ev_delta,
        limit,
        300,
        db_path,
        notify_telegram=notify_telegram,
    )
    recommendations = await service.poll_once()
    logging.getLogger(__name__).info("Football scan completed recommendations=%s", len(recommendations))
    expresses = service.latest_expresses()
    payload = {
        "singles": [item.to_dict() for item in recommendations[:15]],
        "expresses": [item.to_dict() for item in expresses[:10]],
    }
    print(json.dumps(payload, ensure_ascii=False, indent=2))


async def _football_serve(
    *,
    host: str,
    port: int,
    bankroll: float,
    min_edge: float,
    min_ev: float,
    target_bookmaker: str,
    bookmakers: list[str],
    daily_dd_limit: float,
    max_single_share: float,
    max_express_share: float,
    max_open_bets: int,
    max_bets_per_day: int,
    enable_btts: bool,
    express_mode: str,
    target_bets_per_day: int,
    min_minutes_to_start: int,
    max_minutes_to_start: int,
    realert_odds_delta: float,
    realert_ev_delta: float,
    limit: int,
    poll_seconds: int,
    db_path: Path,
    notify_telegram: bool,
) -> None:
    logging.getLogger(__name__).info(
        "Starting football serve host=%s port=%s target=%s bookmakers=%s poll_seconds=%s notify_telegram=%s",
        host,
        port,
        target_bookmaker,
        ",".join(bookmakers),
        poll_seconds,
        notify_telegram,
    )
    service = _build_football_service(
        bankroll,
        min_edge,
        min_ev,
        target_bookmaker,
        bookmakers,
        daily_dd_limit,
        max_single_share,
        max_express_share,
        max_open_bets,
        max_bets_per_day,
        enable_btts,
        express_mode,
        target_bets_per_day,
        min_minutes_to_start,
        max_minutes_to_start,
        realert_odds_delta,
        realert_ev_delta,
        limit,
        poll_seconds,
        db_path,
        notify_telegram=notify_telegram,
    )
    server = ApiServer(service, host, port)
    server.start()
    print(f"Football HTTP API listening on http://{host}:{port}")
    try:
        await service.run_forever()
    finally:
        server.stop()


def _build_football_service(
    bankroll: float,
    min_edge: float,
    min_ev: float,
    target_bookmaker: str,
    bookmakers: list[str],
    daily_dd_limit: float,
    max_single_share: float,
    max_express_share: float,
    max_open_bets: int,
    max_bets_per_day: int,
    enable_btts: bool,
    express_mode: str,
    target_bets_per_day: int,
    min_minutes_to_start: int,
    max_minutes_to_start: int,
    realert_odds_delta: float,
    realert_ev_delta: float,
    limit: int,
    poll_seconds: int,
    db_path: Path,
    *,
    notify_telegram: bool,
) -> FootballPollingService:
    api_key = (os.getenv("ODDS_API_KEY") or "").strip()
    if not api_key:
        raise RuntimeError(
            "ODDS_API_KEY is missing or empty. Set it in the host environment "
            "(e.g. Railway → Variables → ODDS_API_KEY)."
        )
    client = OddsApiClient(api_key=api_key)
    engine = FootballConsensusEngine(
        EngineConfig(bankroll=bankroll, min_edge=min_edge, min_expected_value=min_ev)
    )
    repository = SnapshotRepository(db_path)
    risk_governor = FootballRiskGovernor(
        repository,
        bankroll=bankroll,
        risk_config=FootballRiskConfig(
            daily_drawdown_limit=daily_dd_limit,
            max_single_share=max_single_share,
            max_express_share=max_express_share,
            max_open_bets=max_open_bets,
            max_bets_per_day=max_bets_per_day,
        ),
        base_min_edge=min_edge,
        base_min_ev=min_ev,
    )
    comparison_bookmakers = [item for item in bookmakers if item != target_bookmaker]
    notifier = (
        TelegramNotifier.from_env(
            realert_odds_delta=realert_odds_delta,
            realert_ev_delta=realert_ev_delta,
        )
        if notify_telegram
        else None
    )
    if notify_telegram and notifier is None:
        raise RuntimeError("TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID is not set")
    logging.getLogger(__name__).info(
        "Football service configured db=%s target=%s comparison=%s telegram=%s",
        db_path,
        target_bookmaker,
        ",".join(comparison_bookmakers),
        notify_telegram,
    )
    return FootballPollingService(
        client,
        engine,
        repository,
        target_bookmaker=target_bookmaker,
        comparison_bookmakers=comparison_bookmakers,
        risk_governor=risk_governor,
        enable_btts=enable_btts,
        express_mode=express_mode,
        notifier=notifier,
        poll_seconds=poll_seconds,
        event_limit=limit,
        target_bets_per_day=target_bets_per_day,
        min_minutes_to_start=min_minutes_to_start,
        max_minutes_to_start=max_minutes_to_start,
        stale_market_minutes=15,
        realert_odds_delta=realert_odds_delta,
        realert_ev_delta=realert_ev_delta,
    )


async def _moex_scan(
    *,
    symbols: list[str],
    history_days: int,
    news_limit: int,
    news_window_hours: int,
    poll_seconds: int,
    db_path: Path,
    notify_telegram: bool,
) -> None:
    logging.getLogger(__name__).info(
        "Starting MOEX scan symbols=%s history_days=%s news_limit=%s window_hours=%s notify_telegram=%s",
        ",".join(symbols),
        history_days,
        news_limit,
        news_window_hours,
        notify_telegram,
    )
    service = _build_moex_service(
        symbols=symbols,
        history_days=history_days,
        news_limit=news_limit,
        news_window_hours=news_window_hours,
        poll_seconds=poll_seconds,
        db_path=db_path,
        notify_telegram=notify_telegram,
    )
    signals = await service.poll_once()
    payload = [item.to_dict() for item in signals]
    print(json.dumps(payload, ensure_ascii=False, indent=2))


async def _moex_serve(
    *,
    host: str,
    port: int,
    symbols: list[str],
    history_days: int,
    news_limit: int,
    news_window_hours: int,
    poll_seconds: int,
    db_path: Path,
    notify_telegram: bool,
) -> None:
    logging.getLogger(__name__).info(
        "Starting MOEX serve host=%s port=%s symbols=%s poll=%s",
        host,
        port,
        ",".join(symbols),
        poll_seconds,
    )
    service = _build_moex_service(
        symbols=symbols,
        history_days=history_days,
        news_limit=news_limit,
        news_window_hours=news_window_hours,
        poll_seconds=poll_seconds,
        db_path=db_path,
        notify_telegram=notify_telegram,
    )
    server = ApiServer(service, host, port)
    server.start()
    print(f"MOEX HTTP API listening on http://{host}:{port}")
    try:
        await service.run_forever()
    finally:
        server.stop()


async def _moex_longterm_scan(
    *,
    symbols: list[str],
    profiles: list[str],
    history_days: int,
    news_limit: int,
    news_window_hours: int,
    poll_seconds: int,
    max_open_positions: int,
    db_path: Path,
    notify_telegram: bool,
) -> None:
    logging.getLogger(__name__).info(
        "Starting MOEX long-term scan symbols=%s profiles=%s history_days=%s news_limit=%s window_hours=%s",
        ",".join(symbols),
        ",".join(profiles),
        history_days,
        news_limit,
        news_window_hours,
    )
    service = _build_moex_longterm_service(
        symbols=symbols,
        profiles=profiles,
        history_days=history_days,
        news_limit=news_limit,
        news_window_hours=news_window_hours,
        poll_seconds=poll_seconds,
        max_open_positions=max_open_positions,
        db_path=db_path,
        notify_telegram=notify_telegram,
    )
    signals = await service.poll_once()
    payload = [item.to_dict() for item in signals]
    print(json.dumps(payload, ensure_ascii=False, indent=2))


async def _moex_longterm_serve(
    *,
    host: str,
    port: int,
    symbols: list[str],
    profiles: list[str],
    history_days: int,
    news_limit: int,
    news_window_hours: int,
    poll_seconds: int,
    max_open_positions: int,
    db_path: Path,
    notify_telegram: bool,
) -> None:
    logging.getLogger(__name__).info(
        "Starting MOEX long-term serve host=%s port=%s symbols=%s profiles=%s poll=%s",
        host,
        port,
        ",".join(symbols),
        ",".join(profiles),
        poll_seconds,
    )
    service = _build_moex_longterm_service(
        symbols=symbols,
        profiles=profiles,
        history_days=history_days,
        news_limit=news_limit,
        news_window_hours=news_window_hours,
        poll_seconds=poll_seconds,
        max_open_positions=max_open_positions,
        db_path=db_path,
        notify_telegram=notify_telegram,
    )
    server = ApiServer(service, host, port)
    server.start()
    print(f"MOEX long-term HTTP API listening on http://{host}:{port}")
    try:
        await service.run_forever()
    finally:
        server.stop()


def _build_moex_service(
    *,
    symbols: list[str],
    history_days: int,
    news_limit: int,
    news_window_hours: int,
    poll_seconds: int,
    db_path: Path,
    notify_telegram: bool,
) -> MoexStockService:
    if not symbols:
        raise RuntimeError("At least one MOEX symbol is required")
    client = MoexApiClient()
    engine = MoexSignalEngine()
    repository = MoexSignalRepository(db_path)
    notifier = TelegramNotifier.from_env() if notify_telegram else None
    if notify_telegram and notifier is None:
        raise RuntimeError("TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID is not set")
    return MoexStockService(
        client,
        engine,
        symbols=symbols,
        repository=repository,
        notifier=notifier,
        poll_seconds=poll_seconds,
        history_days=history_days,
        news_limit=news_limit,
        news_window_hours=news_window_hours,
    )


def _build_moex_longterm_service(
    *,
    symbols: list[str],
    profiles: list[str],
    history_days: int,
    news_limit: int,
    news_window_hours: int,
    poll_seconds: int,
    max_open_positions: int,
    db_path: Path,
    notify_telegram: bool,
) -> MoexLongTermService:
    if not symbols:
        raise RuntimeError("At least one MOEX symbol is required")
    clean_profiles = [item for item in profiles if item in {"swing", "position"}]
    if not clean_profiles:
        raise RuntimeError("At least one profile is required: swing, position")
    client = MoexApiClient()
    engine = LongTermMoexEngine()
    repository = MoexSignalRepository(db_path)
    notifier = TelegramNotifier.from_env_longterm() if notify_telegram else None
    if notify_telegram and notifier is None:
        raise RuntimeError("TELEGRAM_LONGTERM_CHAT_ID (and bot token) is not set")
    return MoexLongTermService(
        client,
        engine,
        symbols=symbols,
        profiles=clean_profiles,
        repository=repository,
        notifier=notifier,
        poll_seconds=poll_seconds,
        history_days=history_days,
        news_limit=news_limit,
        news_window_hours=news_window_hours,
        max_open_positions=max_open_positions,
    )


def _split_csv(raw: str) -> list[str]:
    return [item.strip() for item in raw.split(",") if item.strip()]


def _load_dotenv(path: Path = Path(".env")) -> None:
    if not path.exists():
        return
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip("'").strip('"')
        if key and key not in os.environ:
            os.environ[key] = value


if __name__ == "__main__":
    main()

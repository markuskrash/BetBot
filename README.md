# BB Bet Signal

Free-football betting signal MVP built around bookmaker consensus. The project does not auto-place bets. It pulls football odds, compares a target bookmaker against the rest of the market, and flags value opportunities for a few simple markets.

## What it does

- fetches upcoming football events from Odds-API.io;
- requests odds for multiple bookmakers in one call;
- normalizes market margin and builds a consensus fair probability from comparison bookmakers;
- checks whether the target bookmaker offers a better price than the market consensus;
- computes `edge`, `expected value`, and a capped fractional-Kelly stake;
- stores every odds snapshot in local `SQLite`;
- exposes recommendations through CLI and HTTP.

## Supported markets

- `1x2`
- `totals_2_5`
- `btts`

## Why this version is free

This version is designed around free odds access instead of historical ML training. It can work without your own dataset because the “model” is the market consensus across bookmakers.

Official sources used for the integration:

- [Odds-API.io docs](https://docs.odds-api.io/)
- [Odds-API.io comparison example](https://docs.odds-api.io/examples/comparing-odds)

## Setup

1. Register for a free API key at [odds-api.io](https://odds-api.io/).
2. Put the key in `.env`:

```bash
cd /Users/markuskrash/Projects/BB
cp .env.example .env
```

Then edit `.env` and set:

```bash
ODDS_API_KEY="your_key"
TELEGRAM_BOT_TOKEN="your_bot_token"
TELEGRAM_CHAT_ID="your_chat_id"
```

You can also export the key manually if you prefer:

```bash
export ODDS_API_KEY="your_key"
```

## Run

Print a one-time scan:

```bash
cd /Users/markuskrash/Projects/BB
PYTHONPATH=src python3 -m bb_bet_signal.cli football-scan \
  --target-bookmaker Bet365 \
  --bookmakers Bet365,Unibet \
  --limit 10 \
  --notify-telegram \
  --log-file logs/football-scan.log
```

Run polling service with HTTP API:

```bash
cd /Users/markuskrash/Projects/BB
PYTHONPATH=src python3 -m bb_bet_signal.cli football-serve \
  --host 127.0.0.1 \
  --port 8080 \
  --target-bookmaker Bet365 \
  --bookmakers Bet365,Unibet \
  --poll-seconds 300 \
  --limit 10 \
  --notify-telegram \
  --log-file logs/football-serve.log
```

Then read:

```bash
curl http://127.0.0.1:8080/health
curl http://127.0.0.1:8080/recommendations
```

## Telegram bot

1. Create a bot in [@BotFather](https://t.me/BotFather) and copy the token.
2. Send any message to your bot from the Telegram account that should receive alerts.
3. Open this URL in a browser:

```text
https://api.telegram.org/bot<TELEGRAM_BOT_TOKEN>/getUpdates
```

4. Find `chat.id` in the JSON and put it into `TELEGRAM_CHAT_ID`.
5. Start `football-scan --notify-telegram` or `football-serve --notify-telegram`.

The notifier only sends new signals and suppresses duplicates with the same event, market, selection, bookmaker, and odds.

## Storage

Odds snapshots are stored in:

- `data/football_odds.sqlite3`

Logs are written to:

- `logs/football-scan.log`
- `logs/football-serve.log`

Table:

- `odds_snapshots`

## Local verification

```bash
cd /Users/markuskrash/Projects/BB
python3 -m compileall src tests
PYTHONPATH=src python3 -m unittest discover -s tests
PYTHONPATH=src python3 -m bb_bet_signal.cli simulate --ticks 1
```

## Limits

- This is not a guarantee of profit.
- Free API tiers have request limits, so polling is set up for low-frequency pre-match scanning.
- Some free plans allow only 2 selected bookmakers, so the default setup uses `Bet365` vs `Unibet`.
- The current engine is strongest for comparison/value detection, not deep predictive modeling.
- It does not automate bookmaker interaction.

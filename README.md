# Bot B - Bybit XRP Structure Zone Mean Reversion Bot

Bot B is an experimental Bybit XRP/USDT perpetual trading bot for hedge mode.
It places post-only limit entries around structure / volume-profile zones and uses reduce-only orders for exits.

## Safety Notice

This is live-trading code. It can lose money. Review the parameters, test with small size, and use a dedicated subaccount before running it.

## What Is Included

- `bot_b.py` - public GitHub-safe bot source
- `.env.example` - environment variable template
- `requirements.txt` - Python dependencies
- `.gitignore` - prevents keys, logs, state files, lock files, and sessions from being committed

## What Is Not Included

- No API keys
- No notification tokens or private account credentials
- No `.env`
- No state JSON
- No log files

## Setup

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
nano .env
```

Fill `.env`:

```env
BYBIT_API_KEY=your_real_key
BYBIT_SECRET_KEY=your_real_secret
```

## Run

```bash
source .venv/bin/activate
python -m py_compile bot_b.py
python -u bot_b.py
```

For VPS/tmux:

```bash
tmux new -s bot_b
cd /root/bot_b
source .venv/bin/activate
python -u bot_b.py >> bot_b.log 2>&1
```

Detach from tmux:

```text
Ctrl+B, then D
```

## Bybit Requirements

- USDT perpetual account
- Hedge mode
- API key with contract trading permission
- IP restriction is recommended
- Withdrawal permission should stay disabled

## Strategy Summary

- Symbol: `XRP/USDT:USDT`
- Timeframe: 5m primary
- Exchange: Bybit via `ccxt`
- Entry: post-only limit orders at structure / volume-profile zones
- DCA: limited staged entries at meaningful zones
- Exit: reduce-only limit orders
- Final risk handling: market reduce after full ladder and adverse movement criteria

## Disclaimer

This project is for research and automation development. It is not financial advice or a trading signal.

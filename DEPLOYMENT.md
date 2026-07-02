# Deployment Runbook — BTC Paper Traders

Operational notes for the two always-on paper-trading bots. This is infra/ops
only. **The strategy is frozen — see "Do not change" below.**

---

## What's running

Two Railway background workers loop forever, each fetching hourly BTC/USD data
and evaluating the volatility-breakout strategy once per hour.

| Service      | Script                     | Strategy            |
|--------------|----------------------------|---------------------|
| `trader-060` | `live/paper_trader.py`     | frozen, `vol_pctl=0.60` |
| `trader-050` | `live/paper_trader_050.py` | variant, `vol_pctl=0.50` |

Both are wrapped by `live/runner.py`, a thin supervisor that calls the script's
existing `_run_tick()` once per hour and survives transient errors. It does not
touch any trading logic.

---

## Who owns this (no confusion later)

- **Railway account:** `gabrielnejad-coder` (gabriel.nejad@gmail.com)
- **Workspace:** "gabrielnejad-coder's Projects"
- **Project:** `btc-paper-traders`
- **GitHub repo:** `gabrielnejad-coder/btc-deterministic-classifier`, branch `main`

> History note: an earlier **duplicate** project was accidentally created under a
> different Railway account (`king-bayethe` / bayethe.rowell@gmail.com, "vijilan's
> Projects"). That one is stale and should be deleted from that account. The
> account above (`gabrielnejad-coder`) is the correct/only one to use.

---

## Data source: keyless Kraken (NOT CryptoCompare)

Data comes from **Kraken's public OHLC API** — no API key, no auth, no
geo-restrictions:

```
https://api.kraken.com/0/public/OHLC?pair=XBTUSD&interval=60
```

It returns ~721 hourly candles, which is exactly the window the strategy's
volatility percentile uses (`rvol[-720:]`).

**Why we moved off CryptoCompare/CoinDesk:** the original scripts hit
CryptoCompare's free `histohour` endpoint. In **mid-2026** CoinDesk (which owns
CryptoCompare) **retired the free tier** — the endpoint now returns
`HTTP 401 "API key required"`. That silently killed the bot (last good signal
~2026-06-09). Rather than depend on a paid/uncertain key, `_fetch_ohlcv()` in
both scripts was swapped to keyless Kraken. The returned candle dict structure is
identical (`time, open, high, low, close, volume`), so **no strategy logic
changed**. Binance was considered but is geo-blocked (HTTP 451) from Railway's US
region; Kraken is not.

---

## The `/data` volume (REQUIRED on each service)

Each service **must** have a Railway Volume mounted at exactly **`/data`**.

- `runner.py` auto-detects `/data` and writes state there:
  - `trader-060` → `/data/state`, `/data/logs`
  - `trader-050` → `/data/state_050`, `/data/logs_050`
- **Why it matters:** Railway's container filesystem is wiped on every
  restart/redeploy. State (equity, open position, trade history) lives in
  `position.json` + the log files. **Without the `/data` volume, equity resets
  to $1000 on every restart.** With it, state persists.

If a service ever logs `no persistent volume found — EPHEMERAL`, its volume is
missing: in the Railway dashboard, open the service → add a Volume mounted at
`/data` (it redeploys automatically). A correct boot logs:

```
Mounting volume on: .../vol_...
[runner 060] state -> /data/state  logs -> /data/logs
```

---

## Start command / config (all in-repo, no dashboard config needed)

- `railway.json` sets `startCommand: python -u live/runner.py` and
  `restartPolicyType: ALWAYS` (stays up 24/7, auto-restarts on crash).
- `runner.py` self-selects the strategy from `RAILWAY_SERVICE_NAME`
  (name contains "050" → 0.50 variant, otherwise → 0.60). So service **names**
  matter: keep them `trader-060` and `trader-050`.
- `requirements.txt`: `numpy`, `requests`. `.python-version`: 3.12.

Any push to `main` auto-redeploys both services (state persists across the
redeploy thanks to the volume).

---

## Frozen strategy params — DO NOT CHANGE

These are validated and frozen. **This is a deployment/infra setup only; do not
alter thresholds or logic.**

| Param                 | trader-060 | trader-050 |
|-----------------------|-----------|-----------|
| `VOL_PCTL_ENTRY`      | **0.60**  | **0.50**  |
| `BRK_BARS`            | **12**    | **12**    |
| `HOLD_MIN_BARS`       | **3**     | **3**     |
| `STOP_LOSS_PCT`       | **0.025 (2.5%)** | **0.025 (2.5%)** |

(Other constants — `NOTIONAL=1000`, fees, slippage, lookback — are likewise
frozen.)

---

## How to check it's alive

Railway dashboard → project `btc-paper-traders` → open a service → **Logs**
(a.k.a. Observability). A healthy bot prints an hourly heartbeat at ~`:01` past
the hour:

```
[YYYY-MM-DDTHH:01:00...] Fetching data...
  close=$.....  vol_pctl=0...  breakout=False  12h_high=$....
  --- FLAT (no signal)
  equity=$....  trades=..  wins=..  wr=..%  pnl=$..  max_dd=..%
[runner 0X0] tick done; sleeping 3599s until next hourly candle
```

- **Alive** = you see that block repeating once per hour.
- The `equity=$...` line is the running paper P&L.
- A green check on the latest deployment = running; `ALWAYS` restart brings it
  back automatically if it ever dies.
- CLI alternative: `railway logs -s trader-060` (after `railway login && railway link`).

---

## Billing reality

Railway has **no true free always-on tier**. Two always-on workers cost roughly
**$3–5/month**. This project is on the **Hobby plan ($5/mo, includes $5 usage)** —
which is what keeps it running 24/7. **If the plan lapses / payment fails, the
services silently stop.** (Trial credit alone would run out in a few weeks.)

Status: **Hobby plan active.**

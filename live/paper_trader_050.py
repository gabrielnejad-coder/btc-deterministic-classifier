"""
PAPER TRADING MONITOR — Volatility Breakout Strategy
"""
from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone, timedelta
from pathlib import Path

import numpy as np
import requests

VOL_PCTL_ENTRY = 0.50
BRK_BARS = 12
HOLD_MIN_BARS = 3
STOP_LOSS_PCT = 0.025
NOTIONAL = 1000.0
FEE_TAKER = 0.0004
SLIPPAGE = 0.0001
LOOKBACK_HOURS = 800

STATE_DIR = Path("live/state_050")
LOG_DIR = Path("live/logs_050")
TRADES_LOG = LOG_DIR / "trades.jsonl"
SIGNALS_LOG = LOG_DIR / "signals.jsonl"
STATE_FILE = STATE_DIR / "position.json"


def _fetch_ohlcv(limit=LOOKBACK_HOURS):
    # Keyless data adapter: Kraken public OHLC, hourly BTC/USD. No API key, no
    # auth, no geo-restrictions. Returns candle dicts with the SAME structure the
    # rest of this script consumes (keys: time [unix seconds], open, high, low,
    # close, volume) so no strategy logic changes. Kraken returns up to ~720
    # hourly bars, which is exactly the window the volatility percentile uses.
    url = "https://api.kraken.com/0/public/OHLC"
    resp = requests.get(url, params={"pair": "XBTUSD", "interval": 60}, timeout=30)
    data = resp.json()
    if data.get("error"):
        raise RuntimeError(f"Kraken error: {data['error']}")
    result = data["result"]
    pair_key = next(k for k in result if k != "last")
    rows = result[pair_key]
    candles = [
        {
            "time": int(r[0]),
            "open": float(r[1]),
            "high": float(r[2]),
            "low": float(r[3]),
            "close": float(r[4]),
            "volume": float(r[6]),
        }
        for r in rows
    ]
    return candles[-limit:]


def _compute_indicators(candles):
    n = len(candles)
    close = np.array([c["close"] for c in candles], dtype=float)
    high = np.array([c["high"] for c in candles], dtype=float)
    ret_1h = np.zeros(n)
    ret_1h[1:] = (close[1:] - close[:-1]) / np.maximum(close[:-1], 1e-10)
    rvol = np.full(n, np.nan)
    for i in range(24, n):
        rvol[i] = np.std(ret_1h[i-24:i])
    vol_pctl = np.nan
    if n >= 720 and not np.isnan(rvol[-1]):
        window = rvol[-720:]
        valid = window[~np.isnan(window)]
        if len(valid) > 10:
            vol_pctl = np.searchsorted(np.sort(valid), rvol[-1]) / len(valid)
    breakout = False
    if n >= BRK_BARS + 1:
        breakout = bool(close[-1] > np.max(high[-BRK_BARS-1:-1]))
    return {
        "ts": datetime.fromtimestamp(candles[-1]["time"], tz=timezone.utc).isoformat(),
        "close": float(close[-1]),
        "high": float(high[-1]),
        "vol_pctl": float(vol_pctl) if not np.isnan(vol_pctl) else None,
        "rvol_24h": float(rvol[-1]) if not np.isnan(rvol[-1]) else None,
        "breakout": breakout,
        "prev_12h_high": float(np.max(high[-BRK_BARS-1:-1])) if n >= BRK_BARS + 1 else None,
    }


def _load_state():
    if STATE_FILE.exists():
        return json.loads(STATE_FILE.read_text())
    return {"in_position": False, "entry_price": None, "entry_ts": None, "bars_held": 0,
            "equity": NOTIONAL, "total_trades": 0, "total_wins": 0, "total_pnl": 0.0,
            "peak_equity": NOTIONAL, "max_drawdown": 0.0}


def _save_state(state):
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    STATE_FILE.write_text(json.dumps(state, indent=2, default=str) + "\n")


def _log(path, entry):
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    with open(path, "a") as f:
        f.write(json.dumps(entry, default=str) + "\n")


def _run_tick():
    state = _load_state()
    print(f"\n[{datetime.now(timezone.utc).isoformat()}] Fetching data...")
    candles = _fetch_ohlcv()
    indicators = _compute_indicators(candles)
    vp = indicators['vol_pctl']
    vp_str = f"{vp:.3f}" if vp is not None else "None"
    print(f"  close=${indicators['close']:.2f}  vol_pctl={vp_str}  "
          f"breakout={indicators['breakout']}  12h_high=${indicators.get('prev_12h_high', 0):.2f}")
    signal = "flat"
    if indicators["vol_pctl"] is not None:
        if indicators["vol_pctl"] >= VOL_PCTL_ENTRY and indicators["breakout"]:
            signal = "up"
    _log(SIGNALS_LOG, {"ts": indicators["ts"], "close": indicators["close"],
         "vol_pctl": indicators["vol_pctl"], "breakout": indicators["breakout"],
         "signal": signal, "in_position": state["in_position"]})
    now_ts = indicators["ts"]
    price = indicators["close"]
    if state["in_position"]:
        state["bars_held"] += 1
        entry_p = state["entry_price"]
        pnl_pct = (price - entry_p) / entry_p
        stop_hit = pnl_pct <= -STOP_LOSS_PCT
        should_exit = stop_hit or (state["bars_held"] >= HOLD_MIN_BARS and signal != "up")
        if should_exit:
            exit_price = price * (1 - SLIPPAGE)
            gross_pnl = (exit_price / entry_p - 1) * state["equity"]
            fees = state["equity"] * FEE_TAKER
            net_pnl = gross_pnl - fees
            state["equity"] += net_pnl
            state["total_pnl"] += net_pnl
            state["total_trades"] += 1
            if net_pnl > 0:
                state["total_wins"] += 1
            state["peak_equity"] = max(state["peak_equity"], state["equity"])
            dd = (state["peak_equity"] - state["equity"]) / state["peak_equity"]
            state["max_drawdown"] = max(state["max_drawdown"], dd)
            reason = "STOP" if stop_hit else "EXIT"
            print(f"  >>> {reason}: held {state['bars_held']} bars, PnL=${net_pnl:.2f}, equity=${state['equity']:.2f}")
            _log(TRADES_LOG, {"action": "close", "reason": reason, "entry_ts": state["entry_ts"],
                 "exit_ts": now_ts, "entry_price": entry_p, "exit_price": price,
                 "bars_held": state["bars_held"], "gross_pnl": round(gross_pnl, 2),
                 "net_pnl": round(net_pnl, 2), "equity_after": round(state["equity"], 2)})
            state["in_position"] = False
            state["entry_price"] = None
            state["entry_ts"] = None
            state["bars_held"] = 0
        else:
            print(f"  --- HOLDING: bar {state['bars_held']}, unrealized {pnl_pct:+.2%}")
    elif signal == "up":
        entry_price = price * (1 + SLIPPAGE)
        entry_fee = state["equity"] * FEE_TAKER
        state["in_position"] = True
        state["entry_price"] = entry_price
        state["entry_ts"] = now_ts
        state["bars_held"] = 0
        state["equity"] -= entry_fee
        print(f"  >>> ENTRY at ${entry_price:.2f} (slip from ${price:.2f})")
        _log(TRADES_LOG, {"action": "open", "ts": now_ts, "signal_price": price,
             "entry_price": entry_price, "equity_before": round(state["equity"] + entry_fee, 2)})
    else:
        print(f"  --- FLAT (no signal)")
    wr = state["total_wins"] / state["total_trades"] if state["total_trades"] > 0 else 0
    print(f"  equity=${state['equity']:.2f}  trades={state['total_trades']}  "
          f"wins={state['total_wins']}  wr={wr:.1%}  pnl=${state['total_pnl']:.2f}  "
          f"max_dd={state['max_drawdown']:.2%}")
    _save_state(state)


def _show_status():
    state = _load_state()
    print(f"\nPAPER TRADER STATUS")
    print(f"  Equity:       ${state['equity']:.2f}")
    print(f"  In position:  {state['in_position']}")
    if state["in_position"]:
        print(f"  Entry price:  ${state['entry_price']:.2f}")
        print(f"  Entry time:   {state['entry_ts']}")
        print(f"  Bars held:    {state['bars_held']}")
    print(f"  Total trades: {state['total_trades']}")
    print(f"  Wins:         {state['total_wins']}")
    wr = state["total_wins"] / state["total_trades"] if state["total_trades"] > 0 else 0
    print(f"  Win rate:     {wr:.1%}")
    print(f"  Total PnL:    ${state['total_pnl']:.2f}")
    print(f"  Max drawdown: {state['max_drawdown']:.2%}")
    if SIGNALS_LOG.exists():
        lines = SIGNALS_LOG.read_text().strip().split("\n")
        recent = [json.loads(l) for l in lines[-10:]]
        print(f"\n  Last {len(recent)} signals:")
        for s in recent:
            marker = ">>>" if s["signal"] == "up" else "   "
            vp = s.get('vol_pctl', 0)
            vp_str = f"{vp:.3f}" if vp else "0.000"
            print(f"    {marker} {s['ts']}  close=${s['close']:.0f}  vol={vp_str}  brk={s['breakout']}  sig={s['signal']}")
    if TRADES_LOG.exists():
        lines = TRADES_LOG.read_text().strip().split("\n")
        trades = [json.loads(l) for l in lines if '"close"' in l][-10:]
        if trades:
            print(f"\n  Last {len(trades)} closed trades:")
            for t in trades:
                print(f"    {t['entry_ts']} -> {t['exit_ts']}  ${t['entry_price']:.0f}->${t['exit_price']:.0f}  pnl=${t['net_pnl']:.2f}  {t['reason']}")


def main():
    parser = argparse.ArgumentParser(description="Paper trading monitor")
    parser.add_argument("--status", action="store_true")
    parser.add_argument("--export", action="store_true")
    args = parser.parse_args()
    if args.status:
        _show_status()
    elif args.export:
        state = _load_state()
        print(f"equity=${state['equity']:.2f}  trades={state['total_trades']}  pnl=${state['total_pnl']:.2f}")
    else:
        _run_tick()


if __name__ == "__main__":
    main()

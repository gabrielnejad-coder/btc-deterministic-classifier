"""
VOLATILITY BREAKOUT REGIME STRATEGY — v2 (fixed)

Changes from v1:
  - Precompute all indicators on full dataset (fixes lookback issue)
  - Val window: 3 months (enough events for param sweep)
  - Min trades on val: 1 (breakouts are inherently rare)
  - Simplified: no position tracking in signal gen (engine handles that)
"""
from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd

from backtest.engine import EngineConfig, run_engine


REPORTS_DIR = Path("reports/vol_breakout_v2")
DATA_DIR = Path("data_parquet")

ENGINE_CFG = EngineConfig(
    fee_taker=0.0004,
    slippage_side=0.0001,
    stop_loss_pct=0.02,
    hold_min_bars=24,
    initial_equity=1000.0,
    one_position=True,
)

MAX_DD = 0.10

PARAM_GRID = []
for vol_pctl in [0.60, 0.65, 0.70, 0.75]:
    for breakout_bars in [12, 24, 48]:
        PARAM_GRID.append({
            "vol_pctl_entry": vol_pctl,
            "breakout_bars": breakout_bars,
        })


def _precompute_indicators(btc: pd.DataFrame) -> pd.DataFrame:
    """Compute vol percentile and breakout flags on FULL dataset."""
    df = btc.copy()
    df["ts"] = pd.to_datetime(df["ts"], utc=True)
    df = df.sort_values("ts").reset_index(drop=True)

    close = df["close"].astype(float).values
    high = df["high"].astype(float).values
    n = len(close)

    # 1h returns
    ret_1h = np.zeros(n)
    ret_1h[1:] = (close[1:] - close[:-1]) / np.maximum(close[:-1], 1e-10)

    # 24h realized vol
    rvol = np.full(n, np.nan)
    for i in range(24, n):
        rvol[i] = np.std(ret_1h[i-24:i])

    # Rolling percentile vs 720 bars
    vol_pctl = np.full(n, np.nan)
    for i in range(720, n):
        w = rvol[i-720:i+1]
        valid = w[~np.isnan(w)]
        if len(valid) > 10:
            vol_pctl[i] = np.searchsorted(np.sort(valid), rvol[i]) / len(valid)

    # Breakout flags for each lookback period
    for bb in [12, 24, 48]:
        brk = np.full(n, False)
        for i in range(bb, n):
            brk[i] = close[i] > np.max(high[i-bb:i])
        df[f"breakout_{bb}"] = brk

    df["vol_pctl"] = vol_pctl

    return df


def _get_signals(indicators: pd.DataFrame, start, end, params) -> pd.Series:
    """Extract signals for a time window using precomputed indicators."""
    mask = (indicators["ts"] >= start) & (indicators["ts"] < end)
    chunk = indicators.loc[mask].copy()

    if len(chunk) == 0:
        return pd.Series(dtype="object")

    vp = params["vol_pctl_entry"]
    bb = params["breakout_bars"]
    brk_col = f"breakout_{bb}"

    entry = (chunk["vol_pctl"] >= vp) & chunk[brk_col]
    signals = np.where(entry, "up", "flat")

    idx = pd.DatetimeIndex(chunk["ts"].values, name="ts")
    return pd.Series(signals, index=idx, dtype="object")


def _months_range(start_ts, end_ts):
    cursor = start_ts + pd.DateOffset(months=12)
    while cursor < end_ts:
        w = {
            "val_start": cursor - pd.DateOffset(months=3),
            "val_end": cursor,
            "test_start": cursor,
            "test_end": cursor + pd.DateOffset(months=1),
        }
        if w["test_start"] >= end_ts:
            break
        w["test_end"] = min(w["test_end"], end_ts)
        yield w
        cursor += pd.DateOffset(months=1)


def _slice(df, s, e):
    return df[(df["ts"] >= s) & (df["ts"] < e)].copy().reset_index(drop=True)


def main() -> None:
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    btc = pd.read_parquet(DATA_DIR / "BTCUSD_USD_1h_20220323_now.parquet").copy()
    btc["ts"] = pd.to_datetime(btc["ts"], utc=True)
    btc = btc.sort_values("ts").reset_index(drop=True)

    print("Precomputing indicators on full dataset...")
    indicators = _precompute_indicators(btc)
    print(f"  bars: {len(indicators)}")
    print(f"  vol_pctl valid: {(~np.isnan(indicators['vol_pctl'].values)).sum()}")

    windows = list(_months_range(btc["ts"].min(), btc["ts"].max()))

    print(f"\nVOLATILITY BREAKOUT v2")
    print(f"  param grid: {len(PARAM_GRID)} configs")
    print(f"  windows: {len(windows)}")
    print(f"  validate: 3 months, min_trades=1")
    print(f"  test: 1 month")

    results = []
    for i, w in enumerate(windows):
        val_bars = _slice(btc, w["val_start"], w["val_end"])
        test_bars = _slice(btc, w["test_start"], w["test_end"])

        if len(val_bars) < 100 or len(test_bars) < 50:
            results.append({"window": i, "status": "skip",
                            "test_start": str(w["test_start"].date()),
                            "test_end": str(w["test_end"].date())})
            continue

        # Sweep on validate
        best_params = None
        best_eq = 0

        for params in PARAM_GRID:
            sig = _get_signals(indicators, w["val_start"], w["val_end"], params)
            if sig.empty or (sig == "up").sum() == 0:
                continue
            _, _, m = run_engine(val_bars, sig, ENGINE_CFG)
            if m["max_drawdown"] <= MAX_DD and m["num_trades"] >= 1:
                if m["final_equity"] > best_eq:
                    best_eq = m["final_equity"]
                    best_params = params.copy()

        if best_params is None:
            results.append({"window": i, "status": "no_params",
                            "test_start": str(w["test_start"].date()),
                            "test_end": str(w["test_end"].date())})
            continue

        # Test
        sig_test = _get_signals(indicators, w["test_start"], w["test_end"], best_params)
        if sig_test.empty:
            results.append({"window": i, "status": "skip",
                            "test_start": str(w["test_start"].date()),
                            "test_end": str(w["test_end"].date())})
            continue

        _, _, met = run_engine(test_bars, sig_test, ENGINE_CFG)

        n_up = int((sig_test == "up").sum())

        # Baseline
        us = pd.Series("up", index=pd.DatetimeIndex(
            pd.to_datetime(test_bars["ts"].values, utc=True)), dtype="object")
        _, _, bm = run_engine(test_bars, us, ENGINE_CFG)

        dok = met["max_drawdown"] <= MAX_DD
        beats = met["final_equity"] > bm["final_equity"]
        real = met["num_trades"] >= 1
        status = "PASS" if (dok and beats and real) else ("HOLLOW" if (dok and met["num_trades"] == 0) else "FAIL")

        results.append({
            "window": i,
            "test_start": str(w["test_start"].date()),
            "test_end": str(w["test_end"].date()),
            "params": best_params,
            "equity": met["final_equity"],
            "ret": met["total_return"],
            "dd": met["max_drawdown"],
            "trades": met["num_trades"],
            "wins": met["num_wins"],
            "signals_up": n_up,
            "baseline_eq": bm["final_equity"],
            "status": status,
        })

    # ── Summary ──────────────────────────────────────────────────────
    print(f"\n{'='*90}")
    print(f"  {'W':>3s} {'period':>25s} {'vol':>5s} {'brk':>4s} {'eq':>8s} {'ret':>8s} {'dd':>6s} {'tr':>4s} {'w':>3s} {'sigs':>5s} {'base':>8s} {'gate':>8s}")
    print(f"  {'-'*3} {'-'*25} {'-'*5} {'-'*4} {'-'*8} {'-'*8} {'-'*6} {'-'*4} {'-'*3} {'-'*5} {'-'*8} {'-'*8}")

    np_ = nf = nh = ns = 0
    pr = []
    ar = []

    for r in results:
        if "equity" not in r:
            ns += 1
            ts_ = r.get("test_start", "?")
            te = r.get("test_end", "?")
            st = r.get("status", "skip")
            print(f"  {r['window']:>3d} {(ts_+'->'+te):>25s}   ---  ---      ---      ---    ---  ---  ---   ---      --- {st:>8s}")
        else:
            p = f"{r['test_start']}->{r['test_end']}"
            vp = r["params"]["vol_pctl_entry"]
            bb = r["params"]["breakout_bars"]
            print(f"  {r['window']:>3d} {p:>25s} {vp:>5.2f} {bb:>4d} "
                  f"${r['equity']:>7.0f} ${r['ret']:>7.0f} {r['dd']:>5.3f} "
                  f"{r['trades']:>4d} {r['wins']:>3d} {r['signals_up']:>5d} ${r['baseline_eq']:>7.0f} {r['status']:>8s}")
            ar.append(r["ret"])
            if r["status"] == "PASS":
                np_ += 1
                pr.append(r["ret"])
            elif r["status"] == "HOLLOW":
                nh += 1
            else:
                nf += 1

    avg_p = np.mean(pr) if pr else 0
    avg_a = np.mean(ar) if ar else 0
    total = sum(ar)

    total_trades = sum(r.get("trades", 0) for r in results if "trades" in r)
    total_wins = sum(r.get("wins", 0) for r in results if "wins" in r)
    win_rate = total_wins / total_trades if total_trades > 0 else 0
    avg_per_trade = total / total_trades if total_trades > 0 else 0

    print(f"\n  PASS: {np_}  HOLLOW: {nh}  FAIL: {nf}  SKIP: {ns}")
    print(f"  avg PASS ret: ${avg_p:.2f}  avg ALL ret: ${avg_a:.2f}  total P&L: ${total:.2f}")
    print(f"  trades: {total_trades}  wins: {total_wins}  rate: {win_rate:.1%}  avg/trade: ${avg_per_trade:.2f}")

    print(f"\n  COMPARISON:")
    print(f"    24h GBDT wide:     PASS=19/34  total=$-412  avg/trade~$-1.85")
    print(f"    Vol breakout v2:   PASS={np_}/{len(windows)}  total=${total:.0f}  avg/trade=${avg_per_trade:.2f}")
    print(f"{'='*90}")

    Path(REPORTS_DIR / "results.json").write_text(json.dumps(results, indent=2, default=str) + "\n")
    print(f"  WROTE {REPORTS_DIR}/results.json")


if __name__ == "__main__":
    main()

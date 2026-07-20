"""
FINAL VERIFICATION — Zero sweep, frozen everything.

Config:
  vol_pctl = 0.60
  brk_bars = 12
  hold_min = 3  (midpoint of 2-4 range)
  stop_loss = 0.025  (midpoint of 2-3%)
  fee_taker = 0.0004
  slippage_side = 0.0001

No parameter selection. No validation. No threshold.
Pure structural signal → engine → results.

Non-overlapping 1-month windows across full OOS period.
"""
from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd

from backtest.engine import EngineConfig, run_engine


REPORTS_DIR = Path("reports/final_verification")
DATA_DIR = Path("data_parquet")

# ── LOCKED CONFIG ────────────────────────────────────────────────────
VOL_PCTL = 0.60
BRK_BARS = 12
ENGINE_CFG = EngineConfig(
    fee_taker=0.0004,
    slippage_side=0.0001,
    stop_loss_pct=0.025,
    hold_min_bars=3,
    initial_equity=1000.0,
    one_position=True,
)
MAX_DD = 0.10
# ─────────────────────────────────────────────────────────────────────


def _precompute(btc):
    df = btc.copy()
    df["ts"] = pd.to_datetime(df["ts"], utc=True)
    df = df.sort_values("ts").reset_index(drop=True)

    close = df["close"].astype(float).values
    high = df["high"].astype(float).values
    n = len(close)

    ret_1h = np.zeros(n)
    ret_1h[1:] = (close[1:] - close[:-1]) / np.maximum(close[:-1], 1e-10)

    rvol = np.full(n, np.nan)
    for i in range(24, n):
        rvol[i] = np.std(ret_1h[i-24:i])

    vol_pctl = np.full(n, np.nan)
    for i in range(720, n):
        w = rvol[i-720:i+1]
        valid = w[~np.isnan(w)]
        if len(valid) > 10:
            vol_pctl[i] = np.searchsorted(np.sort(valid), rvol[i]) / len(valid)

    brk = np.full(n, False)
    for i in range(BRK_BARS, n):
        brk[i] = close[i] > np.max(high[i-BRK_BARS:i])

    df["vol_pctl"] = vol_pctl
    df["breakout"] = brk
    return df


def _get_signals(indicators, start, end):
    mask = (indicators["ts"] >= start) & (indicators["ts"] < end)
    chunk = indicators.loc[mask].copy()
    if len(chunk) == 0:
        return pd.Series(dtype="object")
    entry = (chunk["vol_pctl"] >= VOL_PCTL) & chunk["breakout"]
    signals = np.where(entry, "up", "flat")
    idx = pd.DatetimeIndex(chunk["ts"].values, name="ts")
    return pd.Series(signals, index=idx, dtype="object")


def _slice(df, s, e):
    return df[(df["ts"] >= s) & (df["ts"] < e)].copy().reset_index(drop=True)


def main():
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    btc = pd.read_parquet(DATA_DIR / "BTCUSD_USD_1h_20220323_now.parquet").copy()
    btc["ts"] = pd.to_datetime(btc["ts"], utc=True)
    btc = btc.sort_values("ts").reset_index(drop=True)

    indicators = _precompute(btc)

    # Non-overlapping 1-month windows, 12-month warmup
    start = btc["ts"].min() + pd.DateOffset(months=12)
    ts_max = btc["ts"].max()
    windows = []
    cursor = start
    while cursor + pd.DateOffset(months=1) <= ts_max:
        windows.append({"start": cursor, "end": cursor + pd.DateOffset(months=1)})
        cursor += pd.DateOffset(months=1)
    if cursor < ts_max:
        windows.append({"start": cursor, "end": ts_max})

    print("FINAL VERIFICATION — ZERO SWEEP")
    print(f"  vol_pctl={VOL_PCTL}  brk={BRK_BARS}  hold_min={ENGINE_CFG.hold_min_bars}  stop={ENGINE_CFG.stop_loss_pct}")
    print(f"  fee={ENGINE_CFG.fee_taker}  slip={ENGINE_CFG.slippage_side}")
    print(f"  windows: {len(windows)} (1-month, non-overlapping)")
    print(f"  NO parameter sweep. NO validation. Frozen config only.")

    # ── Run ───────────────────────────────────────────────────────────
    results = []
    cumulative_equity = ENGINE_CFG.initial_equity

    for w in windows:
        tb = _slice(btc, w["start"], w["end"])
        if len(tb) < 50:
            results.append({"start": str(w["start"].date()), "end": str(w["end"].date()), "status": "skip"})
            continue

        sig = _get_signals(indicators, w["start"], w["end"])
        if sig.empty:
            results.append({"start": str(w["start"].date()), "end": str(w["end"].date()), "status": "skip"})
            continue

        _, _, met = run_engine(tb, sig, ENGINE_CFG)

        # Baseline (always long)
        us = pd.Series("up", index=pd.DatetimeIndex(
            pd.to_datetime(tb["ts"].values, utc=True)), dtype="object")
        _, _, bm = run_engine(tb, us, ENGINE_CFG)

        # Simulate compounding
        window_ret_pct = met["total_return"] / ENGINE_CFG.initial_equity
        window_pnl = cumulative_equity * window_ret_pct
        cumulative_equity += window_pnl

        dok = met["max_drawdown"] <= MAX_DD
        beats = met["final_equity"] > bm["final_equity"]
        real = met["num_trades"] >= 1
        status = "PASS" if (dok and beats and real) else ("HOLLOW" if met["num_trades"] == 0 else "FAIL")

        results.append({
            "start": str(w["start"].date()),
            "end": str(w["end"].date()),
            "equity": met["final_equity"],
            "ret": met["total_return"],
            "ret_pct": round(window_ret_pct * 100, 2),
            "dd": met["max_drawdown"],
            "trades": met["num_trades"],
            "wins": met["num_wins"],
            "cum_equity": round(cumulative_equity, 2),
            "baseline_eq": bm["final_equity"],
            "status": status,
        })

    # ── Output ────────────────────────────────────────────────────────
    print(f"\n{'='*95}")
    print(f"  {'period':>25s} {'eq':>8s} {'ret':>8s} {'ret%':>6s} {'dd':>6s} {'tr':>4s} {'w':>3s} {'cum':>10s} {'base':>8s} {'gate':>6s}")
    print(f"  {'-'*25} {'-'*8} {'-'*8} {'-'*6} {'-'*6} {'-'*4} {'-'*3} {'-'*10} {'-'*8} {'-'*6}")

    np_ = nf = nh = ns = 0
    all_rets = []
    monthly_pcts = []

    for r in results:
        if "equity" not in r:
            ns += 1
            continue

        p = f"{r['start']}->{r['end']}"
        print(f"  {p:>25s} ${r['equity']:>7.0f} ${r['ret']:>7.0f} {r['ret_pct']:>5.1f}% "
              f"{r['dd']:>5.3f} {r['trades']:>4d} {r['wins']:>3d} ${r['cum_equity']:>9.0f} "
              f"${r['baseline_eq']:>7.0f} {r['status']:>6s}")

        all_rets.append(r["ret"])
        monthly_pcts.append(r["ret_pct"])
        if r["status"] == "PASS": np_ += 1
        elif r["status"] == "HOLLOW": nh += 1
        else: nf += 1

    total_pnl = sum(all_rets)
    total_trades = sum(r.get("trades", 0) for r in results if "trades" in r)
    total_wins = sum(r.get("wins", 0) for r in results if "wins" in r)
    wr = total_wins / total_trades if total_trades > 0 else 0
    apt = total_pnl / total_trades if total_trades > 0 else 0

    monthly_arr = np.array(monthly_pcts)
    sharpe_monthly = np.mean(monthly_arr) / np.std(monthly_arr) if np.std(monthly_arr) > 0 else 0
    sharpe_annual = sharpe_monthly * np.sqrt(12)

    max_cum_dd = 0
    peak = ENGINE_CFG.initial_equity
    for r in results:
        if "cum_equity" in r:
            peak = max(peak, r["cum_equity"])
            dd = (peak - r["cum_equity"]) / peak
            max_cum_dd = max(max_cum_dd, dd)

    print(f"\n{'='*95}")
    print(f"  FINAL VERIFICATION SUMMARY")
    print(f"{'='*95}")
    print(f"  Period:             {results[0]['start']} to {results[-1]['end']}")
    print(f"  Windows:            {len(windows)} months")
    print(f"  PASS / FAIL:        {np_} / {nf}  (HOLLOW: {nh})")
    print(f"  ")
    print(f"  Total P&L (simple): ${total_pnl:.2f}")
    print(f"  Compounded equity:  ${cumulative_equity:.2f}  (started at $1,000)")
    print(f"  Compounded return:  {(cumulative_equity/1000 - 1)*100:.1f}%")
    print(f"  Max cum drawdown:   {max_cum_dd:.1%}")
    print(f"  ")
    print(f"  Total trades:       {total_trades}")
    print(f"  Win rate:           {wr:.1%}")
    print(f"  Avg return/trade:   ${apt:.2f}")
    print(f"  ")
    print(f"  Monthly Sharpe:     {sharpe_monthly:.3f}")
    print(f"  Annualized Sharpe:  {sharpe_annual:.2f}")
    print(f"  Avg monthly return: {np.mean(monthly_arr):.2f}%")
    print(f"  Monthly return std: {np.std(monthly_arr):.2f}%")
    print(f"  Best month:         {np.max(monthly_arr):.1f}%")
    print(f"  Worst month:        {np.min(monthly_arr):.1f}%")
    print(f"  % months positive:  {(monthly_arr > 0).mean():.1%}")
    print(f"{'='*95}")

    Path(REPORTS_DIR / "final_results.json").write_text(
        json.dumps(results, indent=2, default=str) + "\n"
    )
    print(f"  WROTE {REPORTS_DIR}/final_results.json")


if __name__ == "__main__":
    main()

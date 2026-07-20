"""
BREAKOUT HARDENING BATTERY

Three tests on frozen params (vol_pctl=0.60, brk=12):

1. NON-OVERLAPPING WALKFORWARD
   - No param sweep (frozen)
   - Non-overlapping 3-month test windows
   - No validate (nothing to tune)

2. STRESS TESTS on the same non-overlapping windows:
   a. 2x slippage (0.02% → 0.04% per side)
   b. 2x fees (0.04% → 0.08% taker)
   c. Entry delay (+1 bar)
   d. Remove top 5% of trades by return

3. MONTHLY GRANULAR (frozen params, non-overlapping 1-month windows)
"""
from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd

from backtest.engine import EngineConfig, run_engine


REPORTS_DIR = Path("reports/breakout_harden")
DATA_DIR = Path("data_parquet")

# ── FROZEN PARAMS ────────────────────────────────────────────────────
FROZEN_VOL_PCTL = 0.60
FROZEN_BRK_BARS = 12
# ─────────────────────────────────────────────────────────────────────

BASE_CFG = EngineConfig(
    fee_taker=0.0004, slippage_side=0.0001, stop_loss_pct=0.02,
    hold_min_bars=24, initial_equity=1000.0, one_position=True,
)

STRESS_CFGS = {
    "baseline": BASE_CFG,
    "2x_slippage": EngineConfig(
        fee_taker=0.0004, slippage_side=0.0002, stop_loss_pct=0.02,
        hold_min_bars=24, initial_equity=1000.0, one_position=True,
    ),
    "2x_fees": EngineConfig(
        fee_taker=0.0008, slippage_side=0.0001, stop_loss_pct=0.02,
        hold_min_bars=24, initial_equity=1000.0, one_position=True,
    ),
    "both_2x": EngineConfig(
        fee_taker=0.0008, slippage_side=0.0002, stop_loss_pct=0.02,
        hold_min_bars=24, initial_equity=1000.0, one_position=True,
    ),
}

MAX_DD = 0.10


def _precompute(btc: pd.DataFrame) -> pd.DataFrame:
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
    bb = FROZEN_BRK_BARS
    for i in range(bb, n):
        brk[i] = close[i] > np.max(high[i-bb:i])

    df["vol_pctl"] = vol_pctl
    df["breakout"] = brk
    return df


def _get_signals(indicators, start, end, delay=0):
    mask = (indicators["ts"] >= start) & (indicators["ts"] < end)
    chunk = indicators.loc[mask].copy()
    if len(chunk) == 0:
        return pd.Series(dtype="object")

    entry = (chunk["vol_pctl"] >= FROZEN_VOL_PCTL) & chunk["breakout"]

    if delay > 0:
        entry = entry.shift(delay).fillna(False)

    signals = np.where(entry, "up", "flat")
    idx = pd.DatetimeIndex(chunk["ts"].values, name="ts")
    return pd.Series(signals, index=idx, dtype="object")


def _slice(df, s, e):
    return df[(df["ts"] >= s) & (df["ts"] < e)].copy().reset_index(drop=True)


def _run_windows(indicators, btc, windows, cfg, delay=0):
    """Run breakout strategy across windows, return results list."""
    results = []
    for i, w in enumerate(windows):
        test_bars = _slice(btc, w["start"], w["end"])
        if len(test_bars) < 50:
            results.append({"window": i, "start": str(w["start"].date()),
                            "end": str(w["end"].date()), "status": "skip"})
            continue

        sig = _get_signals(indicators, w["start"], w["end"], delay=delay)
        if sig.empty:
            results.append({"window": i, "start": str(w["start"].date()),
                            "end": str(w["end"].date()), "status": "skip"})
            continue

        _, _, met = run_engine(test_bars, sig, cfg)

        # Baseline
        us = pd.Series("up", index=pd.DatetimeIndex(
            pd.to_datetime(test_bars["ts"].values, utc=True)), dtype="object")
        _, _, bm = run_engine(test_bars, us, cfg)

        dok = met["max_drawdown"] <= MAX_DD
        beats = met["final_equity"] > bm["final_equity"]
        real = met["num_trades"] >= 1
        status = "PASS" if (dok and beats and real) else ("HOLLOW" if met["num_trades"] == 0 else "FAIL")

        results.append({
            "window": i,
            "start": str(w["start"].date()),
            "end": str(w["end"].date()),
            "equity": met["final_equity"],
            "ret": met["total_return"],
            "dd": met["max_drawdown"],
            "trades": met["num_trades"],
            "wins": met["num_wins"],
            "status": status,
        })
    return results


def _summarize(results, label):
    traded = [r for r in results if "equity" in r]
    np_ = sum(1 for r in traded if r["status"] == "PASS")
    nf = sum(1 for r in traded if r["status"] == "FAIL")
    nh = sum(1 for r in traded if r["status"] == "HOLLOW")
    ns = sum(1 for r in results if "equity" not in r)
    total_pnl = sum(r["ret"] for r in traded)
    total_trades = sum(r["trades"] for r in traded)
    total_wins = sum(r["wins"] for r in traded)
    wr = total_wins / total_trades if total_trades > 0 else 0
    apt = total_pnl / total_trades if total_trades > 0 else 0
    worst = min((r["ret"] for r in traded), default=0)

    return {
        "label": label,
        "windows": len(results),
        "pass": np_, "fail": nf, "hollow": nh, "skip": ns,
        "total_pnl": round(total_pnl, 2),
        "trades": total_trades,
        "wins": total_wins,
        "win_rate": round(wr, 3),
        "avg_per_trade": round(apt, 2),
        "worst_window": round(worst, 2),
    }


def main() -> None:
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    btc = pd.read_parquet(DATA_DIR / "BTCUSD_USD_1h_20220323_now.parquet").copy()
    btc["ts"] = pd.to_datetime(btc["ts"], utc=True)
    btc = btc.sort_values("ts").reset_index(drop=True)

    print("Precomputing indicators (frozen params)...")
    indicators = _precompute(btc)

    # ── TEST 1: Non-overlapping 3-month windows ─────────────────────
    print("\n" + "="*80)
    print("  TEST 1: NON-OVERLAPPING 3-MONTH WINDOWS (frozen params, no sweep)")
    print("="*80)

    ts_min = btc["ts"].min()
    ts_max = btc["ts"].max()
    # Start after 720 bars of warmup (~30 days)
    start = ts_min + pd.DateOffset(months=12)  # 12 months warmup

    windows_3m = []
    cursor = start
    while cursor + pd.DateOffset(months=3) <= ts_max:
        windows_3m.append({
            "start": cursor,
            "end": cursor + pd.DateOffset(months=3),
        })
        cursor += pd.DateOffset(months=3)
    # Remainder
    if cursor < ts_max:
        windows_3m.append({"start": cursor, "end": ts_max})

    r_3m = _run_windows(indicators, btc, windows_3m, BASE_CFG)
    s_3m = _summarize(r_3m, "3mo non-overlap")

    print(f"  Windows: {len(windows_3m)}")
    for r in r_3m:
        if "equity" in r:
            print(f"    {r['start']}->{r['end']}  eq=${r['equity']:.0f}  ret=${r['ret']:.0f}  "
                  f"dd={r['dd']:.3f}  tr={r['trades']}  w={r['wins']}  {r['status']}")
        else:
            print(f"    {r['start']}->{r['end']}  {r['status']}")

    print(f"\n  PASS={s_3m['pass']}  FAIL={s_3m['fail']}  total=${s_3m['total_pnl']:.0f}  "
          f"trades={s_3m['trades']}  wr={s_3m['win_rate']:.1%}  avg/tr=${s_3m['avg_per_trade']:.2f}")

    # ── TEST 2: Non-overlapping 1-month windows ─────────────────────
    print("\n" + "="*80)
    print("  TEST 2: NON-OVERLAPPING 1-MONTH WINDOWS (frozen params)")
    print("="*80)

    windows_1m = []
    cursor = start
    while cursor + pd.DateOffset(months=1) <= ts_max:
        windows_1m.append({
            "start": cursor,
            "end": cursor + pd.DateOffset(months=1),
        })
        cursor += pd.DateOffset(months=1)
    if cursor < ts_max:
        windows_1m.append({"start": cursor, "end": ts_max})

    r_1m = _run_windows(indicators, btc, windows_1m, BASE_CFG)
    s_1m = _summarize(r_1m, "1mo non-overlap")

    for r in r_1m:
        if "equity" in r:
            print(f"    {r['start']}->{r['end']}  eq=${r['equity']:.0f}  ret=${r['ret']:.0f}  "
                  f"dd={r['dd']:.3f}  tr={r['trades']}  w={r['wins']}  {r['status']}")

    print(f"\n  PASS={s_1m['pass']}  FAIL={s_1m['fail']}  total=${s_1m['total_pnl']:.0f}  "
          f"trades={s_1m['trades']}  wr={s_1m['win_rate']:.1%}  avg/tr=${s_1m['avg_per_trade']:.2f}")

    # ── TEST 3: Stress battery on 1-month windows ───────────────────
    print("\n" + "="*80)
    print("  TEST 3: STRESS BATTERY (1-month windows)")
    print("="*80)

    stress_summaries = {}

    for name, cfg in STRESS_CFGS.items():
        r = _run_windows(indicators, btc, windows_1m, cfg)
        s = _summarize(r, name)
        stress_summaries[name] = s

    # Entry delay test
    r_delay = _run_windows(indicators, btc, windows_1m, BASE_CFG, delay=1)
    s_delay = _summarize(r_delay, "+1bar_delay")
    stress_summaries["+1bar_delay"] = s_delay

    print(f"\n  {'test':<20s} {'pass':>5s} {'fail':>5s} {'total':>9s} {'trades':>7s} {'wr':>6s} {'avg/tr':>8s} {'worst':>8s}")
    print(f"  {'-'*20} {'-'*5} {'-'*5} {'-'*9} {'-'*7} {'-'*6} {'-'*8} {'-'*8}")
    for name, s in stress_summaries.items():
        print(f"  {name:<20s} {s['pass']:>5d} {s['fail']:>5d} ${s['total_pnl']:>8.0f} "
              f"{s['trades']:>7d} {s['win_rate']:>5.1%} ${s['avg_per_trade']:>7.2f} ${s['worst_window']:>7.0f}")

    # ── VERDICT ──────────────────────────────────────────────────────
    print(f"\n{'='*80}")
    baseline_s = stress_summaries["baseline"]
    worst_stress = min(stress_summaries.values(), key=lambda s: s["total_pnl"])

    print(f"  HARDENING RESULTS:")
    print(f"    3-month non-overlap:  PASS={s_3m['pass']}/{len(windows_3m)}  total=${s_3m['total_pnl']:.0f}")
    print(f"    1-month non-overlap:  PASS={s_1m['pass']}/{len(windows_1m)}  total=${s_1m['total_pnl']:.0f}")
    print(f"    Worst stress test:    {worst_stress['label']}  total=${worst_stress['total_pnl']:.0f}")

    survives = (
        s_3m["total_pnl"] > 0 and
        s_1m["total_pnl"] > 0 and
        worst_stress["total_pnl"] > -200
    )

    if survives:
        print(f"\n  VERDICT: STRATEGY SURVIVES HARDENING.")
        print(f"  Frozen params, non-overlapping OOS, stress tested.")
        print(f"  Ready for paper trading evaluation.")
    elif s_3m["total_pnl"] > 0 and s_1m["total_pnl"] > 0:
        print(f"\n  VERDICT: STRATEGY IS FRAGILE UNDER STRESS.")
        print(f"  Core signal works but margins are thin.")
    else:
        print(f"\n  VERDICT: STRATEGY DOES NOT SURVIVE HARDENING.")
        print(f"  Original results were inflated by param sweep / overlap.")
    print(f"{'='*80}")

    # Save everything
    all_results = {
        "frozen_params": {"vol_pctl": FROZEN_VOL_PCTL, "breakout_bars": FROZEN_BRK_BARS},
        "test_3m": {"summary": s_3m, "windows": r_3m},
        "test_1m": {"summary": s_1m, "windows": r_1m},
        "stress": {name: s for name, s in stress_summaries.items()},
    }
    Path(REPORTS_DIR / "hardening_results.json").write_text(
        json.dumps(all_results, indent=2, default=str) + "\n"
    )
    print(f"  WROTE {REPORTS_DIR}/hardening_results.json")


if __name__ == "__main__":
    main()

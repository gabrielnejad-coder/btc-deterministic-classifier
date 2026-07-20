"""
INTRABAR EXECUTION REALISM TEST

Tests whether breakout edge survives realistic fill degradation.

Variants (all using frozen params, 1-month non-overlapping):
  1. baseline: current engine (entry at signal bar close / next open)
  2. extra_5bps: +5bps additional slippage on entry (simulates chasing)
  3. extra_10bps: +10bps additional slippage
  4. extra_20bps: +20bps (extreme adverse fill)
  5. hold_min_2: force hold_min = 2 bars (survive micro pullback)
  6. hold_min_4: force hold_min = 4 bars
  7. wider_stop: stop_loss = 3% instead of 2%
  8. tighter_stop: stop_loss = 1.5%

The key question: does edge survive realistic execution degradation,
or does it live entirely in the theoretical fill price?
"""
from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd

from backtest.engine import EngineConfig, run_engine


REPORTS_DIR = Path("reports/entry_realism")
DATA_DIR = Path("data_parquet")

FROZEN_VOL_PCTL = 0.60
FROZEN_BRK_BARS = 12
MAX_DD = 0.10


def _make_cfg(extra_slip=0.0, hold_min=24, stop_pct=0.02):
    return EngineConfig(
        fee_taker=0.0004,
        slippage_side=0.0001 + extra_slip,
        stop_loss_pct=stop_pct,
        hold_min_bars=hold_min,
        initial_equity=1000.0,
        one_position=True,
    )


VARIANTS = {
    "baseline":       _make_cfg(),
    "+5bps_slip":     _make_cfg(extra_slip=0.0005),
    "+10bps_slip":    _make_cfg(extra_slip=0.0010),
    "+20bps_slip":    _make_cfg(extra_slip=0.0020),
    "hold_min=2":     _make_cfg(hold_min=2),
    "hold_min=4":     _make_cfg(hold_min=4),
    "hold_min=12":    _make_cfg(hold_min=12),
    "stop=1.5%":      _make_cfg(stop_pct=0.015),
    "stop=3.0%":      _make_cfg(stop_pct=0.03),
    "stop=5.0%":      _make_cfg(stop_pct=0.05),
}


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

    bb = FROZEN_BRK_BARS
    brk = np.full(n, False)
    for i in range(bb, n):
        brk[i] = close[i] > np.max(high[i-bb:i])

    df["vol_pctl"] = vol_pctl
    df["breakout"] = brk
    return df


def _get_signals(indicators, start, end):
    mask = (indicators["ts"] >= start) & (indicators["ts"] < end)
    chunk = indicators.loc[mask].copy()
    if len(chunk) == 0:
        return pd.Series(dtype="object")

    entry = (chunk["vol_pctl"] >= FROZEN_VOL_PCTL) & chunk["breakout"]
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

    # Non-overlapping 1-month windows
    start = btc["ts"].min() + pd.DateOffset(months=12)
    ts_max = btc["ts"].max()
    windows = []
    cursor = start
    while cursor + pd.DateOffset(months=1) <= ts_max:
        windows.append({"start": cursor, "end": cursor + pd.DateOffset(months=1)})
        cursor += pd.DateOffset(months=1)
    if cursor < ts_max:
        windows.append({"start": cursor, "end": ts_max})

    print(f"ENTRY REALISM TEST")
    print(f"  frozen: vol_pctl={FROZEN_VOL_PCTL}, brk={FROZEN_BRK_BARS}")
    print(f"  windows: {len(windows)} (1-month, non-overlapping)")
    print(f"  variants: {len(VARIANTS)}")

    all_summaries = {}

    for vname, cfg in VARIANTS.items():
        results = []
        for w in windows:
            tb = _slice(btc, w["start"], w["end"])
            if len(tb) < 50:
                continue
            sig = _get_signals(indicators, w["start"], w["end"])
            if sig.empty:
                continue

            _, _, met = run_engine(tb, sig, cfg)

            us = pd.Series("up", index=pd.DatetimeIndex(
                pd.to_datetime(tb["ts"].values, utc=True)), dtype="object")
            _, _, bm = run_engine(tb, us, cfg)

            dok = met["max_drawdown"] <= MAX_DD
            beats = met["final_equity"] > bm["final_equity"]
            real = met["num_trades"] >= 1
            status = "PASS" if (dok and beats and real) else ("HOLLOW" if met["num_trades"] == 0 else "FAIL")

            results.append({
                "ret": met["total_return"],
                "dd": met["max_drawdown"],
                "trades": met["num_trades"],
                "wins": met["num_wins"],
                "status": status,
            })

        traded = [r for r in results]
        np_ = sum(1 for r in traded if r["status"] == "PASS")
        nf = sum(1 for r in traded if r["status"] == "FAIL")
        total_pnl = sum(r["ret"] for r in traded)
        total_tr = sum(r["trades"] for r in traded)
        total_w = sum(r["wins"] for r in traded)
        wr = total_w / total_tr if total_tr > 0 else 0
        apt = total_pnl / total_tr if total_tr > 0 else 0
        worst = min((r["ret"] for r in traded), default=0)
        max_dd = max((r["dd"] for r in traded), default=0)

        all_summaries[vname] = {
            "pass": np_, "fail": nf, "total_pnl": round(total_pnl, 2),
            "trades": total_tr, "win_rate": round(wr, 3),
            "avg_per_trade": round(apt, 2), "worst": round(worst, 2),
            "max_dd": round(max_dd, 3),
        }

    # ── Results ──────────────────────────────────────────────────────
    print(f"\n{'='*95}")
    print(f"  {'variant':<16s} {'pass':>5s} {'fail':>5s} {'total':>9s} {'trades':>7s} {'wr':>6s} {'avg/tr':>8s} {'worst':>8s} {'max_dd':>7s}")
    print(f"  {'-'*16} {'-'*5} {'-'*5} {'-'*9} {'-'*7} {'-'*6} {'-'*8} {'-'*8} {'-'*7}")

    for vname, s in all_summaries.items():
        marker = " <<<" if s["total_pnl"] > 0 else ""
        print(f"  {vname:<16s} {s['pass']:>5d} {s['fail']:>5d} ${s['total_pnl']:>8.0f} "
              f"{s['trades']:>7d} {s['win_rate']:>5.1%} ${s['avg_per_trade']:>7.2f} "
              f"${s['worst']:>7.0f} {s['max_dd']:>6.3f}{marker}")

    # ── Analysis ─────────────────────────────────────────────────────
    print(f"\n{'='*95}")
    base = all_summaries["baseline"]
    print(f"  EXECUTION SENSITIVITY ANALYSIS:")
    print(f"  ")

    # Slippage degradation curve
    print(f"  Slippage degradation:")
    for v in ["baseline", "+5bps_slip", "+10bps_slip", "+20bps_slip"]:
        s = all_summaries[v]
        pnl_pct = (s["total_pnl"] / base["total_pnl"] * 100) if base["total_pnl"] != 0 else 0
        print(f"    {v:<16s}  ${s['total_pnl']:>8.0f}  ({pnl_pct:>5.1f}% of baseline)")

    print(f"  ")
    print(f"  Hold sensitivity:")
    for v in ["hold_min=2", "hold_min=4", "hold_min=12", "baseline"]:
        s = all_summaries[v]
        print(f"    {v:<16s}  ${s['total_pnl']:>8.0f}  trades={s['trades']}  wr={s['win_rate']:.1%}")

    print(f"  ")
    print(f"  Stop sensitivity:")
    for v in ["stop=1.5%", "baseline", "stop=3.0%", "stop=5.0%"]:
        s = all_summaries[v]
        print(f"    {v:<16s}  ${s['total_pnl']:>8.0f}  trades={s['trades']}  wr={s['win_rate']:.1%}  max_dd={s['max_dd']:.3f}")

    # Verdict
    survives_slip = all_summaries["+10bps_slip"]["total_pnl"] > 0
    survives_hold = all_summaries["hold_min=4"]["total_pnl"] > 0
    survives_stop = all_summaries["stop=3.0%"]["total_pnl"] > 0

    print(f"\n  SURVIVES +10bps slip:  {'YES' if survives_slip else 'NO'}")
    print(f"  SURVIVES hold_min=4:   {'YES' if survives_hold else 'NO'}")
    print(f"  SURVIVES stop=3.0%:    {'YES' if survives_stop else 'NO'}")

    if survives_slip and survives_hold:
        print(f"\n  VERDICT: EDGE SURVIVES EXECUTION REALISM.")
        print(f"  Ready for paper trading with prompt hourly execution.")
    elif survives_slip:
        print(f"\n  VERDICT: EDGE SURVIVES COST DEGRADATION.")
        print(f"  Hold sensitivity suggests entry timing matters but is manageable.")
    else:
        print(f"\n  VERDICT: EDGE IS BAR-CLOSE ARTIFACT.")
        print(f"  Does not survive realistic execution degradation.")
    print(f"{'='*95}")

    Path(REPORTS_DIR / "realism_results.json").write_text(
        json.dumps(all_summaries, indent=2, default=str) + "\n"
    )
    print(f"  WROTE {REPORTS_DIR}/realism_results.json")


if __name__ == "__main__":
    main()

"""
Threshold sweep for v3 long-only policy.
Runs on VALIDATE split only. Never touches test.

Finds trade_threshold that maximizes final_equity
subject to: max_drawdown <= 0.10 and num_trades >= 5.

Writes winner to reports/v3_policy_thresholds.json.
"""
from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd

from backtest.engine import EngineConfig, run_engine
from backtest.walkforward import split_walkforward
from features.build_features_leaders_v1 import build_features_leaders_v1
from model.classifier_v3 import load_v3, predict_proba_df


REPORTS_DIR = Path("reports")
MAX_DD = 0.10
MIN_TRADES = 5

ENGINE_CFG = EngineConfig(
    fee_taker=0.0004,
    slippage_side=0.0001,
    stop_loss_pct=0.02,
    hold_min_bars=12,
    initial_equity=1000.0,
    one_position=True,
)


def _build_signals(ts, p_up, threshold):
    idx = pd.DatetimeIndex(pd.to_datetime(ts.values, utc=True), name="ts")
    values = np.where(p_up >= threshold, "up", "flat")
    return pd.Series(values, index=idx, dtype="object", name="signal")


def main() -> None:
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    btc = pd.read_parquet("data_parquet/BTCUSD_USD_1h_20220323_now.parquet").copy()
    eth = pd.read_parquet("data_parquet/ETH_1h.parquet").copy()
    sol = pd.read_parquet("data_parquet/SOL_1h.parquet").copy()

    btc["ts"] = pd.to_datetime(btc["ts"], utc=True)
    btc = btc.sort_values("ts").reset_index(drop=True)

    splits = split_walkforward(btc)
    val_df = splits["validate"].copy().reset_index(drop=True)

    # Build features + probabilities once (threshold only changes policy)
    model = load_v3()
    feats = build_features_leaders_v1(val_df, eth, sol)
    feats = feats.dropna().reset_index(drop=True)
    probs = predict_proba_df(model, feats)
    p_up = probs["p_up"].values

    # Sweep
    grid = np.round(np.arange(0.40, 0.76, 0.01), 2)
    results = []

    print("V3 THRESHOLD SWEEP — VALIDATE ONLY")
    print(f"  grid: {grid[0]} to {grid[-1]}, step 0.01")
    print(f"  constraints: dd <= {MAX_DD}, trades >= {MIN_TRADES}")
    print(f"  {'thr':>5s} {'equity':>9s} {'return':>9s} {'dd':>8s} {'trades':>7s} {'status':>10s}")
    print(f"  {'-'*5} {'-'*9} {'-'*9} {'-'*8} {'-'*7} {'-'*10}")

    for thr in grid:
        signals = _build_signals(feats["ts"], p_up, float(thr))
        _, _, m = run_engine(val_df, signals, ENGINE_CFG)

        feasible = m["max_drawdown"] <= MAX_DD and m["num_trades"] >= MIN_TRADES
        status = "FEASIBLE" if feasible else "---"

        results.append({
            "trade_threshold": float(thr),
            "final_equity": m["final_equity"],
            "total_return": m["total_return"],
            "max_drawdown": m["max_drawdown"],
            "num_trades": m["num_trades"],
            "num_completed": m["num_completed"],
            "feasible": feasible,
        })

        print(f"  {thr:>5.2f} ${m['final_equity']:>8.2f} ${m['total_return']:>8.2f} {m['max_drawdown']:>7.4f} {m['num_trades']:>7d} {status:>10s}")

    # Find best feasible
    feasible_results = [r for r in results if r["feasible"]]

    if feasible_results:
        best = max(feasible_results, key=lambda r: r["final_equity"])
        print(f"\n  BEST FEASIBLE")
        print(f"    threshold:  {best['trade_threshold']}")
        print(f"    equity:     ${best['final_equity']:.2f}")
        print(f"    return:     ${best['total_return']:.2f}")
        print(f"    dd:         {best['max_drawdown']:.4f}")
        print(f"    trades:     {best['num_trades']}")

        out = {"trade_threshold": best["trade_threshold"]}
        Path(REPORTS_DIR / "v3_policy_thresholds.json").write_text(
            json.dumps(out, indent=2) + "\n"
        )
        print(f"\n  WROTE reports/v3_policy_thresholds.json")
    else:
        print(f"\n  NO FEASIBLE THRESHOLD FOUND")
        print(f"  Model has no edge under dd <= {MAX_DD} constraint.")
        print(f"  Next step: revisit classifier or features (Layer 2).")

    # Save full sweep for audit
    Path(REPORTS_DIR / "v3_threshold_sweep.json").write_text(
        json.dumps(results, indent=2) + "\n"
    )
    print(f"  WROTE reports/v3_threshold_sweep.json")


if __name__ == "__main__":
    main()

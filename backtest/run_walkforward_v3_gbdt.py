"""
Walk-forward evaluation — v3 GBDT classifier, long-only policy.
Uses threshold selected on validate. Test is untouched holdout.
"""
from __future__ import annotations

import json
import pickle
from pathlib import Path
from typing import Any, Dict

import numpy as np
import pandas as pd

from backtest.engine import EngineConfig, run_engine
from backtest.walkforward import split_walkforward
from features.build_features_leaders_v1 import build_features_leaders_v1


REPORTS_DIR = Path("reports")

ENGINE_CFG = EngineConfig(
    fee_taker=0.0004,
    slippage_side=0.0001,
    stop_loss_pct=0.02,
    hold_min_bars=12,
    initial_equity=1000.0,
    one_position=True,
)

FEATURE_NAMES = [
    "BTC_ret_1", "BTC_ret_4", "BTC_ret_24", "BTC_vol_24", "BTC_vol_chg_1",
    "ETH_ret_1", "ETH_ret_4", "ETH_ret_24", "ETH_vol_24", "ETH_vol_chg_1",
    "SOL_ret_1", "SOL_ret_4", "SOL_ret_24", "SOL_vol_24", "SOL_vol_chg_1",
]


def _load_model():
    with open(REPORTS_DIR / "v3_gbdt_model.pkl", "rb") as f:
        return pickle.load(f)


def _load_threshold() -> float:
    path = REPORTS_DIR / "v3_gbdt_policy_thresholds.json"
    if path.exists():
        return float(json.loads(path.read_text())["trade_threshold"])
    return 0.45


def _build_signals(ts, p_up, threshold):
    idx = pd.DatetimeIndex(pd.to_datetime(ts.values, utc=True), name="ts")
    values = np.where(p_up >= threshold, "up", "flat")
    return pd.Series(values, index=idx, dtype="object", name="signal")


def _run_split(
    name: str,
    btc_split: pd.DataFrame,
    eth: pd.DataFrame,
    sol: pd.DataFrame,
    model,
    classes: list,
    trade_threshold: float,
) -> Dict[str, Any]:

    feats = build_features_leaders_v1(btc_split, eth, sol)
    feats = feats.dropna().reset_index(drop=True)

    X = feats[FEATURE_NAMES].astype(float).values
    probs = model.predict_proba(X)
    up_idx = classes.index("up")
    p_up = probs[:, up_idx]

    signals = _build_signals(feats["ts"], p_up, trade_threshold)

    trades_df, equity_df, metrics = run_engine(btc_split, signals, ENGINE_CFG)

    # Save
    prefix = f"v3_gbdt_{name}"
    trades_df.to_parquet(REPORTS_DIR / f"{prefix}_trades.parquet", index=False)
    equity_df.to_parquet(REPORTS_DIR / f"{prefix}_equity.parquet", index=False)
    with open(REPORTS_DIR / f"{prefix}_metrics.json", "w") as f:
        json.dump(metrics, f, indent=2)

    sig_counts = pd.Series(signals.values).value_counts().to_dict()
    print(f"\n  [{name}]")
    print(f"    bars={len(btc_split)}  feat_rows={len(feats)}  signals={sig_counts}")
    print(f"    trades={metrics['num_trades']}  completed={metrics['num_completed']}  wins={metrics['num_wins']}")
    print(f"    return=${metrics['total_return']:.2f}  dd={metrics['max_drawdown']:.4f}")
    print(f"    equity=${metrics['final_equity']:.2f}  fees=${metrics['total_fees']:.2f}")

    return metrics


def main() -> None:
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    btc = pd.read_parquet("data_parquet/BTCUSD_USD_1h_20220323_now.parquet").copy()
    eth = pd.read_parquet("data_parquet/ETH_1h.parquet").copy()
    sol = pd.read_parquet("data_parquet/SOL_1h.parquet").copy()

    btc["ts"] = pd.to_datetime(btc["ts"], utc=True)
    btc = btc.sort_values("ts").reset_index(drop=True)

    splits = split_walkforward(btc)

    bundle = _load_model()
    model = bundle["model"]
    classes = list(model.classes_)
    trade_threshold = _load_threshold()

    print("V3 GBDT WALKFORWARD — LONG/FLAT")
    print(f"  threshold: {trade_threshold}")
    print(f"  stop: {ENGINE_CFG.stop_loss_pct}  hold_min: {ENGINE_CFG.hold_min_bars}")
    print(f"  fees: {ENGINE_CFG.fee_taker}  slip: {ENGINE_CFG.slippage_side}")

    results = {}
    for split_name in ("train", "validate", "test"):
        split_df = splits[split_name].copy().reset_index(drop=True)
        results[split_name] = _run_split(
            name=split_name,
            btc_split=split_df,
            eth=eth,
            sol=sol,
            model=model,
            classes=classes,
            trade_threshold=trade_threshold,
        )

    print(f"\n{'='*65}")
    print(f"  {'split':<10s} {'equity':>10s} {'return':>10s} {'dd':>8s} {'trades':>7s} {'wins':>5s}")
    print(f"  {'-'*10} {'-'*10} {'-'*10} {'-'*8} {'-'*7} {'-'*5}")
    for name in ("train", "validate", "test"):
        m = results[name]
        print(f"  {name:<10s} ${m['final_equity']:>9.2f} ${m['total_return']:>9.2f} {m['max_drawdown']:>7.4f} {m['num_trades']:>7d} {m['num_wins']:>5d}")
    print(f"{'='*65}")


if __name__ == "__main__":
    main()

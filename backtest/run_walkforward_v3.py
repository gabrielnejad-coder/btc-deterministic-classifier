"""
Walk-forward evaluation — v3 classifier, long-only policy.

Signal contract with engine.py:
  - pd.Series, DatetimeIndex (UTC), values in {"up", "flat"}
  - "up"  → engine opens long at next open
  - "flat" → engine exits long (after hold_min_bars) or stays flat
  - engine expects reindex-able alignment to df["ts"]

Policy (Phase 1 — long only):
  p_up >= trade_threshold  →  "up"
  otherwise                →  "flat"

No flip_threshold. No engine_runner.py. No signal_filters.py.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict

import numpy as np
import pandas as pd

from backtest.engine import EngineConfig, run_engine
from backtest.walkforward import split_walkforward
from features.build_features_leaders_v1 import build_features_leaders_v1
from model.classifier_v3 import load_v3, predict_proba_df


# ─── Configuration ───────────────────────────────────────────────────

THRESHOLDS_PATH = Path("reports/v3_policy_thresholds.json")
REPORTS_DIR = Path("reports")

ENGINE_CFG = EngineConfig(
    fee_taker=0.0004,
    slippage_side=0.0001,
    stop_loss_pct=0.02,
    hold_min_bars=12,
    initial_equity=1000.0,
    one_position=True,
)


def _load_threshold() -> float:
    """Load trade_threshold from disk, or default to 0.60."""
    if THRESHOLDS_PATH.exists():
        d = json.loads(THRESHOLDS_PATH.read_text())
        return float(d["trade_threshold"])
    return 0.60


# ─── Policy ──────────────────────────────────────────────────────────

def _build_signals(
    ts: pd.Series,
    p_up: np.ndarray,
    trade_threshold: float,
) -> pd.Series:
    """
    Long-only policy: p_up >= threshold -> "up", else "flat".

    Returns pd.Series with UTC DatetimeIndex (engine contract).
    """
    idx = pd.DatetimeIndex(pd.to_datetime(ts.values, utc=True), name="ts")
    values = np.where(p_up >= trade_threshold, "up", "flat")
    return pd.Series(values, index=idx, dtype="object", name="signal")


# ─── Per-split runner ────────────────────────────────────────────────

def _run_split(
    name: str,
    btc_split: pd.DataFrame,
    eth: pd.DataFrame,
    sol: pd.DataFrame,
    model: dict,
    trade_threshold: float,
) -> Dict[str, Any]:
    """Features -> classifier -> policy -> engine for one walkforward split."""

    # 1. Features (BTC + leaders)
    feats = build_features_leaders_v1(btc_split, eth, sol)
    feats = feats.dropna().reset_index(drop=True)

    # 2. Classifier -> probabilities
    probs = predict_proba_df(model, feats)

    # 3. Policy -> signal Series
    signals = _build_signals(
        ts=feats["ts"],
        p_up=probs["p_up"].values,
        trade_threshold=trade_threshold,
    )

    # 4. Engine
    trades_df, equity_df, metrics = run_engine(btc_split, signals, ENGINE_CFG)

    # 5. Save artifacts
    prefix = f"v3_{name}"
    trades_df.to_parquet(REPORTS_DIR / f"{prefix}_trades.parquet", index=False)
    equity_df.to_parquet(REPORTS_DIR / f"{prefix}_equity.parquet", index=False)
    with open(REPORTS_DIR / f"{prefix}_metrics.json", "w") as f:
        json.dump(metrics, f, indent=2)

    # 6. Report
    sig_counts = pd.Series(signals.values).value_counts().to_dict()
    print(f"\n  [{name}]")
    print(f"    bars={len(btc_split)}  feat_rows={len(feats)}  signals={sig_counts}")
    print(f"    trades={metrics['num_trades']}  completed={metrics['num_completed']}")
    print(f"    return=${metrics['total_return']:.2f}  dd={metrics['max_drawdown']:.4f}")
    print(f"    equity=${metrics['final_equity']:.2f}  fees=${metrics['total_fees']:.2f}")

    return metrics


# ─── Main ────────────────────────────────────────────────────────────

def main() -> None:
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    # Load data
    btc = pd.read_parquet("data_parquet/BTCUSD_USD_1h_20220323_now.parquet").copy()
    eth = pd.read_parquet("data_parquet/ETH_1h.parquet").copy()
    sol = pd.read_parquet("data_parquet/SOL_1h.parquet").copy()

    btc["ts"] = pd.to_datetime(btc["ts"], utc=True)
    btc = btc.sort_values("ts").reset_index(drop=True)

    # Splits
    splits = split_walkforward(btc)

    # Model + threshold (loaded once, shared across splits)
    model = load_v3()
    trade_threshold = _load_threshold()

    print("V3 WALKFORWARD — LONG/FLAT")
    print(f"  trade_threshold: {trade_threshold}")
    print(f"  stop_loss: {ENGINE_CFG.stop_loss_pct}")
    print(f"  hold_min_bars: {ENGINE_CFG.hold_min_bars}")
    print(f"  fees: {ENGINE_CFG.fee_taker}  slip: {ENGINE_CFG.slippage_side}")

    # Run each split
    results = {}
    for split_name in ("train", "validate", "test"):
        split_df = splits[split_name].copy().reset_index(drop=True)
        results[split_name] = _run_split(
            name=split_name,
            btc_split=split_df,
            eth=eth,
            sol=sol,
            model=model,
            trade_threshold=trade_threshold,
        )

    # Summary table
    print(f"\n{'='*60}")
    print(f"  {'split':<10s} {'equity':>10s} {'return':>10s} {'dd':>8s} {'trades':>7s}")
    print(f"  {'-'*10} {'-'*10} {'-'*10} {'-'*8} {'-'*7}")
    for name in ("train", "validate", "test"):
        m = results[name]
        print(f"  {name:<10s} ${m['final_equity']:>9.2f} ${m['total_return']:>9.2f} {m['max_drawdown']:>7.4f} {m['num_trades']:>7d}")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()

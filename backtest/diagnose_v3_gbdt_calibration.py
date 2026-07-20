"""
V3 GBDT Calibration Diagnostic — TRAIN ONLY.

Same diagnostic as diagnose_v3_calibration.py but using the GBDT model.
Answers: did the nonlinear model fix the tail collapse?
"""
from __future__ import annotations

import json
import pickle
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.stats import spearmanr

from backtest.walkforward import split_walkforward
from features.build_features_leaders_v1 import build_features_leaders_v1


FEATURE_NAMES = [
    "BTC_ret_1", "BTC_ret_4", "BTC_ret_24", "BTC_vol_24", "BTC_vol_chg_1",
    "ETH_ret_1", "ETH_ret_4", "ETH_ret_24", "ETH_vol_24", "ETH_vol_chg_1",
    "SOL_ret_1", "SOL_ret_4", "SOL_ret_24", "SOL_vol_24", "SOL_vol_chg_1",
]


def _load_gbdt():
    path = Path("reports/v3_gbdt_model.pkl")
    if not path.exists():
        raise FileNotFoundError("Run train_v3_gbdt.py first")
    with open(path, "rb") as f:
        return pickle.load(f)


def main() -> None:
    btc = pd.read_parquet("data_parquet/BTCUSD_USD_1h_20220323_now.parquet").copy()
    eth = pd.read_parquet("data_parquet/ETH_1h.parquet").copy()
    sol = pd.read_parquet("data_parquet/SOL_1h.parquet").copy()

    btc["ts"] = pd.to_datetime(btc["ts"], utc=True)
    btc = btc.sort_values("ts").reset_index(drop=True)

    splits = split_walkforward(btc)
    train = splits["train"].copy().reset_index(drop=True)

    # Features
    feats = build_features_leaders_v1(train, eth, sol)
    feats = feats.dropna().reset_index(drop=True)

    # Predict
    bundle = _load_gbdt()
    model = bundle["model"]
    classes = list(model.classes_)
    X = feats[FEATURE_NAMES].astype(float).values
    probs = model.predict_proba(X)

    up_idx = classes.index("up")
    p_up = probs[:, up_idx]

    # Forward return
    train_ts = train.set_index("ts").sort_index()
    close = train_ts["close"].astype(float)
    fwd_12h = (close.shift(-12) / close - 1.0)
    feats_ts = pd.to_datetime(feats["ts"], utc=True)
    fwd_aligned = fwd_12h.reindex(feats_ts).values

    df = pd.DataFrame({
        "p_up": p_up,
        "fwd_ret_12h": fwd_aligned,
    }).dropna().reset_index(drop=True)

    n = len(df)
    print("V3 GBDT CALIBRATION DIAGNOSTIC — TRAIN ONLY")
    print(f"  rows: {n}")
    print(f"  p_up range: [{df['p_up'].min():.4f}, {df['p_up'].max():.4f}]")

    # Decile analysis
    df["decile"] = pd.qcut(df["p_up"], 10, labels=False, duplicates="drop")

    print(f"\n  DECILE ANALYSIS")
    print(f"  {'decile':>7s} {'p_up_lo':>8s} {'p_up_hi':>8s} {'avg_ret':>10s} {'hit_rate':>9s} {'count':>6s}")
    print(f"  {'-'*7} {'-'*8} {'-'*8} {'-'*10} {'-'*9} {'-'*6}")

    decile_stats = []
    for d in sorted(df["decile"].unique()):
        sub = df[df["decile"] == d]
        avg = sub["fwd_ret_12h"].mean()
        hit = (sub["fwd_ret_12h"] > 0).mean()
        lo = sub["p_up"].min()
        hi = sub["p_up"].max()
        decile_stats.append({"decile": int(d), "avg_ret": float(avg), "hit_rate": float(hit)})
        print(f"  {d:>7d} {lo:>8.4f} {hi:>8.4f} {avg:>10.5f} {hit:>9.3f} {len(sub):>6d}")

    # Monotonicity
    avg_rets = [s["avg_ret"] for s in decile_stats]
    increases = sum(1 for i in range(1, len(avg_rets)) if avg_rets[i] > avg_rets[i-1])
    total = len(avg_rets) - 1
    mono_score = increases / total if total > 0 else 0

    print(f"\n  MONOTONICITY: {increases}/{total} = {mono_score:.2f}")

    # Spearman
    rho, pval = spearmanr(df["p_up"], df["fwd_ret_12h"])
    print(f"\n  SPEARMAN: rho={rho:.6f}  p={pval:.2e}")

    # Top vs bottom
    top = df[df["decile"] == df["decile"].max()]["fwd_ret_12h"]
    bot = df[df["decile"] == df["decile"].min()]["fwd_ret_12h"]
    spread = top.mean() - bot.mean()
    print(f"\n  TOP DECILE:    {top.mean():.5f}")
    print(f"  BOTTOM DECILE: {bot.mean():.5f}")
    print(f"  SPREAD:        {spread:.5f}")

    # Hit rates
    print(f"\n  HIT RATE AT THRESHOLDS")
    print(f"  {'thr':>6s} {'count':>6s} {'hit':>7s} {'avg_ret':>10s}")
    print(f"  {'-'*6} {'-'*6} {'-'*7} {'-'*10}")
    for thr in [0.35, 0.40, 0.45, 0.50, 0.55, 0.60]:
        sub = df[df["p_up"] >= thr]
        if len(sub) == 0:
            print(f"  {thr:>6.2f} {0:>6d}     ---        ---")
        else:
            print(f"  {thr:>6.2f} {len(sub):>6d} {(sub['fwd_ret_12h']>0).mean():>7.3f} {sub['fwd_ret_12h'].mean():>10.5f}")

    # Comparison verdict
    print(f"\n  {'='*55}")
    print(f"  COMPARISON vs SOFTMAX")
    print(f"    Softmax: mono=0.33, rho=0.058, spread=+0.00226")
    print(f"    GBDT:    mono={mono_score:.2f}, rho={rho:.4f}, spread={spread:+.5f}")

    if mono_score > 0.50 and spread > 0.003:
        print(f"    IMPROVED. Proceed to threshold sweep on validate.")
    elif mono_score > 0.33 and rho > 0.058:
        print(f"    MARGINAL IMPROVEMENT. May be worth sweeping validate.")
    else:
        print(f"    NO IMPROVEMENT. Features likely lack tradable signal.")
    print(f"  {'='*55}")

    # Save
    Path("reports").mkdir(parents=True, exist_ok=True)
    Path("reports/v3_gbdt_calibration.json").write_text(json.dumps({
        "n": n, "rho": float(rho), "pval": float(pval),
        "spread": float(spread), "mono_score": float(mono_score),
        "decile_stats": decile_stats,
    }, indent=2) + "\n")
    print(f"  WROTE reports/v3_gbdt_calibration.json")


if __name__ == "__main__":
    main()

"""
V3 Classifier Calibration Diagnostic — TRAIN ONLY.

No engine. No thresholds. No stops.
Pure question: does p_up predict 12h forward return?

Tests:
  1. Decile monotonicity: as p_up increases, does avg fwd return increase?
  2. Top decile edge: what is avg return when p_up is highest?
  3. Hit rate by bucket: when p_up > X, how often is fwd return > 0?
  4. Rank correlation: Spearman(p_up, fwd_return_12h)
  5. Anti-predictiveness check: is the slope negative?
"""
from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd

from backtest.walkforward import split_walkforward
from features.build_features_leaders_v1 import build_features_leaders_v1
from model.classifier_v3 import load_v3, predict_proba_df


def main() -> None:
    # ── Load data ────────────────────────────────────────────────────
    btc = pd.read_parquet("data_parquet/BTCUSD_USD_1h_20220323_now.parquet").copy()
    eth = pd.read_parquet("data_parquet/ETH_1h.parquet").copy()
    sol = pd.read_parquet("data_parquet/SOL_1h.parquet").copy()

    btc["ts"] = pd.to_datetime(btc["ts"], utc=True)
    btc = btc.sort_values("ts").reset_index(drop=True)

    splits = split_walkforward(btc)
    train = splits["train"].copy().reset_index(drop=True)

    # ── Features + probabilities ─────────────────────────────────────
    feats = build_features_leaders_v1(train, eth, sol)
    feats = feats.dropna().reset_index(drop=True)

    model = load_v3()
    probs = predict_proba_df(model, feats)

    # ── Forward return (12h = 12 bars at 1h) ─────────────────────────
    # Computed from the TRAIN bars, aligned by timestamp
    train_ts = train.set_index("ts").sort_index()
    close = train_ts["close"].astype(float)
    fwd_12h = (close.shift(-12) / close - 1.0)

    # Align: feats["ts"] → fwd return
    feats_ts = pd.to_datetime(feats["ts"], utc=True)
    fwd_aligned = fwd_12h.reindex(feats_ts).values

    # Build analysis frame
    df = pd.DataFrame({
        "p_up": probs["p_up"].values,
        "p_down": probs["p_down"].values,
        "p_flat": probs["p_flat"].values,
        "fwd_ret_12h": fwd_aligned,
    }).dropna().reset_index(drop=True)

    n = len(df)
    print("V3 CALIBRATION DIAGNOSTIC — TRAIN ONLY")
    print(f"  rows with valid fwd return: {n}")
    print(f"  p_up range: [{df['p_up'].min():.4f}, {df['p_up'].max():.4f}]")
    print(f"  fwd_ret_12h range: [{df['fwd_ret_12h'].min():.4f}, {df['fwd_ret_12h'].max():.4f}]")

    # ── 1. Decile analysis ───────────────────────────────────────────
    df["decile"] = pd.qcut(df["p_up"], 10, labels=False, duplicates="drop")

    print(f"\n  DECILE ANALYSIS (p_up buckets → avg fwd return)")
    print(f"  {'decile':>7s} {'p_up_lo':>8s} {'p_up_hi':>8s} {'avg_ret':>10s} {'hit_rate':>9s} {'count':>6s}")
    print(f"  {'-'*7} {'-'*8} {'-'*8} {'-'*10} {'-'*9} {'-'*6}")

    decile_stats = []
    for d in sorted(df["decile"].unique()):
        subset = df[df["decile"] == d]
        avg_ret = subset["fwd_ret_12h"].mean()
        hit = (subset["fwd_ret_12h"] > 0).mean()
        lo = subset["p_up"].min()
        hi = subset["p_up"].max()
        cnt = len(subset)
        decile_stats.append({"decile": int(d), "avg_ret": avg_ret, "hit_rate": hit})
        print(f"  {d:>7d} {lo:>8.4f} {hi:>8.4f} {avg_ret:>10.5f} {hit:>9.3f} {cnt:>6d}")

    # ── 2. Monotonicity check ────────────────────────────────────────
    avg_rets = [s["avg_ret"] for s in decile_stats]
    monotonic_increases = sum(1 for i in range(1, len(avg_rets)) if avg_rets[i] > avg_rets[i-1])
    total_steps = len(avg_rets) - 1
    monotonic_score = monotonic_increases / total_steps if total_steps > 0 else 0

    print(f"\n  MONOTONICITY")
    print(f"    increases: {monotonic_increases}/{total_steps}")
    print(f"    score: {monotonic_score:.2f} (1.0 = perfect monotonic, 0.5 = random)")

    # ── 3. Rank correlation ──────────────────────────────────────────
    from scipy.stats import spearmanr
    rho, pval = spearmanr(df["p_up"], df["fwd_ret_12h"])

    print(f"\n  RANK CORRELATION")
    print(f"    Spearman rho:  {rho:.6f}")
    print(f"    p-value:       {pval:.2e}")

    if rho > 0.01 and pval < 0.05:
        verdict_corr = "WEAK POSITIVE (signal exists but may be too weak)"
    elif rho > 0.03 and pval < 0.01:
        verdict_corr = "POSITIVE (signal present)"
    elif rho < -0.01 and pval < 0.05:
        verdict_corr = "ANTI-PREDICTIVE (higher p_up → worse returns)"
    else:
        verdict_corr = "NO SIGNAL (rho near zero or not significant)"

    print(f"    verdict:       {verdict_corr}")

    # ── 4. Top/bottom decile spread ──────────────────────────────────
    top = df[df["decile"] == df["decile"].max()]["fwd_ret_12h"]
    bot = df[df["decile"] == df["decile"].min()]["fwd_ret_12h"]
    spread = top.mean() - bot.mean()

    print(f"\n  TOP vs BOTTOM DECILE")
    print(f"    top decile avg ret:    {top.mean():.5f}")
    print(f"    bottom decile avg ret: {bot.mean():.5f}")
    print(f"    spread:                {spread:.5f}")

    if spread > 0.001:
        verdict_spread = "POSITIVE SPREAD (model ranks correctly)"
    elif spread < -0.001:
        verdict_spread = "INVERTED SPREAD (model ranks backwards)"
    else:
        verdict_spread = "NO SPREAD (model cannot separate outcomes)"

    print(f"    verdict:               {verdict_spread}")

    # ── 5. Hit rate at thresholds ────────────────────────────────────
    print(f"\n  HIT RATE AT THRESHOLDS")
    print(f"  {'threshold':>10s} {'count':>6s} {'hit_rate':>9s} {'avg_ret':>10s}")
    print(f"  {'-'*10} {'-'*6} {'-'*9} {'-'*10}")

    for thr in [0.40, 0.45, 0.50, 0.55, 0.60]:
        subset = df[df["p_up"] >= thr]
        if len(subset) == 0:
            print(f"  {thr:>10.2f} {0:>6d}       ---        ---")
            continue
        hit = (subset["fwd_ret_12h"] > 0).mean()
        avg = subset["fwd_ret_12h"].mean()
        print(f"  {thr:>10.2f} {len(subset):>6d} {hit:>9.3f} {avg:>10.5f}")

    # ── 6. Overall verdict ───────────────────────────────────────────
    print(f"\n  {'='*55}")
    print(f"  VERDICT")

    if rho < -0.01 and pval < 0.05:
        print(f"    ANTI-PREDICTIVE. Higher p_up correlates with WORSE returns.")
        print(f"    Possible causes: label inversion, feature sign error, regime issue.")
        print(f"    Action: inspect labels and feature construction before retraining.")
    elif abs(rho) < 0.01 or pval >= 0.05:
        print(f"    NO SIGNAL. p_up has no statistically significant relationship")
        print(f"    with 12h forward returns on the training set.")
        print(f"    Action: features or model class are inadequate. Layer 2 rebuild.")
    elif rho > 0.01 and spread > 0:
        print(f"    WEAK SIGNAL. Directionally correct but likely too weak to")
        print(f"    overcome fees + stops. Check if raw edge exceeds ~0.1% per trade.")
        print(f"    Action: consider feature expansion or nonlinear model.")
    print(f"  {'='*55}")

    # ── Save for audit ───────────────────────────────────────────────
    Path("reports").mkdir(parents=True, exist_ok=True)
    audit = {
        "n_rows": n,
        "spearman_rho": float(rho),
        "spearman_pval": float(pval),
        "top_bottom_spread": float(spread),
        "monotonic_score": float(monotonic_score),
        "decile_stats": decile_stats,
    }
    Path("reports/v3_calibration_diagnostic.json").write_text(
        json.dumps(audit, indent=2) + "\n"
    )
    print(f"\n  WROTE reports/v3_calibration_diagnostic.json")


if __name__ == "__main__":
    main()

"""
Feature Expansion Experiment — Derivs Pack V1.

Hypothesis: funding rate features improve OOS stability.
Kill condition: if adaptive walkforward doesn't materially improve
over 3/12 PASS (with 2 hollow), the 12h horizon needs rethinking.

Pipeline:
  1. Train GBDT on train split (22 features)
  2. Calibration diagnostic on train
  3. Threshold sweep on validate
  4. Adaptive rolling walkforward (same V1.1 schedule)
  5. Compare vs baseline (no-derivs GBDT)
"""
from __future__ import annotations

import json
import pickle
from pathlib import Path
from datetime import datetime, timezone
from dateutil.relativedelta import relativedelta
from typing import Any, Dict, List

import numpy as np
import pandas as pd
from scipy.stats import spearmanr
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.calibration import CalibratedClassifierCV

from backtest.engine import EngineConfig, run_engine
from backtest.walkforward import split_walkforward
from features.packs.pack_v1_derivs import build_features_pack_v1, FEATURE_NAMES_PACK_V1


REPORTS_DIR = Path("reports/derivs_v1")
DATA_DIR = Path("data_parquet")

ENGINE_CFG = EngineConfig(
    fee_taker=0.0004,
    slippage_side=0.0001,
    stop_loss_pct=0.02,
    hold_min_bars=12,
    initial_equity=1000.0,
    one_position=True,
)

GBDT_PARAMS = dict(
    max_iter=300, max_depth=4, learning_rate=0.05,
    min_samples_leaf=50, max_leaf_nodes=31,
    l2_regularization=1.0, random_state=42,
)

LABEL_HORIZON = 12
LABEL_THRESHOLD = 0.002
MAX_DD = 0.10
THRESHOLD_GRID = np.round(np.arange(0.40, 0.56, 0.01), 2)
MIN_TRADES_VAL = 3

TRAIN_MONTHS = 12
RETRAIN_MONTHS = 3
VAL_MONTHS = 1
GAP_BARS = 12

FEAT_NAMES = FEATURE_NAMES_PACK_V1


def _make_labels(df):
    close = df["close"].astype(float)
    fwd = close.shift(-LABEL_HORIZON) / close - 1.0
    y = pd.Series("flat", index=df.index, dtype="object")
    y[fwd > LABEL_THRESHOLD] = "up"
    y[fwd < -LABEL_THRESHOLD] = "down"
    y[fwd.isna()] = None
    return y


def _build_signals(ts, p_up, thr):
    idx = pd.DatetimeIndex(pd.to_datetime(ts.values, utc=True), name="ts")
    return pd.Series(np.where(p_up >= thr, "up", "flat"), index=idx, dtype="object")


def _train_model(X, y):
    if not isinstance(y, np.ndarray):
        y = np.array(y)
    clf = HistGradientBoostingClassifier(**GBDT_PARAMS)
    clf.fit(X, y)
    cal = CalibratedClassifierCV(clf, cv=3, method="isotonic")
    cal.fit(X, y)
    return cal


def _p_up(model, X):
    classes = list(model.classes_)
    return model.predict_proba(X)[:, classes.index("up")]


def _months_range(start_ts, end_ts):
    cursor = start_ts + pd.DateOffset(months=TRAIN_MONTHS)
    while cursor < end_ts:
        yield {
            "train_start": cursor - pd.DateOffset(months=TRAIN_MONTHS),
            "train_end": cursor,
            "val_start": cursor + pd.Timedelta(hours=GAP_BARS),
            "val_end": cursor + pd.DateOffset(months=VAL_MONTHS),
            "test_start": cursor + pd.DateOffset(months=VAL_MONTHS),
            "test_end": min(cursor + pd.DateOffset(months=VAL_MONTHS + RETRAIN_MONTHS), end_ts),
        }
        cursor += pd.DateOffset(months=RETRAIN_MONTHS)


def _slice(df, start, end):
    return df[(df["ts"] >= start) & (df["ts"] < end)].copy().reset_index(drop=True)


def main() -> None:
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    # ── Load data ────────────────────────────────────────────────────
    btc = pd.read_parquet(DATA_DIR / "BTCUSD_USD_1h_20220323_now.parquet").copy()
    eth = pd.read_parquet(DATA_DIR / "ETH_1h.parquet").copy()
    sol = pd.read_parquet(DATA_DIR / "SOL_1h.parquet").copy()
    btc["ts"] = pd.to_datetime(btc["ts"], utc=True)
    btc = btc.sort_values("ts").reset_index(drop=True)

    # ── Build all features once ──────────────────────────────────────
    all_feats = build_features_pack_v1(btc, eth, sol)
    all_feats = all_feats.dropna(subset=FEAT_NAMES).reset_index(drop=True)

    # Labels from canonical BTC bars
    btc_indexed = btc.set_index("ts").sort_index()
    close_aligned = btc_indexed["close"].reindex(all_feats["ts"]).values
    fwd = pd.Series(close_aligned).shift(-LABEL_HORIZON) / pd.Series(close_aligned) - 1.0
    labels = pd.Series("flat", index=all_feats.index, dtype="object")
    labels[fwd > LABEL_THRESHOLD] = "up"
    labels[fwd < -LABEL_THRESHOLD] = "down"
    labels[fwd.isna()] = None
    all_feats["label"] = labels.values

    print(f"DERIVS V1 EXPERIMENT")
    print(f"  features: {len(FEAT_NAMES)} ({len(FEAT_NAMES) - 15} new from funding)")
    print(f"  total rows: {len(all_feats)}")

    # ── 1. CALIBRATION DIAGNOSTIC (train split) ─────────────────────
    splits = split_walkforward(btc)
    train_btc = splits["train"]
    train_feats = all_feats[
        (all_feats["ts"] >= train_btc["ts"].min()) &
        (all_feats["ts"] <= train_btc["ts"].max())
    ].copy()
    train_clean = train_feats.dropna(subset=["label", *FEAT_NAMES]).reset_index(drop=True)

    X_train = train_clean[FEAT_NAMES].astype(float).values
    y_train = train_clean["label"].values

    model = _train_model(X_train, y_train)
    p_up_train = _p_up(model, X_train)

    # Forward return for calibration
    close_train = btc_indexed["close"].reindex(
        pd.to_datetime(train_clean["ts"].values, utc=True)
    )
    fwd_train = (close_train.shift(-12) / close_train - 1.0).values

    cal_df = pd.DataFrame({"p_up": p_up_train, "fwd": fwd_train}).dropna()
    cal_df["decile"] = pd.qcut(cal_df["p_up"], 10, labels=False, duplicates="drop")

    print(f"\n  CALIBRATION (train, {len(cal_df)} rows)")
    print(f"  {'dec':>4s} {'p_lo':>7s} {'p_hi':>7s} {'avg_ret':>9s} {'hit':>6s}")
    print(f"  {'-'*4} {'-'*7} {'-'*7} {'-'*9} {'-'*6}")

    for d in sorted(cal_df["decile"].unique()):
        s = cal_df[cal_df["decile"] == d]
        print(f"  {d:>4d} {s['p_up'].min():>7.4f} {s['p_up'].max():>7.4f} "
              f"{s['fwd'].mean():>9.5f} {(s['fwd']>0).mean():>6.3f}")

    rho, pval = spearmanr(cal_df["p_up"], cal_df["fwd"])
    avg_rets = [cal_df[cal_df["decile"]==d]["fwd"].mean() for d in sorted(cal_df["decile"].unique())]
    mono = sum(1 for i in range(1, len(avg_rets)) if avg_rets[i] > avg_rets[i-1]) / max(len(avg_rets)-1, 1)
    spread = avg_rets[-1] - avg_rets[0]

    print(f"\n  Spearman: {rho:.4f} (p={pval:.2e})")
    print(f"  Monotonicity: {mono:.2f}")
    print(f"  Spread: {spread:+.5f}")
    print(f"  (Baseline GBDT was: rho=0.4825, mono=1.00, spread=+0.02740)")

    # ── 2. ADAPTIVE WALKFORWARD ──────────────────────────────────────
    windows = list(_months_range(btc["ts"].min(), btc["ts"].max()))

    print(f"\n  ADAPTIVE WALKFORWARD ({len(windows)} windows)")

    results = []
    for i, w in enumerate(windows):
        tr = all_feats[(all_feats["ts"] >= w["train_start"]) & (all_feats["ts"] < w["train_end"])]
        tr = tr.dropna(subset=["label", *FEAT_NAMES]).reset_index(drop=True)

        if len(tr) < 500:
            results.append({"window": i, "status": "skip_small_train"})
            continue

        mdl = _train_model(tr[FEAT_NAMES].values, tr["label"].values)

        # Validate
        val_bars = _slice(btc, w["val_start"], w["val_end"])
        val_f = all_feats[(all_feats["ts"] >= w["val_start"]) & (all_feats["ts"] < w["val_end"])]
        val_f = val_f.dropna(subset=FEAT_NAMES).reset_index(drop=True)

        if len(val_f) < 50 or len(val_bars) < 50:
            results.append({"window": i, "status": "skip_small_val"})
            continue

        p_val = _p_up(mdl, val_f[FEAT_NAMES].values)
        best_thr, best_eq = None, 0
        for thr in THRESHOLD_GRID:
            sig = _build_signals(val_f["ts"], p_val, float(thr))
            _, _, m = run_engine(val_bars, sig, ENGINE_CFG)
            if m["max_drawdown"] <= MAX_DD and m["num_trades"] >= MIN_TRADES_VAL:
                if m["final_equity"] > best_eq:
                    best_eq, best_thr = m["final_equity"], float(thr)

        if best_thr is None:
            results.append({"window": i, "status": "no_feasible_thr",
                            "test_start": str(w["test_start"].date()),
                            "test_end": str(w["test_end"].date())})
            print(f"    W{i}: no feasible threshold")
            continue

        # Test
        test_bars = _slice(btc, w["test_start"], w["test_end"])
        test_f = all_feats[(all_feats["ts"] >= w["test_start"]) & (all_feats["ts"] < w["test_end"])]
        test_f = test_f.dropna(subset=FEAT_NAMES).reset_index(drop=True)

        if len(test_f) < 10 or len(test_bars) < 10:
            results.append({"window": i, "status": "skip_small_test"})
            continue

        p_test = _p_up(mdl, test_f[FEAT_NAMES].values)
        sig_test = _build_signals(test_f["ts"], p_test, best_thr)
        _, _, met = run_engine(test_bars, sig_test, ENGINE_CFG)

        # Baseline
        up_sig = pd.Series("up", index=pd.DatetimeIndex(
            pd.to_datetime(test_bars["ts"].values, utc=True)), dtype="object")
        _, _, base_m = run_engine(test_bars, up_sig, ENGINE_CFG)

        dd_pass = met["max_drawdown"] <= MAX_DD
        beats = met["final_equity"] > base_m["final_equity"]
        status = "PASS" if (dd_pass and beats) else "FAIL"

        results.append({
            "window": i,
            "test_start": str(w["test_start"].date()),
            "test_end": str(w["test_end"].date()),
            "threshold": best_thr,
            "equity": met["final_equity"],
            "ret": met["total_return"],
            "dd": met["max_drawdown"],
            "trades": met["num_trades"],
            "wins": met["num_wins"],
            "baseline_eq": base_m["final_equity"],
            "status": status,
        })

        print(f"    W{i}: thr={best_thr} eq=${met['final_equity']:.0f} "
              f"dd={met['max_drawdown']:.3f} tr={met['num_trades']} → {status}")

    # ── Summary ──────────────────────────────────────────────────────
    print(f"\n{'='*70}")
    print(f"  DERIVS V1 ADAPTIVE WALKFORWARD SUMMARY")
    print(f"{'='*70}")
    print(f"  {'W':>3s} {'period':>25s} {'thr':>5s} {'equity':>8s} {'ret':>8s} {'dd':>6s} {'tr':>4s} {'gate':>8s}")
    print(f"  {'-'*3} {'-'*25} {'-'*5} {'-'*8} {'-'*8} {'-'*6} {'-'*4} {'-'*8}")

    n_pass = n_fail = n_skip = 0
    for r in results:
        if "equity" not in r:
            n_skip += 1
            ts = r.get("test_start", "?")
            te = r.get("test_end", "?")
            print(f"  {r['window']:>3d} {ts+'->'+te:>25s}   ---      ---      ---    ---   --- {r['status']:>8s}")
        else:
            period = f"{r['test_start']}->{r['test_end']}"
            print(f"  {r['window']:>3d} {period:>25s} {r['threshold']:>5.2f} ${r['equity']:>7.0f} ${r['ret']:>7.0f} "
                  f"{r['dd']:>5.3f} {r['trades']:>4d} {r['status']:>8s}")
            if r["status"] == "PASS":
                n_pass += 1
            else:
                n_fail += 1

    print(f"\n  PASS: {n_pass}  FAIL: {n_fail}  SKIP: {n_skip}")
    print(f"  BASELINE (no derivs): PASS=3 (2 hollow), FAIL=9")
    improvement = n_pass > 3
    print(f"  IMPROVED: {'YES' if improvement else 'NO'}")
    print(f"{'='*70}")

    Path(REPORTS_DIR / "experiment_results.json").write_text(
        json.dumps(results, indent=2, default=str) + "\n"
    )
    print(f"  WROTE {REPORTS_DIR}/experiment_results.json")


if __name__ == "__main__":
    main()

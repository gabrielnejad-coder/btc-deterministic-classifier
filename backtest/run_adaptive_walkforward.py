"""
Layer 4 — Adaptive Rolling Walkforward.

Frozen decisions:
  - Model: HistGradientBoostingClassifier + isotonic calibration
  - Features: leaders_v1 (15 features, BTC/ETH/SOL)
  - Labels: 12h forward return, ±0.2% threshold
  - Policy: long-only, p_up >= trade_threshold → "up"

Rolling schedule (V1.1):
  - Train window: 12 months trailing
  - Retrain cadence: every 3 months
  - Threshold selection: next 1 month after train (validate slice)
  - Forward test: next 3 months after validate (until next retrain)
  - Gap: 12 bars between train end and validate start (label safety)

Each segment is independently gated:
  - max drawdown <= 10%
  - must beat always_long baseline after costs

Drift metrics logged per window:
  - p_up distribution (mean, std, >0.45 count)
  - feature z-score shift vs train
  - trade frequency
  - segment drawdown
"""
from __future__ import annotations

import json
import pickle
from pathlib import Path
from typing import Any, Dict, List, Tuple

import numpy as np
import pandas as pd
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.calibration import CalibratedClassifierCV

from backtest.engine import EngineConfig, run_engine
from features.build_features_leaders_v1 import build_features_leaders_v1


# ─── Frozen Configuration ────────────────────────────────────────────

REPORTS_DIR = Path("reports/adaptive")

FEATURE_NAMES = [
    "BTC_ret_1", "BTC_ret_4", "BTC_ret_24", "BTC_vol_24", "BTC_vol_chg_1",
    "ETH_ret_1", "ETH_ret_4", "ETH_ret_24", "ETH_vol_24", "ETH_vol_chg_1",
    "SOL_ret_1", "SOL_ret_4", "SOL_ret_24", "SOL_vol_24", "SOL_vol_chg_1",
]

ENGINE_CFG = EngineConfig(
    fee_taker=0.0004,
    slippage_side=0.0001,
    stop_loss_pct=0.02,
    hold_min_bars=12,
    initial_equity=1000.0,
    one_position=True,
)

# Rolling schedule V1.1
TRAIN_MONTHS = 12
RETRAIN_CADENCE_MONTHS = 3
VALIDATE_MONTHS = 1
LABEL_GAP_BARS = 12
LABEL_HORIZON = 12
LABEL_THRESHOLD = 0.002

# GBDT hyperparams (frozen from train_v3_gbdt.py)
GBDT_PARAMS = dict(
    max_iter=300,
    max_depth=4,
    learning_rate=0.05,
    min_samples_leaf=50,
    max_leaf_nodes=31,
    l2_regularization=1.0,
    random_state=42,
)

# Governance gates
MAX_DD = 0.10
THRESHOLD_GRID = np.round(np.arange(0.40, 0.56, 0.01), 2)
MIN_TRADES_VALIDATE = 3


# ─── Helpers ─────────────────────────────────────────────────────────

def _make_labels(df: pd.DataFrame) -> pd.Series:
    close = df["close"].astype(float)
    fwd = close.shift(-LABEL_HORIZON) / close - 1.0
    y = pd.Series("flat", index=df.index, dtype="object")
    y[fwd > LABEL_THRESHOLD] = "up"
    y[fwd < -LABEL_THRESHOLD] = "down"
    y[fwd.isna()] = None
    return y


def _build_signals(ts, p_up, threshold):
    idx = pd.DatetimeIndex(pd.to_datetime(ts.values, utc=True), name="ts")
    values = np.where(p_up >= threshold, "up", "flat")
    return pd.Series(values, index=idx, dtype="object", name="signal")


def _train_model(X, y):
    clf = HistGradientBoostingClassifier(**GBDT_PARAMS)
    clf.fit(X, y)
    cal = CalibratedClassifierCV(clf, cv=3, method="isotonic")
    cal.fit(X, y)
    return cal


def _predict_p_up(model, X):
    classes = list(model.classes_)
    up_idx = classes.index("up")
    return model.predict_proba(X)[:, up_idx]


def _compute_drift(train_X, test_X):
    """Per-feature z-score shift: how many train-stds has the test mean moved."""
    train_mean = train_X.mean(axis=0)
    train_std = train_X.std(axis=0)
    train_std = np.where(train_std == 0, 1.0, train_std)
    test_mean = test_X.mean(axis=0)
    shift = (test_mean - train_mean) / train_std
    return {
        "max_abs_z_shift": float(np.max(np.abs(shift))),
        "mean_abs_z_shift": float(np.mean(np.abs(shift))),
        "per_feature": {FEATURE_NAMES[i]: float(shift[i]) for i in range(len(FEATURE_NAMES))},
    }


def _generate_windows(all_ts: pd.Series) -> List[Dict[str, pd.Timestamp]]:
    """Generate rolling retrain windows from the data timeline."""
    min_ts = all_ts.min()
    max_ts = all_ts.max()

    # First train window starts at data start
    # First retrain point = data start + train_months
    first_retrain = min_ts + pd.DateOffset(months=TRAIN_MONTHS)

    windows = []
    cursor = first_retrain

    while cursor < max_ts:
        train_start = cursor - pd.DateOffset(months=TRAIN_MONTHS)
        train_end = cursor
        val_start = train_end + pd.Timedelta(hours=LABEL_GAP_BARS)
        val_end = val_start + pd.DateOffset(months=VALIDATE_MONTHS)
        test_start = val_end
        test_end = test_start + pd.DateOffset(months=RETRAIN_CADENCE_MONTHS)

        # Clip to data bounds
        if test_start >= max_ts:
            break

        test_end = min(test_end, max_ts)

        windows.append({
            "train_start": train_start,
            "train_end": train_end,
            "val_start": val_start,
            "val_end": val_end,
            "test_start": test_start,
            "test_end": test_end,
        })

        cursor += pd.DateOffset(months=RETRAIN_CADENCE_MONTHS)

    return windows


def _slice_df(df: pd.DataFrame, start: pd.Timestamp, end: pd.Timestamp) -> pd.DataFrame:
    mask = (df["ts"] >= start) & (df["ts"] < end)
    return df.loc[mask].copy().reset_index(drop=True)


# ─── Main ────────────────────────────────────────────────────────────

def main() -> None:
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    # Load data
    btc = pd.read_parquet("data_parquet/BTCUSD_USD_1h_20220323_now.parquet").copy()
    eth = pd.read_parquet("data_parquet/ETH_1h.parquet").copy()
    sol = pd.read_parquet("data_parquet/SOL_1h.parquet").copy()

    btc["ts"] = pd.to_datetime(btc["ts"], utc=True)
    btc = btc.sort_values("ts").reset_index(drop=True)

    # Build ALL features once (sliced per window)
    all_feats = build_features_leaders_v1(btc, eth, sol)
    all_feats = all_feats.dropna().reset_index(drop=True)
    all_feats["label"] = _make_labels(
        btc.set_index("ts").reindex(all_feats["ts"]).reset_index()
    )

    windows = _generate_windows(btc["ts"])

    print("ADAPTIVE ROLLING WALKFORWARD — V1.1")
    print(f"  train_window: {TRAIN_MONTHS}mo  retrain: every {RETRAIN_CADENCE_MONTHS}mo")
    print(f"  validate: {VALIDATE_MONTHS}mo  gap: {LABEL_GAP_BARS} bars")
    print(f"  windows: {len(windows)}")

    all_results = []
    cumulative_equity = ENGINE_CFG.initial_equity

    for i, w in enumerate(windows):
        seg_name = f"seg_{i:02d}"
        print(f"\n{'─'*65}")
        print(f"  WINDOW {i}: train [{w['train_start'].date()} → {w['train_end'].date()}]")
        print(f"            val   [{w['val_start'].date()} → {w['val_end'].date()}]")
        print(f"            test  [{w['test_start'].date()} → {w['test_end'].date()}]")

        # ── Slice data ───────────────────────────────────────────────
        train_feats = all_feats[
            (all_feats["ts"] >= w["train_start"]) & (all_feats["ts"] < w["train_end"])
        ].copy().reset_index(drop=True)

        val_bars = _slice_df(btc, w["val_start"], w["val_end"])
        val_feats = all_feats[
            (all_feats["ts"] >= w["val_start"]) & (all_feats["ts"] < w["val_end"])
        ].copy().reset_index(drop=True)

        test_bars = _slice_df(btc, w["test_start"], w["test_end"])
        test_feats = all_feats[
            (all_feats["ts"] >= w["test_start"]) & (all_feats["ts"] < w["test_end"])
        ].copy().reset_index(drop=True)

        # ── Train ────────────────────────────────────────────────────
        train_clean = train_feats.dropna(subset=["label", *FEATURE_NAMES]).reset_index(drop=True)
        if len(train_clean) < 500:
            print(f"    SKIP: only {len(train_clean)} train rows")
            continue

        X_train = train_clean[FEATURE_NAMES].astype(float).values
        y_train = train_clean["label"].values

        model = _train_model(X_train, y_train)
        classes = list(model.classes_)
        print(f"    trained: {len(train_clean)} rows, classes={classes}")

        # ── Threshold sweep on validate ──────────────────────────────
        if len(val_feats) < 50 or len(val_bars) < 50:
            print(f"    SKIP: validate too small ({len(val_feats)} rows)")
            continue

        X_val = val_feats[FEATURE_NAMES].astype(float).values
        p_up_val = _predict_p_up(model, X_val)

        best_thr = None
        best_eq = 0.0

        for thr in THRESHOLD_GRID:
            sig = _build_signals(val_feats["ts"], p_up_val, float(thr))
            _, _, m = run_engine(val_bars, sig, ENGINE_CFG)
            if m["max_drawdown"] <= MAX_DD and m["num_trades"] >= MIN_TRADES_VALIDATE:
                if m["final_equity"] > best_eq:
                    best_eq = m["final_equity"]
                    best_thr = float(thr)

        if best_thr is None:
            print(f"    SKIP: no feasible threshold on validate")
            # Log as failed window
            all_results.append({
                "window": i,
                "train_start": str(w["train_start"].date()),
                "test_start": str(w["test_start"].date()),
                "test_end": str(w["test_end"].date()),
                "status": "no_feasible_threshold",
                "threshold": None,
                "test_equity": None,
                "test_return": None,
                "test_dd": None,
                "test_trades": None,
            })
            continue

        print(f"    threshold: {best_thr} (val equity: ${best_eq:.2f})")

        # ── Forward test ─────────────────────────────────────────────
        if len(test_feats) < 10 or len(test_bars) < 10:
            print(f"    SKIP: test too small ({len(test_bars)} bars)")
            continue

        X_test = test_feats[FEATURE_NAMES].astype(float).values
        p_up_test = _predict_p_up(model, X_test)

        sig_test = _build_signals(test_feats["ts"], p_up_test, best_thr)
        trades_df, equity_df, metrics = run_engine(test_bars, sig_test, ENGINE_CFG)

        # ── Drift metrics ────────────────────────────────────────────
        drift = _compute_drift(X_train, X_test)
        p_up_drift = {
            "train_mean": float(np.mean(_predict_p_up(model, X_train))),
            "test_mean": float(np.mean(p_up_test)),
            "test_std": float(np.std(p_up_test)),
            "test_above_thr": int((p_up_test >= best_thr).sum()),
        }

        # ── Baseline: always long ────────────────────────────────────
        always_up_sig = pd.Series(
            "up",
            index=pd.DatetimeIndex(pd.to_datetime(test_bars["ts"].values, utc=True)),
            dtype="object",
        )
        _, _, baseline_m = run_engine(test_bars, always_up_sig, ENGINE_CFG)

        # ── Gate check ───────────────────────────────────────────────
        dd_pass = metrics["max_drawdown"] <= MAX_DD
        beats_baseline = metrics["final_equity"] > baseline_m["final_equity"]
        gate_pass = dd_pass and beats_baseline

        status = "PASS" if gate_pass else "FAIL"
        if not dd_pass:
            status += "_DD"
        if not beats_baseline:
            status += "_BASELINE"

        sig_counts = pd.Series(sig_test.values).value_counts().to_dict()

        print(f"    test: equity=${metrics['final_equity']:.2f}  "
              f"ret=${metrics['total_return']:.2f}  "
              f"dd={metrics['max_drawdown']:.4f}  "
              f"trades={metrics['num_trades']}  "
              f"wins={metrics['num_wins']}")
        print(f"    baseline: equity=${baseline_m['final_equity']:.2f}")
        print(f"    drift: max_z={drift['max_abs_z_shift']:.2f}  "
              f"mean_z={drift['mean_abs_z_shift']:.2f}  "
              f"p_up_shift={p_up_drift['test_mean'] - p_up_drift['train_mean']:.4f}")
        print(f"    GATE: {status}")

        # ── Save segment ─────────────────────────────────────────────
        trades_df.to_parquet(REPORTS_DIR / f"{seg_name}_trades.parquet", index=False)
        equity_df.to_parquet(REPORTS_DIR / f"{seg_name}_equity.parquet", index=False)

        seg_result = {
            "window": i,
            "train_start": str(w["train_start"].date()),
            "train_end": str(w["train_end"].date()),
            "test_start": str(w["test_start"].date()),
            "test_end": str(w["test_end"].date()),
            "status": status,
            "threshold": best_thr,
            "signals": sig_counts,
            "test_equity": metrics["final_equity"],
            "test_return": metrics["total_return"],
            "test_dd": metrics["max_drawdown"],
            "test_trades": metrics["num_trades"],
            "test_wins": metrics["num_wins"],
            "test_fees": metrics["total_fees"],
            "baseline_equity": baseline_m["final_equity"],
            "drift": drift,
            "p_up_drift": p_up_drift,
        }
        all_results.append(seg_result)

    # ─── Summary ─────────────────────────────────────────────────────
    print(f"\n{'='*75}")
    print(f"  ADAPTIVE WALKFORWARD SUMMARY")
    print(f"{'='*75}")
    print(f"  {'win':>4s} {'period':>25s} {'thr':>5s} {'equity':>9s} {'ret':>9s} {'dd':>7s} {'trades':>7s} {'gate':>12s}")
    print(f"  {'-'*4} {'-'*25} {'-'*5} {'-'*9} {'-'*9} {'-'*7} {'-'*7} {'-'*12}")

    n_pass = 0
    n_fail = 0
    n_skip = 0

    for r in all_results:
        if r["test_equity"] is None:
            n_skip += 1
            period = f"{r['train_start']}→{r['test_end']}"
            print(f"  {r['window']:>4d} {period:>25s}   ---       ---       ---     ---     --- {r['status']:>12s}")
        else:
            period = f"{r['test_start']}→{r['test_end']}"
            print(f"  {r['window']:>4d} {period:>25s} {r['threshold']:>5.2f} ${r['test_equity']:>8.2f} ${r['test_return']:>8.2f} {r['test_dd']:>6.4f} {r['test_trades']:>7d} {r['status']:>12s}")
            if "PASS" in r["status"]:
                n_pass += 1
            else:
                n_fail += 1

    print(f"\n  PASS: {n_pass}  FAIL: {n_fail}  SKIP: {n_skip}")
    print(f"{'='*75}")

    # Save full audit log
    Path(REPORTS_DIR / "adaptive_walkforward_results.json").write_text(
        json.dumps(all_results, indent=2, default=str) + "\n"
    )
    print(f"\n  WROTE {REPORTS_DIR}/adaptive_walkforward_results.json")


if __name__ == "__main__":
    main()

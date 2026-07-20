"""
Monthly retrain experiment.

Tests: is the 12h signal real but decaying faster than 3-month cadence?

Changes vs previous experiment:
  RETRAIN_MONTHS: 3 → 1
  Test window: 3 months → 1 month

Everything else identical:
  - Same features (pack_v1_derivs, 22 features)
  - Same model (GBDT + isotonic calibration)
  - Same labels (12h, ±0.2%)
  - Same engine config
  - Same gates (dd ≤ 10%, beat always_long)
  - Same threshold grid
"""
from __future__ import annotations

import json
from pathlib import Path
from dateutil.relativedelta import relativedelta

import numpy as np
import pandas as pd
from scipy.stats import spearmanr
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.calibration import CalibratedClassifierCV

from backtest.engine import EngineConfig, run_engine
from backtest.walkforward import split_walkforward
from features.packs.pack_v1_derivs import build_features_pack_v1, FEATURE_NAMES_PACK_V1


REPORTS_DIR = Path("reports/monthly_retrain")
DATA_DIR = Path("data_parquet")

ENGINE_CFG = EngineConfig(
    fee_taker=0.0004, slippage_side=0.0001, stop_loss_pct=0.02,
    hold_min_bars=12, initial_equity=1000.0, one_position=True,
)

GBDT_PARAMS = dict(
    max_iter=300, max_depth=4, learning_rate=0.05,
    min_samples_leaf=50, max_leaf_nodes=31,
    l2_regularization=1.0, random_state=42,
)

FEAT_NAMES = FEATURE_NAMES_PACK_V1
LABEL_HORIZON = 12
LABEL_THRESHOLD = 0.002
MAX_DD = 0.10
THRESHOLD_GRID = np.round(np.arange(0.40, 0.56, 0.01), 2)
MIN_TRADES_VAL = 3

# ── THE ONLY CHANGE ─────────────────────────────────────────────────
TRAIN_MONTHS = 12
RETRAIN_MONTHS = 1    # was 3
VAL_MONTHS = 1
TEST_MONTHS = 1       # was 3 (= RETRAIN_MONTHS)
GAP_BARS = 12
# ─────────────────────────────────────────────────────────────────────


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
        w = {
            "train_start": cursor - pd.DateOffset(months=TRAIN_MONTHS),
            "train_end": cursor,
            "val_start": cursor + pd.Timedelta(hours=GAP_BARS),
            "val_end": cursor + pd.DateOffset(months=VAL_MONTHS),
            "test_start": cursor + pd.DateOffset(months=VAL_MONTHS),
            "test_end": cursor + pd.DateOffset(months=VAL_MONTHS + TEST_MONTHS),
        }
        if w["test_start"] >= end_ts:
            break
        w["test_end"] = min(w["test_end"], end_ts)
        yield w
        cursor += pd.DateOffset(months=RETRAIN_MONTHS)


def _slice(df, start, end):
    return df[(df["ts"] >= start) & (df["ts"] < end)].copy().reset_index(drop=True)


def main() -> None:
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    btc = pd.read_parquet(DATA_DIR / "BTCUSD_USD_1h_20220323_now.parquet").copy()
    eth = pd.read_parquet(DATA_DIR / "ETH_1h.parquet").copy()
    sol = pd.read_parquet(DATA_DIR / "SOL_1h.parquet").copy()
    btc["ts"] = pd.to_datetime(btc["ts"], utc=True)
    btc = btc.sort_values("ts").reset_index(drop=True)

    all_feats = build_features_pack_v1(btc, eth, sol)
    all_feats = all_feats.dropna(subset=FEAT_NAMES).reset_index(drop=True)

    # Labels
    btc_idx = btc.set_index("ts").sort_index()
    close_a = btc_idx["close"].reindex(all_feats["ts"]).values
    fwd = pd.Series(close_a).shift(-LABEL_HORIZON) / pd.Series(close_a) - 1.0
    labels = pd.Series("flat", index=all_feats.index, dtype="object")
    labels[fwd > LABEL_THRESHOLD] = "up"
    labels[fwd < -LABEL_THRESHOLD] = "down"
    labels[fwd.isna()] = None
    all_feats["label"] = labels.values

    windows = list(_months_range(btc["ts"].min(), btc["ts"].max()))

    print("MONTHLY RETRAIN EXPERIMENT")
    print(f"  train: {TRAIN_MONTHS}mo  val: {VAL_MONTHS}mo  test: {TEST_MONTHS}mo  retrain: every {RETRAIN_MONTHS}mo")
    print(f"  features: {len(FEAT_NAMES)} (derivs pack v1)")
    print(f"  windows: {len(windows)}")
    print(f"  gates: dd<={MAX_DD}, beats always_long, trades>={MIN_TRADES_VAL}")

    results = []
    for i, w in enumerate(windows):
        # Train
        tr = all_feats[(all_feats["ts"] >= w["train_start"]) & (all_feats["ts"] < w["train_end"])]
        tr = tr.dropna(subset=["label", *FEAT_NAMES]).reset_index(drop=True)
        if len(tr) < 500:
            results.append({"window": i, "status": "skip_train"})
            continue

        mdl = _train_model(tr[FEAT_NAMES].values, tr["label"].values)

        # Validate
        val_bars = _slice(btc, w["val_start"], w["val_end"])
        val_f = all_feats[(all_feats["ts"] >= w["val_start"]) & (all_feats["ts"] < w["val_end"])]
        val_f = val_f.dropna(subset=FEAT_NAMES).reset_index(drop=True)
        if len(val_f) < 30 or len(val_bars) < 30:
            results.append({"window": i, "status": "skip_val"})
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
            results.append({
                "window": i, "status": "no_thr",
                "test_start": str(w["test_start"].date()),
                "test_end": str(w["test_end"].date()),
            })
            continue

        # Test
        test_bars = _slice(btc, w["test_start"], w["test_end"])
        test_f = all_feats[(all_feats["ts"] >= w["test_start"]) & (all_feats["ts"] < w["test_end"])]
        test_f = test_f.dropna(subset=FEAT_NAMES).reset_index(drop=True)
        if len(test_f) < 10 or len(test_bars) < 10:
            results.append({"window": i, "status": "skip_test"})
            continue

        p_test = _p_up(mdl, test_f[FEAT_NAMES].values)
        sig_test = _build_signals(test_f["ts"], p_test, best_thr)
        _, _, met = run_engine(test_bars, sig_test, ENGINE_CFG)

        # Baseline
        up_sig = pd.Series("up", index=pd.DatetimeIndex(
            pd.to_datetime(test_bars["ts"].values, utc=True)), dtype="object")
        _, _, base_m = run_engine(test_bars, up_sig, ENGINE_CFG)

        dd_ok = met["max_drawdown"] <= MAX_DD
        beats = met["final_equity"] > base_m["final_equity"]
        real_trades = met["num_trades"] >= 3
        status = "PASS" if (dd_ok and beats and real_trades) else "FAIL"
        if not real_trades and dd_ok:
            status = "HOLLOW"

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

    # ── Summary ──────────────────────────────────────────────────────
    print(f"\n{'='*75}")
    print(f"  {'W':>3s} {'period':>25s} {'thr':>5s} {'eq':>8s} {'ret':>8s} {'dd':>6s} {'tr':>4s} {'w':>3s} {'base':>8s} {'gate':>8s}")
    print(f"  {'-'*3} {'-'*25} {'-'*5} {'-'*8} {'-'*8} {'-'*6} {'-'*4} {'-'*3} {'-'*8} {'-'*8}")

    n_pass = n_fail = n_hollow = n_skip = 0
    pass_rets = []

    for r in results:
        if "equity" not in r:
            n_skip += 1
            ts = r.get("test_start", "?")
            te = r.get("test_end", "?")
            print(f"  {r['window']:>3d} {(ts+'->'+te):>25s}   ---      ---      ---    ---  ---      --- {r['status']:>8s}")
        else:
            period = f"{r['test_start']}->{r['test_end']}"
            print(f"  {r['window']:>3d} {period:>25s} {r['threshold']:>5.2f} "
                  f"${r['equity']:>7.0f} ${r['ret']:>7.0f} {r['dd']:>5.3f} "
                  f"{r['trades']:>4d} {r['wins']:>3d} ${r['baseline_eq']:>7.0f} {r['status']:>8s}")
            if r["status"] == "PASS":
                n_pass += 1
                pass_rets.append(r["ret"])
            elif r["status"] == "HOLLOW":
                n_hollow += 1
            else:
                n_fail += 1

    avg_pass_ret = np.mean(pass_rets) if pass_rets else 0

    print(f"\n  PASS: {n_pass}  HOLLOW: {n_hollow}  FAIL: {n_fail}  SKIP: {n_skip}")
    print(f"  avg PASS return: ${avg_pass_ret:.2f}")
    print(f"\n  COMPARISON:")
    print(f"    quarterly retrain (derivs):  PASS=4 (0 profitable), FAIL=8")
    print(f"    monthly retrain (derivs):    PASS={n_pass}, HOLLOW={n_hollow}, FAIL={n_fail}")

    if n_pass >= 7:
        print(f"\n  VERDICT: CADENCE WAS THE ISSUE. Keep 12h, use monthly retrain.")
    elif n_pass > 4:
        print(f"\n  VERDICT: MARGINAL IMPROVEMENT. Signal decays fast but may still be weak.")
    else:
        print(f"\n  VERDICT: CADENCE DID NOT FIX IT. 12h horizon likely structurally weak.")
    print(f"{'='*75}")

    Path(REPORTS_DIR / "monthly_retrain_results.json").write_text(
        json.dumps(results, indent=2, default=str) + "\n"
    )
    print(f"  WROTE {REPORTS_DIR}/monthly_retrain_results.json")


if __name__ == "__main__":
    main()

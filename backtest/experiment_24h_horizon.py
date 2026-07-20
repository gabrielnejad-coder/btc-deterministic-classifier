"""
Horizon Experiment: 24h forward direction.

Changes vs monthly retrain experiment:
  LABEL_HORIZON: 12 → 24 (bars)

Everything else frozen:
  - Same features (pack_v1_derivs, 22 features)
  - Same model (GBDT + isotonic)
  - Same engine config
  - Same monthly retrain schedule
  - Same gates (dd ≤ 10%, beat always_long, trades ≥ 3)
  - Same threshold grid

If 24h works materially better, the issue was noise at 12h.
If 24h also fails, directional classification on BTC is structurally
weak at retail fee levels with available features.
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
from features.packs.pack_v1_derivs import build_features_pack_v1, FEATURE_NAMES_PACK_V1


REPORTS_DIR = Path("reports/horizon_24h")
DATA_DIR = Path("data_parquet")

ENGINE_CFG = EngineConfig(
    fee_taker=0.0004, slippage_side=0.0001, stop_loss_pct=0.02,
    hold_min_bars=24, initial_equity=1000.0, one_position=True,
)

GBDT_PARAMS = dict(
    max_iter=300, max_depth=4, learning_rate=0.05,
    min_samples_leaf=50, max_leaf_nodes=31,
    l2_regularization=1.0, random_state=42,
)

FEAT_NAMES = FEATURE_NAMES_PACK_V1

# ── THE ONLY CHANGES ────────────────────────────────────────────────
LABEL_HORIZON = 24        # was 12
LABEL_THRESHOLD = 0.004   # doubled from 0.002 to match wider horizon
# hold_min_bars also 24 (in ENGINE_CFG above) to match horizon
# ─────────────────────────────────────────────────────────────────────

TRAIN_MONTHS = 12
RETRAIN_MONTHS = 1
VAL_MONTHS = 1
TEST_MONTHS = 1
GAP_BARS = 24
MAX_DD = 0.10
THRESHOLD_GRID = np.round(np.arange(0.40, 0.56, 0.01), 2)
MIN_TRADES_VAL = 3


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

    # Labels with 24h horizon
    btc_idx = btc.set_index("ts").sort_index()
    close_a = btc_idx["close"].reindex(all_feats["ts"]).values
    fwd = pd.Series(close_a).shift(-LABEL_HORIZON) / pd.Series(close_a) - 1.0
    labels = pd.Series("flat", index=all_feats.index, dtype="object")
    labels[fwd > LABEL_THRESHOLD] = "up"
    labels[fwd < -LABEL_THRESHOLD] = "down"
    labels[fwd.isna()] = None
    all_feats["label"] = labels.values

    label_counts = all_feats["label"].value_counts()
    print(f"24H HORIZON EXPERIMENT")
    print(f"  label_horizon: {LABEL_HORIZON} bars (24h)")
    print(f"  label_threshold: +/-{LABEL_THRESHOLD}")
    print(f"  hold_min_bars: {ENGINE_CFG.hold_min_bars}")
    print(f"  gap_bars: {GAP_BARS}")
    print(f"  label distribution: {label_counts.to_dict()}")
    print(f"  features: {len(FEAT_NAMES)}")

    # ── Calibration on train split ───────────────────────────────────
    from backtest.walkforward import split_walkforward
    splits = split_walkforward(btc)
    train_btc = splits["train"]
    train_feats = all_feats[
        (all_feats["ts"] >= train_btc["ts"].min()) &
        (all_feats["ts"] <= train_btc["ts"].max())
    ].copy()
    train_clean = train_feats.dropna(subset=["label", *FEAT_NAMES]).reset_index(drop=True)

    X_tr = train_clean[FEAT_NAMES].astype(float).values
    y_tr = np.array(train_clean["label"].values)
    model = _train_model(X_tr, y_tr)
    p_up_tr = _p_up(model, X_tr)

    close_tr = btc_idx["close"].reindex(pd.to_datetime(train_clean["ts"].values, utc=True))
    fwd_tr = (close_tr.shift(-LABEL_HORIZON) / close_tr - 1.0).values
    cal = pd.DataFrame({"p_up": p_up_tr, "fwd": fwd_tr}).dropna()
    cal["dec"] = pd.qcut(cal["p_up"], 10, labels=False, duplicates="drop")

    print(f"\n  CALIBRATION (train, {len(cal)} rows)")
    print(f"  {'d':>3s} {'p_lo':>7s} {'p_hi':>7s} {'avg_ret':>9s} {'hit':>6s}")
    for d in sorted(cal["dec"].unique()):
        s = cal[cal["dec"] == d]
        print(f"  {d:>3d} {s['p_up'].min():>7.4f} {s['p_up'].max():>7.4f} "
              f"{s['fwd'].mean():>9.5f} {(s['fwd']>0).mean():>6.3f}")

    rho, pval = spearmanr(cal["p_up"], cal["fwd"])
    avgs = [cal[cal["dec"]==d]["fwd"].mean() for d in sorted(cal["dec"].unique())]
    mono = sum(1 for j in range(1, len(avgs)) if avgs[j] > avgs[j-1]) / max(len(avgs)-1, 1)
    spread = avgs[-1] - avgs[0]
    print(f"\n  Spearman: {rho:.4f} (p={pval:.2e})")
    print(f"  Monotonicity: {mono:.2f}")
    print(f"  Spread: {spread:+.5f}")
    print(f"  (12h baseline: rho=0.6461, mono=0.89, spread=+0.04179)")

    # ── Adaptive walkforward ─────────────────────────────────────────
    windows = list(_months_range(btc["ts"].min(), btc["ts"].max()))
    print(f"\n  ADAPTIVE WALKFORWARD ({len(windows)} windows, monthly retrain)")

    results = []
    for i, w in enumerate(windows):
        tr = all_feats[(all_feats["ts"] >= w["train_start"]) & (all_feats["ts"] < w["train_end"])]
        tr = tr.dropna(subset=["label", *FEAT_NAMES]).reset_index(drop=True)
        if len(tr) < 500:
            results.append({"window": i, "status": "skip"})
            continue

        mdl = _train_model(tr[FEAT_NAMES].values, np.array(tr["label"].values))

        val_bars = _slice(btc, w["val_start"], w["val_end"])
        val_f = all_feats[(all_feats["ts"] >= w["val_start"]) & (all_feats["ts"] < w["val_end"])]
        val_f = val_f.dropna(subset=FEAT_NAMES).reset_index(drop=True)
        if len(val_f) < 30 or len(val_bars) < 30:
            results.append({"window": i, "status": "skip"})
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
            results.append({"window": i, "status": "no_thr",
                            "test_start": str(w["test_start"].date()),
                            "test_end": str(w["test_end"].date())})
            continue

        test_bars = _slice(btc, w["test_start"], w["test_end"])
        test_f = all_feats[(all_feats["ts"] >= w["test_start"]) & (all_feats["ts"] < w["test_end"])]
        test_f = test_f.dropna(subset=FEAT_NAMES).reset_index(drop=True)
        if len(test_f) < 10 or len(test_bars) < 10:
            results.append({"window": i, "status": "skip"})
            continue

        p_test = _p_up(mdl, test_f[FEAT_NAMES].values)
        sig_test = _build_signals(test_f["ts"], p_test, best_thr)
        _, _, met = run_engine(test_bars, sig_test, ENGINE_CFG)

        up_sig = pd.Series("up", index=pd.DatetimeIndex(
            pd.to_datetime(test_bars["ts"].values, utc=True)), dtype="object")
        _, _, base_m = run_engine(test_bars, up_sig, ENGINE_CFG)

        dd_ok = met["max_drawdown"] <= MAX_DD
        beats = met["final_equity"] > base_m["final_equity"]
        real = met["num_trades"] >= 3
        status = "PASS" if (dd_ok and beats and real) else ("HOLLOW" if (dd_ok and not real) else "FAIL")

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
    print(f"  {'W':>3s} {'period':>25s} {'thr':>5s} {'eq':>8s} {'ret':>8s} {'dd':>6s} {'tr':>4s} {'w':>3s} {'gate':>8s}")
    print(f"  {'-'*3} {'-'*25} {'-'*5} {'-'*8} {'-'*8} {'-'*6} {'-'*4} {'-'*3} {'-'*8}")

    n_pass = n_fail = n_hollow = n_skip = 0
    pass_rets = []
    total_ret_traded = 0
    traded_count = 0

    for r in results:
        if "equity" not in r:
            n_skip += 1
            ts = r.get("test_start", "?")
            te = r.get("test_end", "?")
            print(f"  {r['window']:>3d} {(ts+'->'+te):>25s}   ---      ---      ---    ---  --- {r['status']:>8s}")
        else:
            period = f"{r['test_start']}->{r['test_end']}"
            print(f"  {r['window']:>3d} {period:>25s} {r['threshold']:>5.2f} "
                  f"${r['equity']:>7.0f} ${r['ret']:>7.0f} {r['dd']:>5.3f} "
                  f"{r['trades']:>4d} {r['wins']:>3d} {r['status']:>8s}")
            total_ret_traded += r["ret"]
            traded_count += 1
            if r["status"] == "PASS":
                n_pass += 1
                pass_rets.append(r["ret"])
            elif r["status"] == "HOLLOW":
                n_hollow += 1
            else:
                n_fail += 1

    avg_pass = np.mean(pass_rets) if pass_rets else 0
    avg_all = total_ret_traded / traded_count if traded_count else 0

    print(f"\n  PASS: {n_pass}  HOLLOW: {n_hollow}  FAIL: {n_fail}  SKIP: {n_skip}")
    print(f"  avg PASS return: ${avg_pass:.2f}")
    print(f"  avg ALL traded return: ${avg_all:.2f}")

    print(f"\n  COMPARISON (12h monthly retrain with derivs):")
    print(f"    12h: PASS=12, HOLLOW=6, FAIL=16, avg PASS ret=$10.47")
    print(f"    24h: PASS={n_pass}, HOLLOW={n_hollow}, FAIL={n_fail}, avg PASS ret=${avg_pass:.2f}")

    if n_pass > 12 and avg_pass > 10.47:
        print(f"\n  VERDICT: 24H IS BETTER. Horizon was the bottleneck.")
    elif n_pass > 12 or avg_pass > 20:
        print(f"\n  VERDICT: 24H SHOWS IMPROVEMENT. Worth further investigation.")
    elif n_pass >= 10:
        print(f"\n  VERDICT: 24H COMPARABLE. No clear advantage over 12h.")
    else:
        print(f"\n  VERDICT: 24H ALSO FAILS. Directional classification likely")
        print(f"           structurally weak for BTC at retail fee levels.")
    print(f"{'='*75}")

    Path(REPORTS_DIR / "horizon_24h_results.json").write_text(
        json.dumps(results, indent=2, default=str) + "\n"
    )
    print(f"  WROTE {REPORTS_DIR}/horizon_24h_results.json")


if __name__ == "__main__":
    main()

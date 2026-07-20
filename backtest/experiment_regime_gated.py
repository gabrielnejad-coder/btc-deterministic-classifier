"""
Regime-Gated Experiment.

Hypothesis: the 24h model's edge is conditional on regime.
It works in some market states, bleeds in others.
If we only trade in favorable regimes, edge > friction.

Regime filter (simple, deterministic, no lookahead):
  A bar is "tradeable" if ANY of:
    1. BTC 24h realized vol is above 60th percentile of trailing 30d
    2. BTC ATR expansion ratio (1h ATR / 24h ATR) > 1.5
    3. Absolute funding rate > 75th percentile of trailing 30d

  If NONE of these: regime = "quiet" → stay flat regardless of model.

This is applied AFTER the model scores, BEFORE signals go to engine.
The model, features, threshold selection — all unchanged from 24h wide experiment.
We just mask signals to zero in quiet regimes.

Replays the same 34-window monthly retrain pipeline.
"""
from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.calibration import CalibratedClassifierCV
from scipy.stats import spearmanr

from backtest.engine import EngineConfig, run_engine
from backtest.walkforward import split_walkforward
from features.packs.pack_v1_derivs import build_features_pack_v1, FEATURE_NAMES_PACK_V1


REPORTS_DIR = Path("reports/regime_gated")
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

FEAT_NAMES = FEATURE_NAMES_PACK_V1  # 22 features (pack v1, NOT v2)
LABEL_HORIZON = 24
LABEL_THRESHOLD = 0.004
TRAIN_MONTHS = 12
RETRAIN_MONTHS = 1
VAL_MONTHS = 1
TEST_MONTHS = 1
GAP_BARS = 24
MAX_DD = 0.10
THRESHOLD_GRID = np.round(np.arange(0.40, 0.56, 0.01), 2)
MIN_TRADES_VAL = 3


def _build_regime_mask(btc_df: pd.DataFrame, feat_ts: pd.Series) -> pd.Series:
    """
    Compute regime filter. Returns boolean Series aligned to feat_ts.
    True = tradeable regime. False = quiet, stay flat.

    All lookbacks are trailing only. No lookahead.
    """
    btc = btc_df.copy()
    btc["ts"] = pd.to_datetime(btc["ts"], utc=True)
    btc = btc.set_index("ts").sort_index()

    close = btc["close"].astype(float)
    high = btc["high"].astype(float)
    low = btc["low"].astype(float)

    # 1. Realized vol (24h rolling std of hourly returns)
    ret_1h = close.pct_change()
    rvol_24h = ret_1h.rolling(24).std()
    # Rolling percentile over 30 days (720 hours)
    rvol_pctl = rvol_24h.rolling(720).apply(
        lambda x: pd.Series(x).rank(pct=True).iloc[-1], raw=False
    )
    vol_active = rvol_pctl >= 0.60

    # 2. ATR expansion
    atr_1h = high - low
    atr_24h = atr_1h.rolling(24).mean()
    atr_ratio = atr_1h / atr_24h.replace(0, np.nan)
    atr_expanded = atr_ratio >= 1.5

    # 3. Funding extreme (need canonical derivs)
    derivs_path = DATA_DIR / "canonical_derivs_1h.parquet"
    if derivs_path.exists():
        derivs = pd.read_parquet(derivs_path)
        derivs["ts"] = pd.to_datetime(derivs["ts"], utc=True)
        derivs = derivs.set_index("ts").sort_index()
        funding = derivs["BTC_funding"].reindex(btc.index, method="ffill")
        funding_abs = funding.abs()
        funding_pctl = funding_abs.rolling(720).apply(
            lambda x: pd.Series(x).rank(pct=True).iloc[-1], raw=False
        )
        funding_extreme = funding_pctl >= 0.75
    else:
        funding_extreme = pd.Series(False, index=btc.index)

    # Combine: tradeable if ANY condition met
    tradeable = vol_active | atr_expanded | funding_extreme

    # Align to feature timestamps
    feat_ts_idx = pd.DatetimeIndex(pd.to_datetime(feat_ts.values, utc=True))
    aligned = tradeable.reindex(feat_ts_idx, method="ffill").fillna(False)

    return aligned


def _build_signals(ts, p_up, thr):
    idx = pd.DatetimeIndex(pd.to_datetime(ts.values, utc=True), name="ts")
    return pd.Series(np.where(p_up >= thr, "up", "flat"), index=idx, dtype="object")


def _apply_regime_mask(signals: pd.Series, regime_mask: pd.Series) -> pd.Series:
    """Zero out signals in quiet regimes."""
    masked = signals.copy()
    # Align regime mask to signal index
    mask_aligned = regime_mask.reindex(signals.index, method="ffill").fillna(False)
    masked[~mask_aligned.values] = "flat"
    return masked


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


def _slice(df, s, e):
    return df[(df["ts"] >= s) & (df["ts"] < e)].copy().reset_index(drop=True)


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

    # Regime mask for all bars
    print("Computing regime mask...")
    regime_mask = _build_regime_mask(btc, all_feats["ts"])
    pct_tradeable = regime_mask.mean()
    print(f"  tradeable bars: {regime_mask.sum()}/{len(regime_mask)} ({pct_tradeable:.1%})")

    windows = list(_months_range(btc["ts"].min(), btc["ts"].max()))

    print(f"\nREGIME-GATED EXPERIMENT (24h horizon)")
    print(f"  features: {len(FEAT_NAMES)} (pack v1)")
    print(f"  regime: vol_pctl>=60 OR atr_ratio>=1.5 OR funding_pctl>=75")
    print(f"  windows: {len(windows)}")

    results = []
    for i, w in enumerate(windows):
        tr = all_feats[(all_feats["ts"] >= w["train_start"]) & (all_feats["ts"] < w["train_end"])]
        tr = tr.dropna(subset=["label", *FEAT_NAMES]).reset_index(drop=True)
        if len(tr) < 500:
            results.append({"window": i, "status": "skip"})
            continue

        mdl = _train_model(tr[FEAT_NAMES].values, np.array(tr["label"].values))

        # Validate (with regime mask applied)
        vb = _slice(btc, w["val_start"], w["val_end"])
        vf = all_feats[(all_feats["ts"] >= w["val_start"]) & (all_feats["ts"] < w["val_end"])]
        vf = vf.dropna(subset=FEAT_NAMES).reset_index(drop=True)
        if len(vf) < 30 or len(vb) < 30:
            results.append({"window": i, "status": "skip"})
            continue

        pv = _p_up(mdl, vf[FEAT_NAMES].values)
        # Get regime mask for validate period
        vm = _build_regime_mask(vb, vf["ts"])

        bt, be = None, 0
        for thr in THRESHOLD_GRID:
            sig = _build_signals(vf["ts"], pv, float(thr))
            sig_masked = _apply_regime_mask(sig, vm)
            _, _, m = run_engine(vb, sig_masked, ENGINE_CFG)
            if m["max_drawdown"] <= MAX_DD and m["num_trades"] >= MIN_TRADES_VAL:
                if m["final_equity"] > be:
                    be, bt = m["final_equity"], float(thr)

        if bt is None:
            results.append({"window": i, "status": "no_thr",
                            "test_start": str(w["test_start"].date()),
                            "test_end": str(w["test_end"].date())})
            continue

        # Test (with regime mask)
        tb = _slice(btc, w["test_start"], w["test_end"])
        tf = all_feats[(all_feats["ts"] >= w["test_start"]) & (all_feats["ts"] < w["test_end"])]
        tf = tf.dropna(subset=FEAT_NAMES).reset_index(drop=True)
        if len(tf) < 10 or len(tb) < 10:
            results.append({"window": i, "status": "skip"})
            continue

        pt = _p_up(mdl, tf[FEAT_NAMES].values)
        tm = _build_regime_mask(tb, tf["ts"])
        sig_test = _build_signals(tf["ts"], pt, bt)
        sig_gated = _apply_regime_mask(sig_test, tm)

        # Count how many signals survived gating
        raw_ups = (sig_test == "up").sum()
        gated_ups = (sig_gated == "up").sum()

        _, _, met = run_engine(tb, sig_gated, ENGINE_CFG)

        # Baseline
        us = pd.Series("up", index=pd.DatetimeIndex(
            pd.to_datetime(tb["ts"].values, utc=True)), dtype="object")
        _, _, bm = run_engine(tb, us, ENGINE_CFG)

        dok = met["max_drawdown"] <= MAX_DD
        beats = met["final_equity"] > bm["final_equity"]
        real = met["num_trades"] >= 3
        status = "PASS" if (dok and beats and real) else ("HOLLOW" if (dok and not real) else "FAIL")

        results.append({
            "window": i,
            "test_start": str(w["test_start"].date()),
            "test_end": str(w["test_end"].date()),
            "threshold": bt,
            "equity": met["final_equity"],
            "ret": met["total_return"],
            "dd": met["max_drawdown"],
            "trades": met["num_trades"],
            "wins": met["num_wins"],
            "raw_signals": int(raw_ups),
            "gated_signals": int(gated_ups),
            "baseline_eq": bm["final_equity"],
            "status": status,
        })

    # ── Summary ──────────────────────────────────────────────────────
    print(f"\n{'='*85}")
    print(f"  {'W':>3s} {'period':>25s} {'thr':>5s} {'eq':>8s} {'ret':>8s} {'dd':>6s} {'tr':>4s} {'w':>3s} {'raw':>4s} {'gat':>4s} {'gate':>8s}")
    print(f"  {'-'*3} {'-'*25} {'-'*5} {'-'*8} {'-'*8} {'-'*6} {'-'*4} {'-'*3} {'-'*4} {'-'*4} {'-'*8}")

    np_ = nf = nh = ns = 0
    pr = []
    ar = []
    for r in results:
        if "equity" not in r:
            ns += 1
            ts_ = r.get("test_start", "?")
            te = r.get("test_end", "?")
            print(f"  {r['window']:>3d} {(ts_+'->'+te):>25s}   ---      ---      ---    ---  ---  ---  --- {r['status']:>8s}")
        else:
            p = f"{r['test_start']}->{r['test_end']}"
            print(f"  {r['window']:>3d} {p:>25s} {r['threshold']:>5.2f} "
                  f"${r['equity']:>7.0f} ${r['ret']:>7.0f} {r['dd']:>5.3f} "
                  f"{r['trades']:>4d} {r['wins']:>3d} {r.get('raw_signals',0):>4d} {r.get('gated_signals',0):>4d} {r['status']:>8s}")
            ar.append(r["ret"])
            if r["status"] == "PASS":
                np_ += 1
                pr.append(r["ret"])
            elif r["status"] == "HOLLOW":
                nh += 1
            else:
                nf += 1

    avg_p = np.mean(pr) if pr else 0
    avg_a = np.mean(ar) if ar else 0
    total = sum(ar)

    print(f"\n  PASS: {np_}  HOLLOW: {nh}  FAIL: {nf}  SKIP: {ns}")
    print(f"  avg PASS ret: ${avg_p:.2f}  avg ALL ret: ${avg_a:.2f}  total P&L: ${total:.2f}")

    print(f"\n  COMPARISON:")
    print(f"    24h ungated:         PASS=19  FAIL=9   avg_all=$-12.13  total=$-412")
    print(f"    24h tight:           PASS=17  FAIL=6   avg_all=$-10.96  total=$-307")
    print(f"    24h regime-gated:    PASS={np_}  FAIL={nf}   avg_all=${avg_a:.2f}  total=${total:.2f}")

    if total > 0:
        print(f"\n  VERDICT: REGIME GATING FLIPPED PROFITABILITY.")
        print(f"  The edge IS conditional. Trading only in active regimes works.")
    elif total > -100:
        print(f"\n  VERDICT: NEAR BREAKEVEN. Regime gating helps but not enough.")
    elif np_ > 19 or avg_a > -5:
        print(f"\n  VERDICT: STABILITY IMPROVED but economics still negative.")
    else:
        print(f"\n  VERDICT: REGIME GATING DID NOT SOLVE IT.")
        print(f"  Directional classification of BTC is structurally unprofitable")
        print(f"  at retail cost levels with available data.")
    print(f"{'='*85}")

    Path(REPORTS_DIR / "results.json").write_text(json.dumps(results, indent=2, default=str) + "\n")
    print(f"  WROTE {REPORTS_DIR}/results.json")


if __name__ == "__main__":
    main()

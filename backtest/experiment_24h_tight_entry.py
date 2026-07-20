"""
24h Horizon — Tight Entry Experiment.

Hypothesis: the system is overtrading mid-confidence signals.
Top decile (p_up >= ~0.41) has +2.8% avg return on train.
Lower deciles drag performance down.

Change: raise threshold grid to 0.44-0.56 (skip low-conviction trades).
Also add a stricter gate: trades >= 3 AND avg p_up of signals > 0.45.

Everything else frozen. Same 34 monthly windows.
"""
from __future__ import annotations

import json
from pathlib import Path
from dateutil.relativedelta import relativedelta

import numpy as np
import pandas as pd
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.calibration import CalibratedClassifierCV

from backtest.engine import EngineConfig, run_engine
from features.packs.pack_v1_derivs import build_features_pack_v1, FEATURE_NAMES_PACK_V1


REPORTS_DIR = Path("reports/horizon_24h_tight")
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
LABEL_HORIZON = 24
LABEL_THRESHOLD = 0.004
TRAIN_MONTHS = 12
RETRAIN_MONTHS = 1
VAL_MONTHS = 1
TEST_MONTHS = 1
GAP_BARS = 24
MAX_DD = 0.10
MIN_TRADES_VAL = 3

# ── THE CHANGE: tighter threshold grid ──────────────────────────────
THRESHOLD_GRID = np.round(np.arange(0.44, 0.58, 0.01), 2)
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

    btc_idx = btc.set_index("ts").sort_index()
    close_a = btc_idx["close"].reindex(all_feats["ts"]).values
    fwd = pd.Series(close_a).shift(-LABEL_HORIZON) / pd.Series(close_a) - 1.0
    labels = pd.Series("flat", index=all_feats.index, dtype="object")
    labels[fwd > LABEL_THRESHOLD] = "up"
    labels[fwd < -LABEL_THRESHOLD] = "down"
    labels[fwd.isna()] = None
    all_feats["label"] = labels.values

    windows = list(_months_range(btc["ts"].min(), btc["ts"].max()))

    print(f"24H TIGHT ENTRY EXPERIMENT")
    print(f"  threshold grid: {THRESHOLD_GRID[0]} to {THRESHOLD_GRID[-1]}")
    print(f"  windows: {len(windows)}")

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

        # Stats on signal conviction
        signals_taken = p_test[p_test >= best_thr]
        avg_conviction = float(signals_taken.mean()) if len(signals_taken) > 0 else 0

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
            "avg_conviction": round(avg_conviction, 4),
            "baseline_eq": base_m["final_equity"],
            "status": status,
        })

    # ── Summary ──────────────────────────────────────────────────────
    print(f"\n{'='*80}")
    print(f"  {'W':>3s} {'period':>25s} {'thr':>5s} {'eq':>8s} {'ret':>8s} {'dd':>6s} {'tr':>4s} {'w':>3s} {'conv':>6s} {'gate':>8s}")
    print(f"  {'-'*3} {'-'*25} {'-'*5} {'-'*8} {'-'*8} {'-'*6} {'-'*4} {'-'*3} {'-'*6} {'-'*8}")

    n_pass = n_fail = n_hollow = n_skip = 0
    pass_rets = []
    all_rets = []

    for r in results:
        if "equity" not in r:
            n_skip += 1
            ts = r.get("test_start", "?")
            te = r.get("test_end", "?")
            print(f"  {r['window']:>3d} {(ts+'->'+te):>25s}   ---      ---      ---    ---  ---    --- {r['status']:>8s}")
        else:
            period = f"{r['test_start']}->{r['test_end']}"
            conv = r.get("avg_conviction", 0)
            print(f"  {r['window']:>3d} {period:>25s} {r['threshold']:>5.2f} "
                  f"${r['equity']:>7.0f} ${r['ret']:>7.0f} {r['dd']:>5.3f} "
                  f"{r['trades']:>4d} {r['wins']:>3d} {conv:>5.3f} {r['status']:>8s}")
            all_rets.append(r["ret"])
            if r["status"] == "PASS":
                n_pass += 1
                pass_rets.append(r["ret"])
            elif r["status"] == "HOLLOW":
                n_hollow += 1
            else:
                n_fail += 1

    avg_pass = np.mean(pass_rets) if pass_rets else 0
    avg_all = np.mean(all_rets) if all_rets else 0
    total_pnl = sum(all_rets)

    print(f"\n  PASS: {n_pass}  HOLLOW: {n_hollow}  FAIL: {n_fail}  SKIP: {n_skip}")
    print(f"  avg PASS return: ${avg_pass:.2f}")
    print(f"  avg ALL return: ${avg_all:.2f}")
    print(f"  total P&L (simple sum): ${total_pnl:.2f}")

    print(f"\n  COMPARISON:")
    print(f"    24h wide (0.40-0.55):  PASS=19  FAIL=9   avg_pass=$2.21   avg_all=$-12.13")
    print(f"    24h tight (0.44-0.57): PASS={n_pass}  FAIL={n_fail}   avg_pass=${avg_pass:.2f}   avg_all=${avg_all:.2f}")

    if avg_all > 0:
        print(f"\n  VERDICT: TIGHT ENTRY FLIPPED PROFITABILITY.")
        print(f"  The system was overtrading. Higher conviction trades are net positive.")
    elif avg_pass > 10:
        print(f"\n  VERDICT: PASS QUALITY IMPROVED. Still not net profitable overall.")
    else:
        print(f"\n  VERDICT: TIGHT ENTRY DID NOT SOLVE IT.")
        print(f"  Signal strength is insufficient even at high conviction.")
    print(f"{'='*80}")

    Path(REPORTS_DIR / "tight_entry_results.json").write_text(
        json.dumps(results, indent=2, default=str) + "\n"
    )
    print(f"  WROTE {REPORTS_DIR}/tight_entry_results.json")


if __name__ == "__main__":
    main()

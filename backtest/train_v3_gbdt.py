"""
Train v3 GBDT classifier.

Same as train_v3_model.py but replaces softmax with
HistGradientBoostingClassifier (sklearn, no extra deps).

Same labels, same features, same walkforward splits.
Outputs reports/v3_gbdt_model.pkl
"""
from __future__ import annotations

import json
import pickle
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.calibration import CalibratedClassifierCV

from backtest.walkforward import split_walkforward
from features.build_features_leaders_v1 import build_features_leaders_v1


REPORTS_DIR = Path("reports")

FEATURE_NAMES = [
    "BTC_ret_1", "BTC_ret_4", "BTC_ret_24", "BTC_vol_24", "BTC_vol_chg_1",
    "ETH_ret_1", "ETH_ret_4", "ETH_ret_24", "ETH_vol_24", "ETH_vol_chg_1",
    "SOL_ret_1", "SOL_ret_4", "SOL_ret_24", "SOL_vol_24", "SOL_vol_chg_1",
]


def _make_labels(df: pd.DataFrame, horizon: int = 12, thr: float = 0.002) -> pd.DataFrame:
    df = df.copy()
    df["ts"] = pd.to_datetime(df["ts"], utc=True)
    df = df.sort_values("ts").reset_index(drop=True)
    close = df["close"].astype(float)
    fwd = close.shift(-horizon) / close - 1.0
    y = pd.Series("flat", index=df.index, dtype="object")
    y[fwd > thr] = "up"
    y[fwd < -thr] = "down"
    y[fwd.isna()] = None
    return pd.DataFrame({"ts": df["ts"], "label": y})


def main() -> None:
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    # Load data
    btc = pd.read_parquet("data_parquet/BTCUSD_USD_1h_20220323_now.parquet").copy()
    eth = pd.read_parquet("data_parquet/ETH_1h.parquet").copy()
    sol = pd.read_parquet("data_parquet/SOL_1h.parquet").copy()

    btc["ts"] = pd.to_datetime(btc["ts"], utc=True)
    btc = btc.sort_values("ts").reset_index(drop=True)

    splits = split_walkforward(btc)
    train_btc = splits["train"].copy().reset_index(drop=True)

    # Labels
    labels = _make_labels(train_btc)

    # Features
    feats = build_features_leaders_v1(train_btc, eth, sol)
    feats = feats.merge(labels, on="ts", how="left")
    feats = feats.dropna(subset=["label", *FEATURE_NAMES]).reset_index(drop=True)

    X = feats[FEATURE_NAMES].astype(float).values
    y = feats["label"].values

    # Verify finite
    finite_mask = np.isfinite(X).all(axis=1)
    X = X[finite_mask]
    y = y[finite_mask]
    print(f"  train rows: {len(X)}")
    print(f"  label counts: {dict(zip(*np.unique(y, return_counts=True)))}")

    # Train GBDT — conservative hyperparameters to avoid overfit
    clf = HistGradientBoostingClassifier(
        max_iter=300,
        max_depth=4,
        learning_rate=0.05,
        min_samples_leaf=50,
        max_leaf_nodes=31,
        l2_regularization=1.0,
        random_state=42,
    )
    clf.fit(X, y)

    # Calibrate probabilities with isotonic regression (3-fold on train)
    cal_clf = CalibratedClassifierCV(clf, cv=3, method="isotonic")
    cal_clf.fit(X, y)

    # Verify classes order
    print(f"  classes: {list(cal_clf.classes_)}")

    # Quick train accuracy
    train_probs = cal_clf.predict_proba(X)
    train_preds = cal_clf.classes_[np.argmax(train_probs, axis=1)]
    train_acc = (train_preds == y).mean()
    print(f"  train accuracy: {train_acc:.4f}")

    # Save model
    model_path = REPORTS_DIR / "v3_gbdt_model.pkl"
    with open(model_path, "wb") as f:
        pickle.dump({
            "model": cal_clf,
            "feature_names": FEATURE_NAMES,
            "classes": list(cal_clf.classes_),
        }, f)
    print(f"\n  WROTE {model_path}")

    # Also save metadata as JSON for audit
    meta = {
        "type": "HistGradientBoostingClassifier + isotonic calibration",
        "feature_names": FEATURE_NAMES,
        "classes": list(cal_clf.classes_),
        "train_rows": int(len(X)),
        "train_accuracy": float(train_acc),
        "label_counts": {str(k): int(v) for k, v in zip(*np.unique(y, return_counts=True))},
        "hyperparams": {
            "max_iter": 300,
            "max_depth": 4,
            "learning_rate": 0.05,
            "min_samples_leaf": 50,
            "max_leaf_nodes": 31,
            "l2_regularization": 1.0,
        },
    }
    Path(REPORTS_DIR / "v3_gbdt_meta.json").write_text(json.dumps(meta, indent=2) + "\n")
    print(f"  WROTE reports/v3_gbdt_meta.json")


if __name__ == "__main__":
    main()

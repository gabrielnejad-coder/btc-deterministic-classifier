import json
from pathlib import Path

import numpy as np
import pandas as pd

from backtest.walkforward import split_walkforward
from features.build_features_leaders_v1 import build_features_leaders_v1

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

def _softmax_train(X: np.ndarray, y: np.ndarray, num_classes: int, lr: float = 0.15, steps: int = 3500, reg: float = 1e-4):
    n, d = X.shape
    W = np.zeros((d, num_classes), dtype=float)
    b = np.zeros((num_classes,), dtype=float)

    Y = np.zeros((n, num_classes), dtype=float)
    Y[np.arange(n), y] = 1.0

    for _ in range(steps):
        logits = X @ W + b
        logits = logits - logits.max(axis=1, keepdims=True)
        exp = np.exp(logits)
        P = exp / exp.sum(axis=1, keepdims=True)

        gradW = (X.T @ (P - Y)) / n + reg * W
        gradb = (P - Y).mean(axis=0)

        W -= lr * gradW
        b -= lr * gradb

    return W, b

def _assert_finite(X: np.ndarray, name: str) -> None:
    bad = ~np.isfinite(X)
    if bad.any():
        idx = np.argwhere(bad)
        i0, j0 = idx[0]
        raise RuntimeError(f"{name} contains non-finite values, first at row={int(i0)} col={int(j0)}")

def main():
    Path("reports").mkdir(parents=True, exist_ok=True)

    btc = pd.read_parquet("data_parquet/BTCUSD_USD_1h_20220323_now.parquet").copy()
    btc["ts"] = pd.to_datetime(btc["ts"], utc=True)
    btc = btc.sort_values("ts").reset_index(drop=True)

    eth = pd.read_parquet("data_parquet/ETH_1h.parquet").copy()
    sol = pd.read_parquet("data_parquet/SOL_1h.parquet").copy()

    splits = split_walkforward(btc)
    train_btc = splits["train"].copy().reset_index(drop=True)

    labels = _make_labels(train_btc)

    feats = build_features_leaders_v1(train_btc, eth, sol)
    feats = feats.merge(labels, on="ts", how="left")

    feature_names = [
        "BTC_ret_1","BTC_ret_4","BTC_ret_24","BTC_vol_24","BTC_vol_chg_1",
        "ETH_ret_1","ETH_ret_4","ETH_ret_24","ETH_vol_24","ETH_vol_chg_1",
        "SOL_ret_1","SOL_ret_4","SOL_ret_24","SOL_vol_24","SOL_vol_chg_1",
    ]

    feats = feats.dropna(subset=["label", *feature_names]).reset_index(drop=True)

    Xraw = feats[feature_names].astype(float).to_numpy()
    _assert_finite(Xraw, "Xraw")

    mean = Xraw.mean(axis=0)
    std = Xraw.std(axis=0)
    std = np.where(std == 0.0, 1.0, std)

    X = (Xraw - mean) / std
    _assert_finite(X, "X")

    classes = ["down", "flat", "up"]
    y_map = {c: i for i, c in enumerate(classes)}
    y = np.array([y_map[v] for v in feats["label"].tolist()], dtype=int)

    W, b = _softmax_train(X, y, num_classes=len(classes))

    out = {
        "version": "v3",
        "leaders": json.loads(Path("reports/leaders_v1.json").read_text()).get("picked_assets", []),
        "feature_names": feature_names,
        "mean": mean.tolist(),
        "std": std.tolist(),
        "W": W.tolist(),
        "b": b.tolist(),
        "classes": classes,
    }

    Path("reports/v3_model.json").write_text(json.dumps(out, indent=2) + "\n")
    print("WROTE reports/v3_model.json")
    print("train_rows", int(len(feats)))
    print("label_counts", feats["label"].value_counts().to_dict())

if __name__ == "__main__":
    main()

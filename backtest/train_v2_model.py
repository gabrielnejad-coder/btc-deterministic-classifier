import json
from pathlib import Path

import numpy as np
import pandas as pd

from backtest.walkforward import split_walkforward
from features.build_features import build_features


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


def _softmax_train(
    X: np.ndarray,
    y: np.ndarray,
    num_classes: int,
    lr: float = 0.2,
    steps: int = 3000,
    reg: float = 1e-4,
):
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


def main():
    Path("reports").mkdir(parents=True, exist_ok=True)

    df = pd.read_parquet("data_parquet/BTCUSD_USD_1h_20220323_now.parquet").copy()
    df["ts"] = pd.to_datetime(df["ts"], utc=True)
    df = df.sort_values("ts").reset_index(drop=True)

    splits = split_walkforward(df)
    train_df = splits["train"].copy().reset_index(drop=True)

    labels_df = _make_labels(train_df)

    feats = build_features(train_df).copy()
    feats["ts"] = pd.to_datetime(feats["ts"], utc=True)
    feats = feats.sort_values("ts").reset_index(drop=True)
    feats = feats.merge(labels_df, on="ts", how="left")

    feature_names = [
        "ret_1", "ret_2", "ret_4", "ret_8", "ret_12", "ret_24",
        "mom_12",
        "vol_12", "vol_24", "vol_48",
        "range_1",
        "vol_chg_1", "vol_z_48",
    ]

    # Ensure all features exist
    missing = [c for c in feature_names if c not in feats.columns]
    if missing:
        raise RuntimeError(f"Missing features from build_features: {missing}")

    # ---- STRICT CLEANING ----
    feats = feats.replace([np.inf, -np.inf], np.nan)
    feats = feats.dropna(subset=["label", *feature_names]).reset_index(drop=True)

    Xraw = feats[feature_names].astype(float).to_numpy()

    # Remove any non-finite rows
    finite_mask = np.isfinite(Xraw).all(axis=1)
    Xraw = Xraw[finite_mask]
    feats = feats.iloc[finite_mask].reset_index(drop=True)

    if len(Xraw) == 0:
        raise RuntimeError("All rows removed after finite filtering.")

    print("rows_after_finite_filter", len(Xraw))

    mean = Xraw.mean(axis=0)
    std = Xraw.std(axis=0)
    std = np.where(std == 0.0, 1.0, std)
    X = (Xraw - mean) / std

    classes = ["down", "flat", "up"]
    y_map = {c: i for i, c in enumerate(classes)}
    y = np.array([y_map[v] for v in feats["label"].tolist()], dtype=int)

    W, b = _softmax_train(X, y, num_classes=len(classes))

    out = {
        "feature_names": feature_names,
        "mean": mean.tolist(),
        "std": std.tolist(),
        "W": W.tolist(),
        "b": b.tolist(),
        "classes": classes,
    }

    Path("reports/v2_model.json").write_text(json.dumps(out, indent=2) + "\n")

    print("WROTE reports/v2_model.json")
    print("n_features", len(feature_names))
    print("train_rows", int(len(feats)))
    print("label_counts", feats["label"].value_counts().to_dict())


if __name__ == "__main__":
    main()

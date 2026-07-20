import json
from pathlib import Path
from typing import Dict, List

import numpy as np
import pandas as pd

MODEL_PATH = Path("reports/v3_model.json")

def _softmax(z: np.ndarray) -> np.ndarray:
    z = z - np.max(z)
    e = np.exp(z)
    return e / np.sum(e)

def load_v3(path: Path = MODEL_PATH) -> dict:
    if not path.exists():
        raise FileNotFoundError(f"missing {path}, run: PYTHONPATH=. python -m backtest.train_v3_model")
    return json.loads(path.read_text())

def predict_proba_df(model: dict, feats: pd.DataFrame) -> pd.DataFrame:
    feat_names: List[str] = model["feature_names"]
    Xraw = feats[feat_names].astype(float).to_numpy()

    mean = np.array(model["mean"], dtype=float)
    std = np.array(model["std"], dtype=float)
    W = np.array(model["W"], dtype=float)
    b = np.array(model["b"], dtype=float)

    X = (Xraw - mean) / std
    logits = X @ W + b
    logits = logits - np.max(logits, axis=1, keepdims=True)
    exp = np.exp(logits)
    P = exp / np.sum(exp, axis=1, keepdims=True)

    classes: List[str] = model["classes"]
    return pd.DataFrame(P, columns=[f"p_{c}" for c in classes])

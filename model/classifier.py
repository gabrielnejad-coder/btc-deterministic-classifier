import json
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Any

import numpy as np

from features.schema import FeatureObject

MODEL_PATH = Path("reports/v2_model.json")


@dataclass(frozen=True)
class V2Model:
    feature_names: list
    mean: np.ndarray
    std: np.ndarray
    W: np.ndarray
    b: np.ndarray
    classes: list

    def predict_proba_row(self, x: np.ndarray) -> np.ndarray:
        z = (x - self.mean) / self.std
        logits = z @ self.W + self.b
        logits = logits - np.max(logits)
        exps = np.exp(logits)
        return exps / np.sum(exps)


_MODEL: V2Model | None = None


def _load_model() -> V2Model:
    global _MODEL
    if _MODEL is not None:
        return _MODEL

    if not MODEL_PATH.exists():
        raise FileNotFoundError(f"missing model file: {MODEL_PATH}. run: PYTHONPATH=. python -m backtest.train_v2_model")

    raw = json.loads(MODEL_PATH.read_text())
    _MODEL = V2Model(
        feature_names=raw["feature_names"],
        mean=np.array(raw["mean"], dtype=float),
        std=np.array(raw["std"], dtype=float),
        W=np.array(raw["W"], dtype=float),
        b=np.array(raw["b"], dtype=float),
        classes=raw["classes"],
    )
    return _MODEL


def classify(f: FeatureObject) -> Dict[str, Any]:
    m = _load_model()

    feat_map = {
        "ret_1": float(f.ret_1),
        "ret_2": float(f.ret_2),
        "ret_4": float(f.ret_4),
        "ret_8": float(f.ret_8),
        "ret_12": float(f.ret_12),
        "ret_24": float(f.ret_24),
        "mom_12": float(f.mom_12),
        "vol_12": float(f.vol_12),
        "vol_24": float(f.vol_24),
        "vol_48": float(f.vol_48),
        "range_1": float(f.range_1),
        "vol_chg_1": float(f.vol_chg_1),
        "vol_z_48": float(f.vol_z_48),
    }

    x = np.array([feat_map[n] for n in m.feature_names], dtype=float)
    p = m.predict_proba_row(x)

    probs = {cls: float(p[i]) for i, cls in enumerate(m.classes)}
    direction = max(probs, key=lambda k: probs[k])
    confidence = float(probs[direction])

    return {
        "p_up": probs.get("up", 0.0),
        "p_down": probs.get("down", 0.0),
        "p_flat": probs.get("flat", 0.0),
        "direction": direction,
        "confidence": confidence,
    }

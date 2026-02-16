import pandas as pd

from features.build_features import build_features
from features.schema import FeatureObject
from model.classifier import classify


def build_signals_v1(df: pd.DataFrame) -> pd.Series:
    """
    Outputs a pd.Series of "up" or "down" indexed by bar CLOSE timestamps (df["ts"]).
    Timing rule:
    signal decided at close of bar t, engine will fill at open of bar t+1.
    """

    # Build features aligned to bar close timestamps
    feats = build_features(df)

    # Map ts -> direction
    sig_map = {}

    for _, row in feats.iterrows():
        fobj = FeatureObject(
            ts=str(row["ts"]),
            close=float(row["close"]),
            ret_1=float(row["ret_1"]),
            ret_4=float(row["ret_4"]),
            ret_24=float(row["ret_24"]),
            vol_24=float(row["vol_24"]),
        )

        out = classify(fobj)

        direction = out["direction"]
        if direction not in ("up", "down"):
            raise ValueError(f"Classifier returned invalid direction: {direction}")

        sig_map[pd.Timestamp(row["ts"])] = direction

    # Build full length signal series aligned to df ts
    ts_index = pd.DatetimeIndex(pd.to_datetime(df["ts"], utc=True))
    sig = pd.Series(index=ts_index, dtype="object")

    # Fill only where features exist, leave the early rows as NaN
    for ts, direction in sig_map.items():
        ts_utc = pd.to_datetime(ts, utc=True)
        sig.loc[ts_utc] = direction

    return sig

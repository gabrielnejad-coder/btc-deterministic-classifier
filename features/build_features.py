import pandas as pd


def build_features(df: pd.DataFrame) -> pd.DataFrame:
    d = df.copy()
    d = d.sort_values("ts").reset_index(drop=True)

    d["ret_1"] = d["close"].pct_change(1)
    d["ret_4"] = d["close"].pct_change(4)
    d["ret_24"] = d["close"].pct_change(24)

    d["vol_24"] = d["ret_1"].rolling(24).std()

    cols = ["ts", "close", "ret_1", "ret_4", "ret_24", "vol_24"]
    out = d[cols].dropna().reset_index(drop=True)
    return out

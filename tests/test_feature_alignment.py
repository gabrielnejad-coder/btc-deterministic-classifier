import pandas as pd
from features.build_features import build_features


def test_features_do_not_use_future_data():
    df = pd.DataFrame(
        {
            "ts": pd.date_range("2024-01-01", periods=50, freq="h"),
            "open": range(50),
            "high": range(50),
            "low": range(50),
            "close": range(50),
            "volume": range(50),
        }
    )

    feats = build_features(df)

    row = feats.iloc[0]
    ts = row["ts"]
    idx = df.index[df["ts"] == ts][0]

    expected_ret = (
        df.loc[idx, "close"] - df.loc[idx - 1, "close"]
    ) / df.loc[idx - 1, "close"]

    assert abs(row["ret_1"] - expected_ret) < 1e-9

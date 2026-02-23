import pandas as pd
from backtest.walkforward import split_walkforward

def test_walkforward_splits_nonempty_and_no_overlap():
    df = pd.read_parquet("data_parquet/BTCUSD_USD_1h_20220323_now.parquet")
    df["ts"] = pd.to_datetime(df["ts"], utc=True)
    df = df.sort_values("ts").reset_index(drop=True)

    splits = split_walkforward(df)
    train = splits["train"]
    validate = splits["validate"]
    test = splits["test"]

    assert len(train) > 0
    assert len(validate) > 0
    assert len(test) > 0

    train_max = train["ts"].max()
    validate_min = validate["ts"].min()
    validate_max = validate["ts"].max()
    test_min = test["ts"].min()

    assert train_max < validate_min
    assert validate_max < test_min

    assert test["ts"].min() >= pd.Timestamp("2024-01-01", tz="UTC")


import json
import pandas as pd
from backtest.engine import EngineConfig, run_engine
from model.baselines import always_up, yesterday_equals_today


def split_walkforward(df: pd.DataFrame):
    df = df.copy()
    df["ts"] = pd.to_datetime(df["ts"], utc=True)
    df = df.sort_values("ts")

    ts = df["ts"]  # FIX: Get ts AFTER sort so indices align

    train_mask = (ts >= "2021-01-01") & (ts < "2023-01-01")
    validate_mask = (ts >= "2023-01-01") & (ts < "2024-01-01")
    test_mask = ts >= "2024-01-01"

    return {
        "train": df.loc[train_mask].copy(),
        "validate": df.loc[validate_mask].copy(),
        "test": df.loc[test_mask].copy(),
    }


def run_baselines_walkforward(df: pd.DataFrame, cfg: EngineConfig):
    splits = split_walkforward(df)
    for split_name, split_df in splits.items():
        signals_up = always_up(split_df)
        signals_yday = yesterday_equals_today(split_df)

        trades_up, equity_up, metrics_up = run_engine(split_df, signals_up, cfg)
        trades_y, equity_y, metrics_y = run_engine(split_df, signals_yday, cfg)

        with open(f"reports/baseline_always_up_{split_name}_metrics.json", "w") as f:
            json.dump(metrics_up, f, indent=2)

        with open(f"reports/baseline_yday_eq_today_{split_name}_metrics.json", "w") as f:
            json.dump(metrics_y, f, indent=2)

        trades_up.to_parquet(f"reports/baseline_always_up_{split_name}_trades.parquet", index=False)
        equity_up.to_parquet(f"reports/baseline_always_up_{split_name}_equity.parquet", index=False)

        trades_y.to_parquet(f"reports/baseline_yday_eq_today_{split_name}_trades.parquet", index=False)
        equity_y.to_parquet(f"reports/baseline_yday_eq_today_{split_name}_equity.parquet", index=False)

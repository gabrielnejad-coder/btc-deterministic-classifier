import json
from pathlib import Path

import pandas as pd

from backtest.engine import EngineConfig, run_engine
from backtest.walkforward import split_walkforward
from model.strategy_v2 import build_signals_v2


def _validate_signals(split_df: pd.DataFrame, signals: pd.Series) -> None:
    if not isinstance(signals, pd.Series):
        raise TypeError(f"signals must be pd.Series, got {type(signals)}")

    if not isinstance(signals.index, pd.DatetimeIndex):
        raise TypeError(f"signals index must be DatetimeIndex, got {type(signals.index)}")

    if signals.index.tz is None:
        raise ValueError("signals index must be tz-aware (UTC)")

    df_ts = pd.DatetimeIndex(pd.to_datetime(split_df["ts"], utc=True), name="ts")

    if not signals.index.equals(df_ts):
        raise ValueError("signals index must exactly equal df['ts'] (same values, same order)")

    bad = set(signals.dropna().unique()) - {"up", "down", "flat"}
    if bad:
        raise ValueError(f"Unexpected signal values: {bad}")


def main():
    Path("reports").mkdir(parents=True, exist_ok=True)

    df = pd.read_parquet("data_parquet/BTCUSD_USD_1h_20220323_now.parquet")
    df = df.copy()
    df["ts"] = pd.to_datetime(df["ts"], utc=True)
    df = df.sort_values("ts")

    cfg = EngineConfig(
        fee_taker=0.0004,
        slippage_side=0.0001,
        stop_loss_pct=0.02,
        initial_equity=1_000.0,
    )

    splits = split_walkforward(df)

    for split_name, split_df in splits.items():
        print(f"Running {split_name}...")

        signals = build_signals_v2(split_df)
        _validate_signals(split_df, signals)

        trades, equity, metrics = run_engine(split_df, signals, cfg)

        with open(f"reports/v2_{split_name}_metrics.json", "w") as f:
            json.dump(metrics, f, indent=2)

        trades.to_parquet(f"reports/v2_{split_name}_trades.parquet", index=False)
        equity.to_parquet(f"reports/v2_{split_name}_equity.parquet", index=False)

        print(
            f"  {split_name}: trades={metrics['num_trades']}, "
            f"return={metrics['total_return']:.2f}, dd={metrics['max_drawdown']:.4f}"
        )

    print("done")


if __name__ == "__main__":
    main()

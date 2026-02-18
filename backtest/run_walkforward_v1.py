import json
import pandas as pd

from backtest.engine import EngineConfig, run_engine
from backtest.walkforward import split_walkforward

# IMPORTANT
# This must match your strategy_v1 public function name.
# Your earlier test referenced build_signals_v1(df), so we use that.
from model.strategy_v1 import build_signals_v1


def main():
    df = pd.read_parquet("data_parquet/BTCUSD_USD_1h_20220323_now.parquet")

    cfg = EngineConfig(
        fee_taker=0.0004,
        slippage_side=0.0001,
        stop_loss_pct=0.02,
        initial_equity=1_000.0,
    )

    splits = split_walkforward(df)

    for split_name, split_df in splits.items():
        signals = build_signals_v1(split_df)

        trades, equity, metrics = run_engine(split_df, signals, cfg)

        with open(f"reports/v1_{split_name}_metrics.json", "w") as f:
            json.dump(metrics, f, indent=2)

        trades.to_parquet(f"reports/v1_{split_name}_trades.parquet", index=False)
        equity.to_parquet(f"reports/v1_{split_name}_equity.parquet", index=False)

    print("done")


if __name__ == "__main__":
    main()

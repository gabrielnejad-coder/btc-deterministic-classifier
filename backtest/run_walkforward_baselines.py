import pandas as pd

from backtest.engine import EngineConfig
from backtest.walkforward import run_baselines_walkforward


def main():
    df = pd.read_parquet("data_parquet/BTCUSD_USD_1h_20220323_now.parquet")

    cfg = EngineConfig(
        fee_taker=0.0004,
        slippage_side=0.0001,
        stop_loss_pct=0.02,
        initial_equity=1_000.0,
    )

    run_baselines_walkforward(df, cfg)
    print("walkforward baselines done")


if __name__ == "__main__":
    main()

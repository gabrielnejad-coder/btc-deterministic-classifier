import pandas as pd

from backtest.engine import EngineConfig, run_engine
from model.strategy_v1 import build_signals_v1


def main():
    df = pd.read_parquet("data_parquet/BTCUSD_USD_1h_20220323_now.parquet")
    df = df.sort_values("ts").reset_index(drop=True)

    # Small slice so it runs fast
    df = df.iloc[:200].copy()

    sig = build_signals_v1(df)

    cfg = EngineConfig(
        fee_taker=0.0004,
        slippage_side=0.0001,
        stop_loss_pct=0.02,
        initial_equity=1.0,
    )

    trades, equity, metrics = run_engine(df, sig, cfg)

    print("trades_count", len(trades))
    print("final_equity", float(equity["equity"].iloc[-1]))
    print("metrics_keys", list(metrics.keys()))


if __name__ == "__main__":
    main()

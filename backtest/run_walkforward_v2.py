import json
from pathlib import Path

import pandas as pd

from backtest.engine import run_engine, EngineConfig
from backtest.walkforward import split_walkforward
from model.strategy_v2 import build_signals_v2
from model.strategy_v2_edge import build_signals_v2_edge


def load_cfg() -> EngineConfig:
    # Hard aligned with v1 config defaults
    return EngineConfig(
        fee_taker=0.0004,
        slippage_side=0.0001,
        stop_loss_pct=0.02,
        hold_min_bars=12,
        initial_equity=1000.0,
        one_position=True,
    )


def main():
    df = pd.read_parquet("data_parquet/BTCUSD_USD_1h_20220323_now.parquet").copy()
    df["ts"] = pd.to_datetime(df["ts"], utc=True)
    df = df.sort_values("ts").reset_index(drop=True)

    splits = split_walkforward(df)
    cfg = load_cfg()

    for split_name, split_df in splits.items():
        print("\n==============================")
        print("SPLIT:", split_name)
        print("==============================")

        # ---- Probability policy ----
        print("\nRunning probability policy...")
        signals_prob = build_signals_v2(split_df)
        trades_p, equity_p, metrics_p = run_engine(split_df, signals_prob, cfg)
        print("prob_metrics:", metrics_p)

        # ---- Edge policy ----
        print("\nRunning edge policy...")
        signals_edge = build_signals_v2_edge(split_df)
        trades_e, equity_e, metrics_e = run_engine(split_df, signals_edge, cfg)
        print("edge_metrics:", metrics_e)

        print("\n------------------------------")
        print("Summary for", split_name)
        print("Prob final_equity:", metrics_p["final_equity"])
        print("Edge final_equity:", metrics_e["final_equity"])
        print("Prob max_dd:", metrics_p["max_drawdown"])
        print("Edge max_dd:", metrics_e["max_drawdown"])
        print("Prob trades:", metrics_p["num_trades"])
        print("Edge trades:", metrics_e["num_trades"])
        print("------------------------------\n")


if __name__ == "__main__":
    main()

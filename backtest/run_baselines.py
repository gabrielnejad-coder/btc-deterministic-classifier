import json
import pandas as pd
import matplotlib.pyplot as plt

from backtest.engine import EngineConfig, run_engine
from model.baselines import always_up, yesterday_equals_today

CANON_PATH = "data_parquet/BTCUSD_USD_1h_20220323_now.parquet"


def write_outputs(prefix: str, trades: pd.DataFrame, equity: pd.DataFrame, metrics: dict):
    trades.to_parquet(f"reports/{prefix}_trades.parquet", index=False)
    equity.to_parquet(f"reports/{prefix}_equity.parquet", index=False)
    with open(f"reports/{prefix}_metrics.json", "w") as f:
        json.dump(metrics, f, indent=2)


def main():
    df = pd.read_parquet(CANON_PATH)
    df["ts"] = pd.to_datetime(df["ts"], utc=True)
    df = df.sort_values("ts").reset_index(drop=True)

    cfg = EngineConfig(
        fee_taker=0.0004,
        slippage_side=0.0001,
        stop_loss_pct=0.02,
        initial_equity=1.0,
    )

    ts = pd.DatetimeIndex(df["ts"])

    # always_up baseline
    sig1 = always_up(ts)
    t1, e1, m1 = run_engine(df, sig1, cfg)
    write_outputs("baseline_always_up", t1, e1, m1)

    # yesterday_equals_today baseline
    close = pd.Series(df["close"].values, index=ts)
    sig2 = yesterday_equals_today(close)
    t2, e2, m2 = run_engine(df, sig2, cfg)
    write_outputs("baseline_yday_eq_today", t2, e2, m2)

    # plot equity curves
    plt.figure()
    plt.plot(e1["ts"], e1["equity"], label="always_up")
    plt.plot(e2["ts"], e2["equity"], label="yday_eq_today")
    plt.legend()
    plt.title("Baseline equity curves")
    plt.xlabel("ts")
    plt.ylabel("equity")
    plt.tight_layout()
    plt.savefig("reports/baseline_equity_plot.png", dpi=150)

    print(
        "always_up_final_equity",
        m1["final_equity"],
        "max_dd",
        m1["max_drawdown"],
        "trades",
        m1["num_trades"],
    )
    print(
        "yday_eq_today_final_equity",
        m2["final_equity"],
        "max_dd",
        m2["max_drawdown"],
        "trades",
        m2["num_trades"],
    )


if __name__ == "__main__":
    main()

import json
from pathlib import Path

import pandas as pd

from backtest.engine import EngineConfig, run_engine
from backtest.walkforward import split_walkforward
from model.baselines import always_up, yesterday_equals_today


def load_cfg(path: str = "config/v1.yaml") -> EngineConfig:
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"missing config file: {path}")

    txt = p.read_text()

    cfg = None
    try:
        import yaml  # type: ignore
        cfg = yaml.safe_load(txt)
    except Exception:
        cfg = None

    if not isinstance(cfg, dict):
        cfg = {}
        for line in txt.splitlines():
            s = line.strip()
            if not s or s.startswith("#") or ":" not in s:
                continue
            k, v = s.split(":", 1)
            cfg[k.strip()] = v.strip()

    def get_num(key: str, default: float) -> float:
        v = cfg.get(key, default)
        if isinstance(v, (int, float)):
            return float(v)
        try:
            return float(str(v))
        except Exception:
            return float(default)

    def get_int(key: str, default: int) -> int:
        v = cfg.get(key, default)
        if isinstance(v, int):
            return int(v)
        try:
            return int(float(str(v)))
        except Exception:
            return int(default)

    def get_bool(key: str, default: bool) -> bool:
        v = cfg.get(key, default)
        if isinstance(v, bool):
            return bool(v)
        s = str(v).strip().lower()
        if s in ("true", "1", "yes", "y"):
            return True
        if s in ("false", "0", "no", "n"):
            return False
        return bool(default)

    fee = get_num("fee", 0.0004)
    slippage = get_num("slippage", 0.0001)
    stop_loss = get_num("stop_loss", 0.02)
    hold_min = get_int("hold_min_bars", 12)
    one_pos = get_bool("one_position_only", True)

    initial_equity = get_num("initial_equity", 1000.0)

    return EngineConfig(
        fee_taker=float(fee),
        slippage_side=float(slippage),
        stop_loss_pct=float(stop_loss),
        hold_min_bars=int(hold_min),
        initial_equity=float(initial_equity),
        one_position=bool(one_pos),
    )


def load_btc_df() -> pd.DataFrame:
    df = pd.read_parquet("data_parquet/BTCUSD_USD_1h_20220323_now.parquet").copy()
    df["ts"] = pd.to_datetime(df["ts"], utc=True)
    df = df.sort_values("ts").reset_index(drop=True)
    return df


def write_outputs(prefix: str, split_name: str, trades: pd.DataFrame, equity: pd.DataFrame, metrics: dict):
    Path("reports").mkdir(parents=True, exist_ok=True)

    trades.to_parquet(f"reports/{prefix}_{split_name}_trades.parquet", index=False)
    equity.to_parquet(f"reports/{prefix}_{split_name}_equity.parquet", index=False)

    with open(f"reports/{prefix}_{split_name}_metrics.json", "w") as f:
        json.dump(metrics, f, indent=2)


def main():
    cfg = load_cfg("config/v1.yaml")
    df = load_btc_df()

    splits = split_walkforward(df)

    for split_name in ("train", "validate", "test"):
        split_df = splits[split_name].copy().reset_index(drop=True)
        print("Running baselines for", split_name, "rows", len(split_df))

        sig_up = always_up(split_df)
        t1, e1, m1 = run_engine(split_df, sig_up, cfg)
        write_outputs("baseline_always_up", split_name, t1, e1, m1)

        sig_y = yesterday_equals_today(split_df)
        t2, e2, m2 = run_engine(split_df, sig_y, cfg)
        write_outputs("baseline_yday_eq_today", split_name, t2, e2, m2)

    print("WROTE reports/baseline_*_{train,validate,test}_{metrics,equity,trades}.json/parquet")


if __name__ == "__main__":
    main()

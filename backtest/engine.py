from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Tuple
import pandas as pd


@dataclass(frozen=True)
class EngineConfig:
    fee_taker: float          # ex 0.0004
    slippage_side: float      # ex 0.0001
    stop_loss_pct: float      # ex 0.02
    initial_equity: float     # 1.0 normalized
    one_position: bool = True


def _apply_fill_price(direction: str, raw_price: float, slip: float) -> float:
    if direction == "long_entry":
        return raw_price * (1.0 + slip)
    if direction == "long_exit":
        return raw_price * (1.0 - slip)
    if direction == "short_entry":
        return raw_price * (1.0 - slip)
    if direction == "short_exit":
        return raw_price * (1.0 + slip)
    raise ValueError("bad direction")


def run_engine(
    df: pd.DataFrame,
    signals: pd.Series,
    cfg: EngineConfig,
) -> Tuple[pd.DataFrame, pd.DataFrame, Dict[str, Any]]:

    required_cols = {"ts", "open", "high", "low", "close", "volume"}
    missing = required_cols - set(df.columns)
    if missing:
        raise ValueError(f"df missing cols: {missing}")

    df = df.sort_values("ts").reset_index(drop=True).copy()
    df["ts"] = pd.to_datetime(df["ts"], utc=True)

    sig = signals.copy()
    if not isinstance(sig.index, pd.DatetimeIndex):
        raise ValueError("signals index must be DateTimeIndex aligned to df ts")
    sig.index = pd.to_datetime(sig.index, utc=True)
    sig = sig.reindex(df["ts"]).fillna("flat")

    equity = float(cfg.initial_equity)
    peak = float(cfg.initial_equity)

    position = "flat"  # flat | long | short
    entry_px = None

    pending_entry_side = None          # "long" | "short"
    pending_entry_trade_idx = None     # int

    pending_exit = False               # exit at next bar open

    trades: List[Dict[str, Any]] = []
    equity_rows: List[Dict[str, Any]] = []

    def record_equity(ts_val: pd.Timestamp) -> None:
        nonlocal peak
        peak = max(peak, equity)
        dd = (peak - equity) / peak if peak > 0 else 0.0
        equity_rows.append(
            {"ts": ts_val, "equity": float(equity), "peak": float(peak), "drawdown": float(dd)}
        )

    n = len(df)

    for i in range(n):
        ts = df.loc[i, "ts"]
        o = float(df.loc[i, "open"])
        h = float(df.loc[i, "high"])
        l = float(df.loc[i, "low"])
        c = float(df.loc[i, "close"])

        # 1) Execute pending entry at THIS bar open
        if pending_entry_side is not None and position == "flat":
            if pending_entry_trade_idx is None:
                raise RuntimeError("pending entry has no trade index")

            if pending_entry_side == "long":
                fill_px = _apply_fill_price("long_entry", o, cfg.slippage_side)
            else:
                fill_px = _apply_fill_price("short_entry", o, cfg.slippage_side)

            fee_entry = equity * float(cfg.fee_taker)
            equity -= fee_entry

            position = pending_entry_side
            entry_px = fill_px

            t = trades[pending_entry_trade_idx]
            t["entry_px"] = float(fill_px)
            t["fee_entry"] = float(fee_entry)

            pending_entry_side = None
            pending_entry_trade_idx = None

        # 2) Execute pending exit at THIS bar open
        if pending_exit and position in ("long", "short"):
            if entry_px is None:
                raise RuntimeError("exit attempted with no entry price")
            if not trades:
                raise RuntimeError("exit attempted with no trade record")

            if position == "long":
                exit_px = _apply_fill_price("long_exit", o, cfg.slippage_side)
                gross_ret = (exit_px / entry_px) - 1.0
            else:
                exit_px = _apply_fill_price("short_exit", o, cfg.slippage_side)
                gross_ret = (entry_px / exit_px) - 1.0

            equity *= (1.0 + gross_ret)

            fee_exit = equity * float(cfg.fee_taker)
            equity -= fee_exit

            trades[-1]["exit_ts"] = ts
            trades[-1]["exit_px"] = float(exit_px)
            trades[-1]["fee_exit"] = float(fee_exit)
            trades[-1]["exit_reason"] = trades[-1]["exit_reason"] or "signal"

            position = "flat"
            entry_px = None
            pending_exit = False

        # 3) Intrabar stop check
        if position == "long":
            if entry_px is None:
                raise RuntimeError("long position with no entry price")
            stop_px = entry_px * (1.0 - float(cfg.stop_loss_pct))
            if l <= stop_px:
                if not trades:
                    raise RuntimeError("stop attempted with no trade record")

                stop_fill = stop_px * (1.0 - float(cfg.slippage_side))
                gross_ret = (stop_fill / entry_px) - 1.0
                equity *= (1.0 + gross_ret)

                fee_exit = equity * float(cfg.fee_taker)
                equity -= fee_exit

                trades[-1]["exit_ts"] = ts
                trades[-1]["exit_px"] = float(stop_fill)
                trades[-1]["fee_exit"] = float(fee_exit)
                trades[-1]["exit_reason"] = "stop"

                position = "flat"
                entry_px = None
                pending_exit = False

        elif position == "short":
            if entry_px is None:
                raise RuntimeError("short position with no entry price")
            stop_px = entry_px * (1.0 + float(cfg.stop_loss_pct))
            if h >= stop_px:
                if not trades:
                    raise RuntimeError("stop attempted with no trade record")

                stop_fill = stop_px * (1.0 + float(cfg.slippage_side))
                gross_ret = (entry_px / stop_fill) - 1.0
                equity *= (1.0 + gross_ret)

                fee_exit = equity * float(cfg.fee_taker)
                equity -= fee_exit

                trades[-1]["exit_ts"] = ts
                trades[-1]["exit_px"] = float(stop_fill)
                trades[-1]["fee_exit"] = float(fee_exit)
                trades[-1]["exit_reason"] = "stop"

                position = "flat"
                entry_px = None
                pending_exit = False

        # 4) Decide at bar close, schedule actions for next bar open
        s = sig.iloc[i]
        can_fill_next = (i + 1) < n

        if position == "flat":
            if s == "up":
                pending_entry_side = "long"
                trades.append(
                    {
                        "entry_ts": ts,
                        "side": "long",
                        "entry_px": None,
                        "exit_ts": None,
                        "exit_px": None,
                        "exit_reason": None,
                        "fee_entry": 0.0,
                        "fee_exit": 0.0,
                    }
                )
                pending_entry_trade_idx = len(trades) - 1

            elif s == "down":
                pending_entry_side = "short"
                trades.append(
                    {
                        "entry_ts": ts,
                        "side": "short",
                        "entry_px": None,
                        "exit_ts": None,
                        "exit_px": None,
                        "exit_reason": None,
                        "fee_entry": 0.0,
                        "fee_exit": 0.0,
                    }
                )
                pending_entry_trade_idx = len(trades) - 1

            # FIX 4: Handle last bar / single bar case
            if pending_entry_side is not None and not can_fill_next:
                if pending_entry_side == "long":
                    fill_px = _apply_fill_price("long_entry", o, cfg.slippage_side)
                else:
                    fill_px = _apply_fill_price("short_entry", o, cfg.slippage_side)

                fee_entry = equity * float(cfg.fee_taker)
                equity -= fee_entry

                position = pending_entry_side
                entry_px = fill_px

                trades[-1]["entry_px"] = float(fill_px)
                trades[-1]["fee_entry"] = float(fee_entry)

                pending_entry_side = None
                pending_entry_trade_idx = None

                # Check stop immediately for this bar
                if position == "long":
                    stop_px = entry_px * (1.0 - float(cfg.stop_loss_pct))
                    if l <= stop_px:
                        stop_fill = stop_px * (1.0 - float(cfg.slippage_side))
                        gross_ret = (stop_fill / entry_px) - 1.0
                        equity *= (1.0 + gross_ret)
                        fee_exit = equity * float(cfg.fee_taker)
                        equity -= fee_exit
                        trades[-1]["exit_ts"] = ts
                        trades[-1]["exit_px"] = float(stop_fill)
                        trades[-1]["fee_exit"] = float(fee_exit)
                        trades[-1]["exit_reason"] = "stop"
                        position = "flat"
                        entry_px = None

                elif position == "short":
                    stop_px = entry_px * (1.0 + float(cfg.stop_loss_pct))
                    if h >= stop_px:
                        stop_fill = stop_px * (1.0 + float(cfg.slippage_side))
                        gross_ret = (entry_px / stop_fill) - 1.0
                        equity *= (1.0 + gross_ret)
                        fee_exit = equity * float(cfg.fee_taker)
                        equity -= fee_exit
                        trades[-1]["exit_ts"] = ts
                        trades[-1]["exit_px"] = float(stop_fill)
                        trades[-1]["fee_exit"] = float(fee_exit)
                        trades[-1]["exit_reason"] = "stop"
                        position = "flat"
                        entry_px = None

        else:
            # FIX 5: Handle last bar exit
            if position == "long" and s == "down":
                if can_fill_next:
                    pending_exit = True
                else:
                    # Last bar exit - exit immediately at this bar's open
                    exit_px = _apply_fill_price("long_exit", o, cfg.slippage_side)
                    gross_ret = (exit_px / entry_px) - 1.0
                    equity *= (1.0 + gross_ret)
                    fee_exit = equity * float(cfg.fee_taker)
                    equity -= fee_exit
                    trades[-1]["exit_ts"] = ts
                    trades[-1]["exit_px"] = float(exit_px)
                    trades[-1]["fee_exit"] = float(fee_exit)
                    trades[-1]["exit_reason"] = "signal"
                    position = "flat"
                    entry_px = None

            elif position == "short" and s == "up":
                if can_fill_next:
                    pending_exit = True
                else:
                    # Last bar exit - exit immediately at this bar's open
                    exit_px = _apply_fill_price("short_exit", o, cfg.slippage_side)
                    gross_ret = (entry_px / exit_px) - 1.0
                    equity *= (1.0 + gross_ret)
                    fee_exit = equity * float(cfg.fee_taker)
                    equity -= fee_exit
                    trades[-1]["exit_ts"] = ts
                    trades[-1]["exit_px"] = float(exit_px)
                    trades[-1]["fee_exit"] = float(fee_exit)
                    trades[-1]["exit_reason"] = "signal"
                    position = "flat"
                    entry_px = None

        record_equity(ts)

    trades_df = pd.DataFrame(trades)
    equity_df = pd.DataFrame(equity_rows)

    if not trades_df.empty:
        trades_df["entry_ts"] = pd.to_datetime(trades_df["entry_ts"], utc=True)
        trades_df["exit_ts"] = pd.to_datetime(trades_df["exit_ts"], utc=True)

    if not equity_df.empty:
        equity_df["ts"] = pd.to_datetime(equity_df["ts"], utc=True)

    max_dd = float(equity_df["drawdown"].max()) if not equity_df.empty else 0.0
    final_equity = float(equity_df["equity"].iloc[-1]) if not equity_df.empty else float(cfg.initial_equity)
    total_ret = float(final_equity - float(cfg.initial_equity))

    metrics = {
        "initial_equity": float(cfg.initial_equity),
        "final_equity": final_equity,
        "total_return": total_ret,
        "max_drawdown": max_dd,
        "num_trades": int(len(trades_df)),
        "num_wins": int(
            (
                ((trades_df["exit_px"] > trades_df["entry_px"]) & (trades_df["side"] == "long")).sum()
                + ((trades_df["exit_px"] < trades_df["entry_px"]) & (trades_df["side"] == "short")).sum()
            )
        ) if not trades_df.empty else 0,
    }

    return trades_df, equity_df, metrics

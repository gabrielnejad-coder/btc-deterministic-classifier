from pathlib import Path
import pandas as pd
import yfinance as yf

TICKERS = {
    "SPY": "SPY",
    "QQQ": "QQQ",
    "DXY": "DX-Y.NYB",
    "VIX": "^VIX",
    "MSTR": "MSTR",
    "NVDA": "NVDA",
}

MAX_DAYS = 729

def _flatten_cols(df: pd.DataFrame) -> pd.DataFrame:
    # yfinance often returns MultiIndex columns like ('Open','SPY')
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [c[0] if isinstance(c, tuple) else str(c) for c in df.columns]
    else:
        df.columns = [str(c) for c in df.columns]
    return df

def _normalize(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or len(df) == 0:
        return pd.DataFrame(columns=["ts","open","high","low","close","volume"])

    df = _flatten_cols(df).copy()

    # index is datetime, make it a column
    df = df.reset_index()

    # locate ts column name
    if "Datetime" in df.columns:
        df = df.rename(columns={"Datetime": "ts"})
    elif "Date" in df.columns:
        df = df.rename(columns={"Date": "ts"})
    elif "index" in df.columns:
        df = df.rename(columns={"index": "ts"})

    df = df.rename(columns={
        "Open": "open",
        "High": "high",
        "Low": "low",
        "Close": "close",
        "Adj Close": "adj_close",
        "Volume": "volume",
    })

    required = {"ts","open","high","low","close","volume"}
    if not required.issubset(set(df.columns)):
        return pd.DataFrame(columns=["ts","open","high","low","close","volume"])

    out = df[["ts","open","high","low","close","volume"]].copy()
    out["ts"] = pd.to_datetime(out["ts"], utc=True, errors="coerce")
    for c in ["open","high","low","close","volume"]:
        out[c] = pd.to_numeric(out[c], errors="coerce")
    out["volume"] = out["volume"].fillna(0.0)

    out = out.dropna(subset=["ts","open","high","low","close"])
    out = out.drop_duplicates(subset=["ts"]).sort_values("ts").reset_index(drop=True)
    return out

def fetch_last_days_1h(ticker: str) -> pd.DataFrame:
    # using period avoids start/end window errors
    df = yf.download(
        ticker,
        period=f"{MAX_DAYS}d",
        interval="1h",
        auto_adjust=False,
        progress=False,
    )
    return _normalize(df)

def main() -> None:
    Path("data_parquet").mkdir(exist_ok=True)

    for asset, ticker in TICKERS.items():
        print("FETCH", asset, ticker, "period", f"{MAX_DAYS}d")
        df = fetch_last_days_1h(ticker)
        out_path = Path(f"data_parquet/{asset}_1h.parquet")
        df.to_parquet(out_path, index=False)

        if len(df) == 0:
            print("  NO DATA", asset)
        else:
            print("  WROTE", out_path, "rows", len(df), "ts_min", df["ts"].min(), "ts_max", df["ts"].max())

if __name__ == "__main__":
    main()

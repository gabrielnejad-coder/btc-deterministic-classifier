import pandas as pd

def build_signals_v2(df: pd.DataFrame) -> pd.Series:
    """
    Strategy V2 goals:
    - Reduce trade frequency
    - Introduce NO-TRADE regime
    - Enforce directional bias only when edge exists
    """

    ts = pd.to_datetime(df["ts"], utc=True)
    close = pd.to_numeric(df["close"], errors="coerce")
    close.index = ts

    # Placeholder: flat everywhere
    signals = pd.Series("flat", index=ts, dtype="object")

    return signals

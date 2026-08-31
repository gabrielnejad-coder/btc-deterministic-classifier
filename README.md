# BTC Deterministic Classifier

**End-to-end Bitcoin trading research, built for reproducibility.**

A Python research system that goes from raw exchange data to a trained classifier to always-on paper traders — with every step deterministic and re-runnable. The point is honest measurement: same inputs, same features, same signals, every time.

## Pipeline

```
data_raw/        raw Kraken market data (keyless public API)
data_scripts/    ingestion + canonicalization
data_parquet/    canonical parquet datasets
features/        engineered feature set (v2: wider features, longer holds)
model/           trained deterministic classifier
backtest/        backtesting harness
reports/         backtest reports
live/            always-on paper traders (Railway deploy)
tests/           test suite
```

## How it works

1. **Ingest** — pull raw BTC market data from Kraken (no API keys needed) into a canonical, versioned dataset
2. **Features** — engineer a reproducible feature set over the canonical data
3. **Train** — fit a deterministic classifier (v2 widened the feature set and extended holding periods)
4. **Backtest** — run the strategy through the harness and write reports; v1 lessons (flat-signal defaults, stable alignment) are encoded as tests
5. **Paper-trade** — deploy always-on paper traders to Railway to see how the signals behave forward, out of sample, with no real money

## Project discipline

- `PROJECT_CONSTITUTION.md` — the rules the project holds itself to
- `DEPLOYMENT.md` — runbook for the always-on paper traders
- Paper traders can be paused via a no-op start command without tearing down the deploy

## Stack

Python · pandas / parquet · scikit-style classifier training · Railway

## Status

Research project, built solo. Paper trading only — this system never places real orders.

> Not financial advice. This is a research codebase for studying whether deterministic, reproducible pipelines produce honest signal measurements — not a product or a recommendation to trade.

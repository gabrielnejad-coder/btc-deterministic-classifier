# Two long-running workers, one per strategy. On Railway, create TWO services
# and give each the matching start command below (or set TRADER as a service
# env var and use `python -u live/runner.py` for both). These are NOT web
# processes — there is no HTTP port; they loop forever.
worker060: TRADER=060 python -u live/runner.py
worker050: TRADER=050 python -u live/runner.py

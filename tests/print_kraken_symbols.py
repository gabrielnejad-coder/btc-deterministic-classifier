import ccxt

ex = ccxt.krakenfutures()
ex.load_markets()

for s in ex.symbols:
    if "BTC" in s or "XBT" in s:
        print(s)


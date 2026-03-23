"""
03_column_ops.py — Column management: add, drop, rename, update.
"""

import sys
import time

sys.path.insert(0, "../src")

from DataTable import DataTable


# ── Setup ─────────────────────────────────────────────────────────────────────
def make_price_table(tickers_prices):
    dt = DataTable(["ticker", "price"])
    for ticker, price in tickers_prices:
        dt.list_append([ticker, price], key=time.time())
        time.sleep(0.001)
    return dt


dt = make_price_table(
    [
        ("BTC-USDT", 42_000.0),
        ("ETH-USDT", 2_800.0),
        ("SOL-USDT", 150.0),
    ]
)
print("=== Original ===")
print(dt)

# ── rename_column ─────────────────────────────────────────────────────────────
dt.rename_column(price="close")
print("=== After rename price→close ===")
print(dt.header_())  # ['ticker', 'close']

# ── drop_column ───────────────────────────────────────────────────────────────
dt2 = dt.copy()
dt2.drop_column("ticker")
print("=== After dropping ticker ===")
print(dt2.header_())  # ['close']

# ── update_column ─────────────────────────────────────────────────────────────
keys = dt.index
dt.update_column("close", {keys[0]: 41_500.0, keys[1]: 2_850.0})
print("=== After update_column ===")
print(dt)

# ── update_rows ───────────────────────────────────────────────────────────────
dt3 = DataTable(["ticker", "price", "status"])
for ticker, price in [("BTC-USDT", 42_000.0), ("ETH-USDT", 2_800.0)]:
    dt3.list_append([ticker, price, "open"], key=time.time())
    time.sleep(0.001)

dt3.update_rows(dt3.index, {"status": "closed"})
print("=== After update_rows (status→closed) ===")
print(dt3)

# ── add_column ────────────────────────────────────────────────────────────────
dt_vol = DataTable(["ticker", "volume"])
for k, ticker in zip(dt.index, ["BTC-USDT", "ETH-USDT", "SOL-USDT"]):
    dt_vol.list_append([ticker, 500 * (dt.index.index(k) + 1)], key=k)

dt4 = dt.copy()
dt4.add_column(dt_vol)
print("=== After add_column (volume) ===")
print(dt4)

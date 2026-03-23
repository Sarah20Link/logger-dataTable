"""
02_querying.py — Slicing, filtering, and column selection.
"""

import sys
import time

sys.path.insert(0, "../src")

from DataTable import DataTable

# ── Setup ─────────────────────────────────────────────────────────────────────
dt = DataTable(["ticker", "side", "price", "qty"])
keys = []
data = [
    ("BTC-USDT", "buy", 42_000.0, 0.50),
    ("ETH-USDT", "sell", 2_800.0, 1.00),
    ("BTC-USDT", "sell", 43_000.0, 0.25),
    ("SOL-USDT", "buy", 150.0, 10.00),
    ("ETH-USDT", "buy", 2_750.0, 2.00),
]
for row in data:
    k = time.time()
    dt.list_append(list(row), key=k)
    keys.append(k)
    time.sleep(0.001)

# ── get_top_rows ───────────────────────────────────────────────────────────────
print("=== Most recent 2 rows ===")
print(dt.get_top_rows(2))

print("=== Oldest 2 rows ===")
print(dt.get_top_rows(2, top=False))

# ── get_slice ──────────────────────────────────────────────────────────────────
print("=== Slice: rows 1-3 (by key range) ===")
print(dt.get_slice(keys[0], keys[3], includeEnd=False))

# Slice syntax also works:
print("=== Slice syntax dt[start:stop] ===")
print(dt[keys[1] : keys[3]])

# ── get_rows_by_keys ──────────────────────────────────────────────────────────
print("=== Rows by specific keys ===")
print(dt.get_rows_by_keys(keys[0], keys[2]))

# ── get_rows_by_col_value ─────────────────────────────────────────────────────
print("=== BTC-USDT OR buy orders ===")
result = dt.get_rows_by_col_value({"ticker": ["BTC-USDT"], "side": ["buy"]})
print(result)

print("=== All sell orders ===")
print(dt.get_rows_by_col_value({"side": ["sell"]}))

# ── get_rows_by_cell_value ────────────────────────────────────────────────────
print("=== Rows containing value 150.0 anywhere ===")
print(dt.get_rows_by_cell_value(150.0))

# ── get_cols ──────────────────────────────────────────────────────────────────
print("=== price column only ===")
print(dt.get_cols("price"))

print("=== price + qty columns ===")
print(dt.get_cols(["price", "qty"]))

# ── get_row_count_by_col_value ────────────────────────────────────────────────
buy_count = dt.get_row_count_by_col_value("side", "buy")
print(f"Number of buy orders: {buy_count}")

"""
05_math_and_csv.py — Math operations, period return, and CSV round-trip.
"""

import sys
from io import StringIO

sys.path.insert(0, "../src")

from DataTable import DataTable

# ── Setup: single-column price table ─────────────────────────────────────────
prices = [100.0, 105.0, 103.0, 110.0, 108.0]
dt_price = DataTable(["close"])
keys = []
for p in prices:
    k = float(len(keys) + 1)  # simple integer-like keys for clarity
    dt_price.list_append([p], key=k)
    keys.append(k)

print("=== Prices ===")
print(dt_price)

# ── abs_col ───────────────────────────────────────────────────────────────────
dt_neg = DataTable(["pnl"])
for k, v in zip(keys, [-5.0, 3.0, -2.0, 7.0, -1.0]):
    dt_neg.list_append([v], key=k)

print("=== abs_col ===")
print(dt_neg.abs_col("pnl"))

# ── sum / average ─────────────────────────────────────────────────────────────
print(f"sum_of_col('close'): {dt_price.sum_of_col('close')}")  # 526.0
print(f"average('close'):    {dt_price.average('close')}")  # 105.2

# ── multiply / divide / add / subtract / mod ─────────────────────────────────
print("\n=== multiply_col(2) ===")
print(dt_price.multiply_col(2))

print("=== divide_col(100) → normalise to ratio ===")
print(dt_price.divide_col(100))

print("=== add_col(10) ===")
print(dt_price.add_col(10))

print("=== subtract_col(100) ===")
print(dt_price.subtract_col(100))

print("=== mod_col(10) ===")
print(dt_price.mod_col(10))

# ── period_return ─────────────────────────────────────────────────────────────
print("=== period_return (chronological) ===")
print(dt_price.period_return("close"))

# ── pct (open→close % change) ────────────────────────────────────────────────
dt_ohlc = DataTable(["open", "close"])
for k, (o, c) in zip(
    keys, [(100, 105), (105, 103), (103, 110), (110, 108), (108, 112)]
):
    dt_ohlc.list_append([o, c], key=k)

print("=== pct(open, close) ===")
print(dt_ohlc.pct("open", "close"))

# ── table_duration ────────────────────────────────────────────────────────────
print(f"table_duration: {dt_price.table_duration()}")  # 4.0

# ── CSV round-trip ────────────────────────────────────────────────────────────
print("\n=== to_csv (string) ===")
csv_str = dt_price.to_csv()
print(csv_str)

print("=== to_csv → StringIO → from_csv ===")
buf = StringIO()
dt_price.to_csv(output=buf)
buf.seek(0)
print(buf.read())

dt_price.to_csv("prices_out.csv")
dt_loaded = DataTable.from_csv("prices_out.csv")
print("Loaded from CSV:", dt_loaded.shape)

import os

os.remove("prices_out.csv")

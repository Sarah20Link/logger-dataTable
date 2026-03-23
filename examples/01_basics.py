import time

from DataTable import DataTable

# ── Create ────────────────────────────────────────────────────────────────────
dt = DataTable(["ticker", "side", "price", "qty"], key_unit="s")
print("Empty table:", dt.shape)  # (0, 4)
print("Is empty:", dt.empty)  # True

# ── Append rows ───────────────────────────────────────────────────────────────
t1 = time.time()
dt.list_append(["BTC-USDT", "buy", 42_000.0, 0.5], key=t1)

time.sleep(0.001)
t2 = time.time()
dt.list_append(["ETH-USDT", "sell", 2_800.0, 1.0], key=t2)

time.sleep(0.001)
dt.list_append(["BTC-USDT", "sell", 43_000.0, 0.25])

print("\nAfter 3 rows:", dt.shape)  # (3, 4)
print(dt)

# ── dict_append: multiple rows at once ────────────────────────────────────────
t4 = time.time()
dt.dict_append({t4: ["SOL-USDT", "buy", 150.0, 10.0]})
print("After dict_append:", dt.shape)  # (4, 4)

# ── dict_extender: append from a named dict, missing cols → None ──────────────
dt2 = DataTable(["ticker", "side", "price", "qty"])
dt2.dict_extender({"ticker": "ADA-USDT", "price": 0.45})
print("\ndict_extender (missing cols → None):")
print(dt2)

# ── Access ────────────────────────────────────────────────────────────────────
print("Row by key:   ", dt[t1])
print("Cell (price): ", dt.get_cell(t1, "price"))
print("Row as dict:  ", dt.row_to_dict(t1))
print("Index:        ", dt.index)

# ── Update ────────────────────────────────────────────────────────────────────
dt.update_cell(t1, "price", 41_500.0)
print("\nAfter price update:", dt.get_cell(t1, "price"))

# ── Delete ────────────────────────────────────────────────────────────────────
del dt[t4]
print("After delete:", dt.shape)  # (3, 4)

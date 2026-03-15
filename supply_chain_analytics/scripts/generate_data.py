"""
generate_data.py
Generates realistic synthetic supply chain data and saves to CSV.
"""

import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta
import os

# ── Reproducibility ──────────────────────────────────────────────────────────
np.random.seed(42)
random.seed(42)

# ── Reference data ───────────────────────────────────────────────────────────
PRODUCTS = {
    "P001": ("Electronics",   120.00),
    "P002": ("Electronics",   250.00),
    "P003": ("Apparel",        35.00),
    "P004": ("Apparel",        55.00),
    "P005": ("Home & Garden",  80.00),
    "P006": ("Home & Garden", 150.00),
    "P007": ("Food & Bev",     12.00),
    "P008": ("Food & Bev",     20.00),
    "P009": ("Industrial",    300.00),
    "P010": ("Industrial",    500.00),
}

SUPPLIERS = ["Apex Supplies", "GlobalTrade Co.", "SwiftVendors",
             "PrimeSources", "NextGen Parts"]

WAREHOUSES = ["Dallas TX", "Chicago IL", "Los Angeles CA",
              "New York NY", "Atlanta GA"]

STATUSES = ["Delivered", "Delivered", "Delivered",   # weight toward Delivered
            "Pending", "In Transit", "Delayed", "Cancelled"]


def random_date(start: datetime, end: datetime) -> datetime:
    return start + timedelta(days=random.randint(0, (end - start).days))


def generate_orders(n: int = 2000) -> pd.DataFrame:
    start, end = datetime(2023, 1, 1), datetime(2024, 12, 31)
    rows = []
    for i in range(1, n + 1):
        product_id                = random.choice(list(PRODUCTS))
        category, unit_price      = PRODUCTS[product_id]
        quantity                  = random.randint(1, 100)
        discount                  = round(random.choice([0, 0, 0, 0.05, 0.10, 0.15]), 2)
        supplier                  = random.choice(SUPPLIERS)
        warehouse                 = random.choice(WAREHOUSES)
        order_date                = random_date(start, end)
        promised_delivery_days    = random.randint(3, 14)
        actual_delivery_days      = promised_delivery_days + random.randint(-2, 10)
        status                    = random.choice(STATUSES)
        # Inject ~5 % nulls in contact_email to simulate dirty data
        contact_email = (f"buyer{i}@client.com"
                         if random.random() > 0.05 else None)

        rows.append({
            "order_id":               f"ORD-{i:05d}",
            "product_id":             product_id,
            "category":               category,
            "supplier":               supplier,
            "warehouse":              warehouse,
            "quantity":               quantity,
            "unit_price":             unit_price,
            "discount":               discount,
            "order_date":             order_date.strftime("%Y-%m-%d"),
            "promised_delivery_days": promised_delivery_days,
            "actual_delivery_days":   actual_delivery_days,
            "status":                 status,
            "contact_email":          contact_email,
        })
    return pd.DataFrame(rows)


def generate_inventory(n_products: int = 10) -> pd.DataFrame:
    rows = []
    for pid, (cat, price) in PRODUCTS.items():
        rows.append({
            "product_id":      pid,
            "category":        cat,
            "stock_on_hand":   random.randint(50, 500),
            "reorder_level":   random.randint(20, 80),
            "unit_cost":       round(price * 0.6, 2),
            "last_restocked":  random_date(datetime(2024, 1, 1),
                                           datetime(2024, 12, 1)).strftime("%Y-%m-%d"),
        })
    return pd.DataFrame(rows)


if __name__ == "__main__":
    out = os.path.join(os.path.dirname(__file__), "..", "data", "raw")
    os.makedirs(out, exist_ok=True)

    orders    = generate_orders(2000)
    inventory = generate_inventory()

    orders.to_csv(   os.path.join(out, "orders.csv"),    index=False)
    inventory.to_csv(os.path.join(out, "inventory.csv"), index=False)

    print(f"✅  Generated {len(orders)} orders    → data/raw/orders.csv")
    print(f"✅  Generated {len(inventory)} products → data/raw/inventory.csv")

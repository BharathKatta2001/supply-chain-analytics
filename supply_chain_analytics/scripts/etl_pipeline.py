"""
etl_pipeline.py
Ingests raw CSVs → cleans & transforms → loads into SQLite → exports
processed CSVs for Power BI.

Pipeline stages
───────────────
1. EXTRACT  – read raw CSVs
2. TRANSFORM – validate, clean, engineer features
3. LOAD     – persist to SQLite + write processed CSVs
"""

import os
import sqlite3
import pandas as pd
import numpy as np

# ── Paths ─────────────────────────────────────────────────────────────────────
BASE        = os.path.dirname(os.path.abspath(__file__))
RAW_DIR     = os.path.join(BASE, "..", "data", "raw")
PROC_DIR    = os.path.join(BASE, "..", "data", "processed")
DB_PATH     = os.path.join(BASE, "..", "outputs", "supply_chain.db")
OUTPUT_DIR  = os.path.join(BASE, "..", "outputs")

os.makedirs(PROC_DIR,  exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)


# ═════════════════════════════════════════════════════════════════════════════
# STAGE 1 – EXTRACT
# ═════════════════════════════════════════════════════════════════════════════
def extract() -> tuple[pd.DataFrame, pd.DataFrame]:
    print("\n── STAGE 1: EXTRACT ─────────────────────────────────────────")
    orders    = pd.read_csv(os.path.join(RAW_DIR, "orders.csv"))
    inventory = pd.read_csv(os.path.join(RAW_DIR, "inventory.csv"))
    print(f"   Orders loaded    : {len(orders):,} rows | {orders.shape[1]} cols")
    print(f"   Inventory loaded : {len(inventory):,} rows | {inventory.shape[1]} cols")
    return orders, inventory


# ═════════════════════════════════════════════════════════════════════════════
# STAGE 2 – TRANSFORM
# ═════════════════════════════════════════════════════════════════════════════
def transform_orders(df: pd.DataFrame) -> pd.DataFrame:
    print("\n── STAGE 2: TRANSFORM (Orders) ──────────────────────────────")

    original_count = len(df)

    # ── 2a. Data-type casting ─────────────────────────────────────────────
    df["order_date"] = pd.to_datetime(df["order_date"])
    df["quantity"]   = pd.to_numeric(df["quantity"],   errors="coerce")
    df["unit_price"] = pd.to_numeric(df["unit_price"], errors="coerce")
    df["discount"]   = pd.to_numeric(df["discount"],   errors="coerce").fillna(0)

    # ── 2b. Null handling ─────────────────────────────────────────────────
    null_summary = df.isnull().sum()
    print(f"   Null values detected:\n{null_summary[null_summary > 0].to_string()}")

    df["contact_email"] = df["contact_email"].fillna("unknown@client.com")   # impute
    df.dropna(subset=["quantity", "unit_price"], inplace=True)        # drop unfixable

    # ── 2c. Duplicate removal ─────────────────────────────────────────────
    dupes = df.duplicated(subset="order_id").sum()
    df.drop_duplicates(subset="order_id", inplace=True)
    print(f"   Duplicates removed : {dupes}")

    # ── 2d. Outlier detection – flag orders with quantity > 3 std devs ────
    mean_q, std_q = df["quantity"].mean(), df["quantity"].std()
    df["is_quantity_outlier"] = (df["quantity"] > mean_q + 3 * std_q).astype(int)
    print(f"   Quantity outliers flagged : {df['is_quantity_outlier'].sum()}")

    # ── 2e. Feature engineering ───────────────────────────────────────────
    df["revenue"]           = df["quantity"] * df["unit_price"] * (1 - df["discount"])
    df["revenue"]           = df["revenue"].round(2)

    df["delivery_variance"] = df["actual_delivery_days"] - df["promised_delivery_days"]
    df["is_late"]           = (df["delivery_variance"] > 0).astype(int)
    df["on_time_flag"]      = (~df["delivery_variance"].gt(0)).astype(int)

    df["order_year"]        = df["order_date"].dt.year
    df["order_month"]       = df["order_date"].dt.month
    df["order_quarter"]     = df["order_date"].dt.quarter
    df["order_month_name"]  = df["order_date"].dt.strftime("%b")
    df["order_yearmonth"]   = df["order_date"].dt.to_period("M").astype(str)

    # ── 2f. Standardise status casing ────────────────────────────────────
    df["status"] = df["status"].str.strip().str.title()

    print(f"   Rows after transform : {len(df):,}  (removed {original_count - len(df)})")
    return df


def transform_inventory(df: pd.DataFrame) -> pd.DataFrame:
    print("\n── STAGE 2: TRANSFORM (Inventory) ───────────────────────────")
    df["last_restocked"]   = pd.to_datetime(df["last_restocked"])
    df["below_reorder"]    = (df["stock_on_hand"] < df["reorder_level"]).astype(int)
    df["inventory_value"]  = (df["stock_on_hand"] * df["unit_cost"]).round(2)
    print(f"   Products below reorder level : {df['below_reorder'].sum()}")
    return df


# ═════════════════════════════════════════════════════════════════════════════
# STAGE 3 – LOAD
# ═════════════════════════════════════════════════════════════════════════════
def load(orders: pd.DataFrame, inventory: pd.DataFrame) -> None:
    print("\n── STAGE 3: LOAD ────────────────────────────────────────────")

    # ── 3a. Save processed CSVs (for Power BI) ───────────────────────────
    orders_path    = os.path.join(PROC_DIR, "orders_cleaned.csv")
    inventory_path = os.path.join(PROC_DIR, "inventory_cleaned.csv")
    orders.to_csv(   orders_path,    index=False)
    inventory.to_csv(inventory_path, index=False)
    print(f"   ✅  orders_cleaned.csv    saved → {orders_path}")
    print(f"   ✅  inventory_cleaned.csv saved → {inventory_path}")

    # ── 3b. Load into SQLite ─────────────────────────────────────────────
    con = sqlite3.connect(DB_PATH)

    # Store datetime as string for SQLite compatibility
    orders_db = orders.copy()
    orders_db["order_date"] = orders_db["order_date"].dt.strftime("%Y-%m-%d")

    inventory_db = inventory.copy()
    inventory_db["last_restocked"] = inventory_db["last_restocked"].dt.strftime("%Y-%m-%d")

    orders_db.to_sql(   "orders",    con, if_exists="replace", index=False)
    inventory_db.to_sql("inventory", con, if_exists="replace", index=False)

    con.close()
    print(f"   ✅  SQLite database saved  → {DB_PATH}")


# ═════════════════════════════════════════════════════════════════════════════
# STAGE 4 – EXPORT KPI SUMMARY TABLES (for Power BI direct import)
# ═════════════════════════════════════════════════════════════════════════════
def export_kpi_tables(orders: pd.DataFrame) -> None:
    print("\n── STAGE 4: EXPORT KPI SUMMARY TABLES ──────────────────────")

    # Monthly revenue trend
    monthly = (orders.groupby("order_yearmonth")
                     .agg(total_revenue=("revenue", "sum"),
                          total_orders=("order_id", "count"),
                          avg_order_value=("revenue", "mean"))
                     .reset_index()
                     .round(2))
    monthly.to_csv(os.path.join(OUTPUT_DIR, "kpi_monthly_revenue.csv"), index=False)

    # Category performance
    category = (orders.groupby("category")
                      .agg(total_revenue=("revenue", "sum"),
                           total_orders=("order_id", "count"),
                           avg_discount=("discount", "mean"),
                           on_time_rate=("on_time_flag", "mean"))
                      .reset_index()
                      .round(3))
    category.to_csv(os.path.join(OUTPUT_DIR, "kpi_category_performance.csv"), index=False)

    # Supplier performance
    supplier = (orders.groupby("supplier")
                      .agg(total_revenue=("revenue", "sum"),
                           total_orders=("order_id", "count"),
                           avg_delivery_variance=("delivery_variance", "mean"),
                           late_delivery_rate=("is_late", "mean"))
                      .reset_index()
                      .round(3))
    supplier.to_csv(os.path.join(OUTPUT_DIR, "kpi_supplier_performance.csv"), index=False)

    # Warehouse performance
    warehouse = (orders.groupby("warehouse")
                       .agg(total_revenue=("revenue", "sum"),
                            total_orders=("order_id", "count"),
                            on_time_rate=("on_time_flag", "mean"))
                       .reset_index()
                       .round(3))
    warehouse.to_csv(os.path.join(OUTPUT_DIR, "kpi_warehouse_performance.csv"), index=False)

    # Order status breakdown
    status = (orders.groupby("status")
                    .agg(order_count=("order_id", "count"),
                         total_revenue=("revenue", "sum"))
                    .reset_index()
                    .round(2))
    status.to_csv(os.path.join(OUTPUT_DIR, "kpi_order_status.csv"), index=False)

    print("   ✅  kpi_monthly_revenue.csv")
    print("   ✅  kpi_category_performance.csv")
    print("   ✅  kpi_supplier_performance.csv")
    print("   ✅  kpi_warehouse_performance.csv")
    print("   ✅  kpi_order_status.csv")


# ═════════════════════════════════════════════════════════════════════════════
# MAIN
# ═════════════════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    print("=" * 60)
    print("  SUPPLY CHAIN ETL PIPELINE")
    print("=" * 60)

    orders_raw,    inventory_raw    = extract()
    orders_clean,  inventory_clean  = (transform_orders(orders_raw),
                                       transform_inventory(inventory_raw))
    load(orders_clean, inventory_clean)
    export_kpi_tables(orders_clean)

    print("\n" + "=" * 60)
    print("  ✅  PIPELINE COMPLETE")
    print("=" * 60)

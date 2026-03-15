"""
sql_analysis.py
Runs analytical SQL queries against the SQLite database and prints
formatted results — simulates what you'd run on SQL Server / Snowflake.
"""

import os
import sqlite3
import pandas as pd

DB_PATH    = os.path.join(os.path.dirname(__file__), "..", "outputs", "supply_chain.db")
OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "..", "outputs")
os.makedirs(OUTPUT_DIR, exist_ok=True)

pd.set_option("display.max_columns", None)
pd.set_option("display.width", 120)
pd.set_option("display.float_format", "{:,.2f}".format)


def run_query(con: sqlite3.Connection, label: str, sql: str) -> pd.DataFrame:
    print(f"\n{'═'*60}")
    print(f"  {label}")
    print(f"{'═'*60}")
    df = pd.read_sql_query(sql, con)
    print(df.to_string(index=False))
    return df


def main():
    con = sqlite3.connect(DB_PATH)

    # ── Q1: Total Revenue, Orders & AOV by Month ─────────────────────────────
    monthly_revenue = run_query(con, "Q1 · Monthly Revenue Trend", """
        SELECT
            order_yearmonth                                  AS month,
            COUNT(order_id)                                  AS total_orders,
            ROUND(SUM(revenue), 2)                           AS total_revenue,
            ROUND(AVG(revenue), 2)                           AS avg_order_value
        FROM   orders
        GROUP  BY order_yearmonth
        ORDER  BY order_yearmonth
    """)

    # ── Q2: Revenue & On-Time Rate by Category ───────────────────────────────
    category_kpi = run_query(con, "Q2 · Category KPI Summary", """
        SELECT
            category,
            COUNT(order_id)                                  AS total_orders,
            ROUND(SUM(revenue), 2)                           AS total_revenue,
            ROUND(AVG(discount) * 100, 1)                    AS avg_discount_pct,
            ROUND(AVG(on_time_flag) * 100, 1)                AS on_time_rate_pct
        FROM   orders
        GROUP  BY category
        ORDER  BY total_revenue DESC
    """)

    # ── Q3: Supplier Performance – Late Delivery Rate ────────────────────────
    supplier_kpi = run_query(con, "Q3 · Supplier Performance", """
        SELECT
            supplier,
            COUNT(order_id)                                  AS total_orders,
            ROUND(SUM(revenue), 2)                           AS total_revenue,
            ROUND(AVG(delivery_variance), 2)                 AS avg_delivery_variance_days,
            ROUND(AVG(is_late) * 100, 1)                     AS late_delivery_rate_pct
        FROM   orders
        GROUP  BY supplier
        ORDER  BY late_delivery_rate_pct DESC
    """)

    # ── Q4: Warehouse Revenue & Efficiency ───────────────────────────────────
    warehouse_kpi = run_query(con, "Q4 · Warehouse Performance", """
        SELECT
            warehouse,
            COUNT(order_id)                                  AS total_orders,
            ROUND(SUM(revenue), 2)                           AS total_revenue,
            ROUND(AVG(on_time_flag) * 100, 1)                AS on_time_rate_pct,
            ROUND(AVG(delivery_variance), 2)                 AS avg_delivery_variance_days
        FROM   orders
        GROUP  BY warehouse
        ORDER  BY total_revenue DESC
    """)

    # ── Q5: Anomaly Detection – Orders with Delivery Variance > 7 days ──────
    anomalies = run_query(con, "Q5 · Delivery Anomalies (Variance > 7 Days)", """
        SELECT
            order_id,
            product_id,
            supplier,
            warehouse,
            order_date,
            promised_delivery_days,
            actual_delivery_days,
            delivery_variance,
            status,
            ROUND(revenue, 2)                                AS revenue
        FROM   orders
        WHERE  delivery_variance > 7
        ORDER  BY delivery_variance DESC
        LIMIT  20
    """)

    # ── Q6: MoM Revenue Growth ───────────────────────────────────────────────
    mom_growth = run_query(con, "Q6 · Month-over-Month Revenue Growth", """
        WITH monthly AS (
            SELECT
                order_yearmonth,
                ROUND(SUM(revenue), 2) AS monthly_revenue
            FROM   orders
            GROUP  BY order_yearmonth
        ),
        with_lag AS (
            SELECT
                order_yearmonth,
                monthly_revenue,
                LAG(monthly_revenue) OVER (ORDER BY order_yearmonth) AS prev_month_revenue
            FROM monthly
        )
        SELECT
            order_yearmonth                                           AS month,
            monthly_revenue,
            prev_month_revenue,
            ROUND(
                (monthly_revenue - prev_month_revenue)
                / NULLIF(prev_month_revenue, 0) * 100, 2)            AS mom_growth_pct
        FROM   with_lag
        ORDER  BY order_yearmonth
    """)

    # ── Q7: Top 5 Products by Revenue ────────────────────────────────────────
    top_products = run_query(con, "Q7 · Top 5 Products by Revenue", """
        SELECT
            product_id,
            category,
            COUNT(order_id)                                  AS total_orders,
            SUM(quantity)                                    AS total_units_sold,
            ROUND(SUM(revenue), 2)                           AS total_revenue
        FROM   orders
        GROUP  BY product_id, category
        ORDER  BY total_revenue DESC
        LIMIT  5
    """)

    # ── Q8: Inventory Risk – Items Below Reorder Level ───────────────────────
    inventory_risk = run_query(con, "Q8 · Inventory Risk (Below Reorder Level)", """
        SELECT
            i.product_id,
            i.category,
            i.stock_on_hand,
            i.reorder_level,
            i.reorder_level - i.stock_on_hand   AS shortfall,
            i.inventory_value,
            i.last_restocked
        FROM   inventory i
        WHERE  i.below_reorder = 1
        ORDER  BY shortfall DESC
    """)

    # ── Q9: Revenue by Order Status ──────────────────────────────────────────
    status_breakdown = run_query(con, "Q9 · Revenue by Order Status", """
        SELECT
            status,
            COUNT(order_id)                                  AS order_count,
            ROUND(SUM(revenue), 2)                           AS total_revenue,
            ROUND(AVG(revenue), 2)                           AS avg_revenue
        FROM   orders
        GROUP  BY status
        ORDER  BY total_revenue DESC
    """)

    # ── Q10: Quarterly Revenue Summary ───────────────────────────────────────
    quarterly = run_query(con, "Q10 · Quarterly Revenue Summary", """
        SELECT
            order_year,
            order_quarter,
            COUNT(order_id)                                  AS total_orders,
            ROUND(SUM(revenue), 2)                           AS total_revenue,
            ROUND(AVG(on_time_flag) * 100, 1)                AS on_time_rate_pct
        FROM   orders
        GROUP  BY order_year, order_quarter
        ORDER  BY order_year, order_quarter
    """)

    # ── Save all results as CSVs ─────────────────────────────────────────────
    results = {
        "sql_monthly_revenue":    monthly_revenue,
        "sql_category_kpi":       category_kpi,
        "sql_supplier_kpi":       supplier_kpi,
        "sql_warehouse_kpi":      warehouse_kpi,
        "sql_anomalies":          anomalies,
        "sql_mom_growth":         mom_growth,
        "sql_top_products":       top_products,
        "sql_inventory_risk":     inventory_risk,
        "sql_status_breakdown":   status_breakdown,
        "sql_quarterly":          quarterly,
    }
    for name, df in results.items():
        df.to_csv(os.path.join(OUTPUT_DIR, f"{name}.csv"), index=False)

    print(f"\n{'═'*60}")
    print("  ✅  All SQL results exported to /outputs/")
    print(f"{'═'*60}")
    con.close()


if __name__ == "__main__":
    main()

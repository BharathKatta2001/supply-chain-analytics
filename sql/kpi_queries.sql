-- ============================================================
--  SUPPLY CHAIN KPI ANALYTICS  |  SQL Query Library
--  Database : supply_chain.db  (SQLite / SQL Server compatible)
-- ============================================================

-- ── Q1: Monthly Revenue Trend ────────────────────────────────
SELECT
    order_yearmonth                              AS month,
    COUNT(order_id)                              AS total_orders,
    ROUND(SUM(revenue), 2)                       AS total_revenue,
    ROUND(AVG(revenue), 2)                       AS avg_order_value
FROM   orders
GROUP  BY order_yearmonth
ORDER  BY order_yearmonth;


-- ── Q2: Category KPI Summary ─────────────────────────────────
SELECT
    category,
    COUNT(order_id)                              AS total_orders,
    ROUND(SUM(revenue), 2)                       AS total_revenue,
    ROUND(AVG(discount) * 100, 1)               AS avg_discount_pct,
    ROUND(AVG(on_time_flag) * 100, 1)           AS on_time_rate_pct
FROM   orders
GROUP  BY category
ORDER  BY total_revenue DESC;


-- ── Q3: Supplier Performance ─────────────────────────────────
SELECT
    supplier,
    COUNT(order_id)                              AS total_orders,
    ROUND(SUM(revenue), 2)                       AS total_revenue,
    ROUND(AVG(delivery_variance), 2)             AS avg_delivery_variance_days,
    ROUND(AVG(is_late) * 100, 1)                AS late_delivery_rate_pct
FROM   orders
GROUP  BY supplier
ORDER  BY late_delivery_rate_pct DESC;


-- ── Q4: Warehouse Performance ────────────────────────────────
SELECT
    warehouse,
    COUNT(order_id)                              AS total_orders,
    ROUND(SUM(revenue), 2)                       AS total_revenue,
    ROUND(AVG(on_time_flag) * 100, 1)           AS on_time_rate_pct,
    ROUND(AVG(delivery_variance), 2)             AS avg_delivery_variance_days
FROM   orders
GROUP  BY warehouse
ORDER  BY total_revenue DESC;


-- ── Q5: Anomaly Detection (Delivery Variance > 7 Days) ───────
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
    ROUND(revenue, 2)                            AS revenue
FROM   orders
WHERE  delivery_variance > 7
ORDER  BY delivery_variance DESC;


-- ── Q6: Month-over-Month Revenue Growth (Window Function) ────
WITH monthly AS (
    SELECT
        order_yearmonth,
        ROUND(SUM(revenue), 2)                   AS monthly_revenue
    FROM   orders
    GROUP  BY order_yearmonth
),
with_lag AS (
    SELECT
        order_yearmonth,
        monthly_revenue,
        LAG(monthly_revenue) OVER (ORDER BY order_yearmonth) AS prev_month_revenue
    FROM   monthly
)
SELECT
    order_yearmonth                              AS month,
    monthly_revenue,
    prev_month_revenue,
    ROUND(
        (monthly_revenue - prev_month_revenue)
        / NULLIF(prev_month_revenue, 0) * 100, 2) AS mom_growth_pct
FROM   with_lag
ORDER  BY order_yearmonth;


-- ── Q7: Top 5 Products by Revenue ────────────────────────────
SELECT
    product_id,
    category,
    COUNT(order_id)                              AS total_orders,
    SUM(quantity)                                AS total_units_sold,
    ROUND(SUM(revenue), 2)                       AS total_revenue
FROM   orders
GROUP  BY product_id, category
ORDER  BY total_revenue DESC
LIMIT  5;


-- ── Q8: Inventory Risk – Below Reorder Level ─────────────────
SELECT
    i.product_id,
    i.category,
    i.stock_on_hand,
    i.reorder_level,
    i.reorder_level - i.stock_on_hand            AS shortfall,
    i.inventory_value,
    i.last_restocked
FROM   inventory i
WHERE  i.below_reorder = 1
ORDER  BY shortfall DESC;


-- ── Q9: Revenue by Order Status ──────────────────────────────
SELECT
    status,
    COUNT(order_id)                              AS order_count,
    ROUND(SUM(revenue), 2)                       AS total_revenue,
    ROUND(AVG(revenue), 2)                       AS avg_revenue
FROM   orders
GROUP  BY status
ORDER  BY total_revenue DESC;


-- ── Q10: Quarterly Revenue Summary ───────────────────────────
SELECT
    order_year,
    order_quarter,
    COUNT(order_id)                              AS total_orders,
    ROUND(SUM(revenue), 2)                       AS total_revenue,
    ROUND(AVG(on_time_flag) * 100, 1)           AS on_time_rate_pct
FROM   orders
GROUP  BY order_year, order_quarter
ORDER  BY order_year, order_quarter;

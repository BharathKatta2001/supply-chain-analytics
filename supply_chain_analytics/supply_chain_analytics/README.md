# Supply Chain KPI Analytics Pipeline
### Solo Project | Bharath Katta | Data Analyst

---

## Overview
An end-to-end data analytics pipeline that ingests raw supply chain data,
performs cleaning and transformation using Python and Pandas, loads the results
into a SQLite relational database, runs 10 analytical SQL queries to surface
KPIs and anomalies, and exports processed tables for Power BI dashboarding.

---

## Tech Stack
| Layer          | Tools                              |
|----------------|------------------------------------|
| Language       | Python 3.10+                       |
| Data Processing| Pandas, NumPy                      |
| Database       | SQLite (SQL Server compatible SQL) |
| Visualization  | Power BI                           |
| Version Control| Git / GitHub                       |

---

## Project Structure
```
supply_chain_analytics/
│
├── data/
│   ├── raw/                   # Source CSVs (generated)
│   │   ├── orders.csv
│   │   └── inventory.csv
│   └── processed/             # Cleaned & transformed CSVs
│       ├── orders_cleaned.csv
│       └── inventory_cleaned.csv
│
├── scripts/
│   ├── generate_data.py       # Synthetic data generation
│   ├── etl_pipeline.py        # ETL: Extract → Transform → Load
│   └── sql_analysis.py        # SQL KPI queries via Python
│
├── sql/
│   └── kpi_queries.sql        # Standalone SQL query library
│
├── outputs/
│   ├── supply_chain.db        # SQLite database
│   ├── kpi_monthly_revenue.csv
│   ├── kpi_category_performance.csv
│   ├── kpi_supplier_performance.csv
│   ├── kpi_warehouse_performance.csv
│   ├── kpi_order_status.csv
│   └── sql_*.csv              # SQL query result exports
│
└── README.md
```

---

## How to Run

### 1. Install dependencies
```bash
pip install pandas numpy
```

### 2. Generate raw data
```bash
python scripts/generate_data.py
```

### 3. Run the ETL pipeline
```bash
python scripts/etl_pipeline.py
```

### 4. Run SQL analysis
```bash
python scripts/sql_analysis.py
```

---

## ETL Pipeline Stages

### Stage 1 – Extract
- Reads `orders.csv` and `inventory.csv` from `data/raw/`
- Validates file existence and column structure

### Stage 2 – Transform
- **Data type casting**: dates, numerics
- **Null handling**: imputes missing emails; drops unfixable rows
- **Duplicate removal**: deduplicates on `order_id`
- **Outlier detection**: flags quantity outliers beyond 3 standard deviations
- **Feature engineering**:
  - `revenue` = quantity × unit_price × (1 − discount)
  - `delivery_variance` = actual − promised delivery days
  - `is_late` flag, `on_time_flag`
  - `order_year`, `order_month`, `order_quarter`, `order_yearmonth`

### Stage 3 – Load
- Writes cleaned data to SQLite database (`outputs/supply_chain.db`)
- Exports processed CSVs to `data/processed/`

### Stage 4 – Export KPI Tables
Exports 5 aggregated summary tables to `outputs/` for direct Power BI import:
- Monthly revenue trend
- Category performance
- Supplier performance
- Warehouse performance
- Order status breakdown

---

## SQL Queries (10 Analytical Queries)

| # | Query                            | Business Value                          |
|---|----------------------------------|-----------------------------------------|
| 1 | Monthly Revenue Trend            | Track revenue growth over time          |
| 2 | Category KPI Summary             | Compare performance across categories   |
| 3 | Supplier Performance             | Identify high late-delivery suppliers   |
| 4 | Warehouse Performance            | Rank warehouses by revenue & efficiency |
| 5 | Delivery Anomaly Detection       | Flag orders with variance > 7 days      |
| 6 | Month-over-Month Growth (Window) | Calculate MoM % revenue change          |
| 7 | Top 5 Products by Revenue        | Surface best-performing products        |
| 8 | Inventory Risk                   | Identify items below reorder level      |
| 9 | Revenue by Order Status          | Understand order fulfilment health      |
|10 | Quarterly Revenue Summary        | Quarterly performance tracking          |

---

## Power BI Dashboard

Connect Power BI to the following CSV files in `outputs/`:

| File                            | Dashboard Page              |
|---------------------------------|-----------------------------|
| `kpi_monthly_revenue.csv`       | Revenue Trend (Line Chart)  |
| `kpi_category_performance.csv`  | Category KPIs (Bar Chart)   |
| `kpi_supplier_performance.csv`  | Supplier Scorecard (Table)  |
| `kpi_warehouse_performance.csv` | Warehouse Map / Bar Chart   |
| `kpi_order_status.csv`          | Order Status (Donut Chart)  |
| `sql_anomalies.csv`             | Anomaly Alert Table         |
| `sql_mom_growth.csv`            | MoM Growth (Waterfall Chart)|

### Recommended KPI Cards
- **Total Revenue** | **Total Orders** | **Avg Order Value**
- **On-Time Delivery Rate %** | **Late Delivery Rate %**
- **Avg Delivery Variance (Days)**

---

## Key Insights (Sample Findings)
- Electronics drives the highest revenue but has above-average discount rates
- Delayed deliveries are concentrated in 1–2 specific suppliers
- Q4 shows consistent revenue uplift across all warehouses
- ~8% of orders exceed delivery promise by more than 7 days — flagged as anomalies

---

## Author
**Bharath Katta**  
MS Business Analytics | University of North Texas  
kattabharath.kb@gmail.com

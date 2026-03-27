import streamlit as st
import pandas as pd
import sqlite3
import os

# --- Configuration ---
# Set the target date for the simulation (as per the problem description)
CURRENT_DATE = "2018-05-01"
DB_PATH = ":memory:"  # Use in-memory database for speed and simplicity
CSV_ROOT_DIR = "../../"  # Path to CSV files relative to this script

# --- Setup Database Functions ---


@st.cache_resource
def init_connection():
    """Initializes the SQLite connection."""
    return sqlite3.connect(DB_PATH, check_same_thread=False)


def load_csv_data(conn):
    """Loads required CSV files into the SQLite database."""
    required_csvs = [
        "olist_orders_dataset.csv",
        "olist_order_items_dataset.csv",
        "olist_products_dataset.csv",
        "olist_customers_dataset.csv",
        "product_category_name_translation.csv",
    ]

    # Check if the tables already exist to avoid reloading (if not using :memory:)
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = [t[0] for t in cursor.fetchall()]

    for csv_file in required_csvs:
        table_name = os.path.splitext(csv_file)[0]
        if table_name in tables:
            continue

        file_path = os.path.join(CSV_ROOT_DIR, csv_file)
        if not os.path.exists(file_path):
            # Try looking in absolute path if relative fails (fallback)
            # Start from likely root
            file_path = os.path.join(
                "/Users/minhdang2803/Documents/learning/data-warehouse-decision-support-system",
                csv_file,
            )

        if os.path.exists(file_path):
            with st.spinner(f"Loading {csv_file}..."):
                df = pd.read_csv(file_path)
                # Clean column names similar to the load script logic
                df.columns = [
                    c.strip().replace(" ", "_").replace("-", "_").replace(".", "_")
                    for c in df.columns
                ]
                df.to_sql(table_name, conn, if_exists="replace", index=False)
        else:
            st.error(f"File not found: {csv_file}")


def run_etl_pipeline(conn):
    """Mô phỏng quy trình ETL để tạo Dim/Fact tables dựa trên problem_2.sql"""
    cursor = conn.cursor()

    # 1. Dim Date
    cursor.executescript(
        """
        DROP TABLE IF EXISTS dim_date;
        CREATE TABLE dim_date (
            date_key INT PRIMARY KEY,
            full_date DATE,
            year INT,
            quarter CHAR(2),
            month INT,
            month_name VARCHAR(10),
            day INT,
            week_no INT,
            yyyymm VARCHAR(6),
            mtd_flag BOOLEAN,
            day_of_week VARCHAR(20),
            is_weekend BOOLEAN
        );
        
        INSERT OR IGNORE INTO dim_date (
            date_key, full_date, year, quarter, month, month_name,
            day, week_no, yyyymm, mtd_flag,
            day_of_week, is_weekend
        )
        SELECT DISTINCT 
            CAST(strftime('%Y%m%d', order_purchase_timestamp) AS INT) as date_key,
            date(order_purchase_timestamp) as full_date,
            CAST(strftime('%Y', order_purchase_timestamp) AS INT) as year,
            'Q' || ((CAST(strftime('%m', order_purchase_timestamp) AS INT) + 2) / 3) as quarter,
            CAST(strftime('%m', order_purchase_timestamp) AS INT) as month,
            CASE strftime('%m', order_purchase_timestamp)   
                WHEN '01' THEN 'January' WHEN '02' THEN 'February' WHEN '03' THEN 'March'
                WHEN '04' THEN 'April'   WHEN '05' THEN 'May'      WHEN '06' THEN 'June'
                WHEN '07' THEN 'July'    WHEN '08' THEN 'August'   WHEN '09' THEN 'September'
                WHEN '10' THEN 'October' WHEN '11' THEN 'November' WHEN '12' THEN 'December'
            END as month_name,
            CAST(strftime('%d', order_purchase_timestamp) AS INT) as day,
            CAST(strftime('%W', order_purchase_timestamp) AS INT) as week_no,
            strftime('%Y%m', order_purchase_timestamp) as yyyymm,
            1 as mtd_flag,
            CASE strftime('%w', order_purchase_timestamp)
                WHEN '0' THEN 'Sunday' WHEN '1' THEN 'Monday' WHEN '2' THEN 'Tuesday'
                WHEN '3' THEN 'Wednesday' WHEN '4' THEN 'Thursday' WHEN '5' THEN 'Friday'
                WHEN '6' THEN 'Saturday'
            END as day_of_week,
            CASE WHEN strftime('%w', order_purchase_timestamp) IN ('0', '6') THEN 1 ELSE 0 END as is_weekend
        FROM olist_orders_dataset
        WHERE order_purchase_timestamp IS NOT NULL;
    """
    )

    # 2. Dim Customer
    cursor.executescript(
        """
        DROP TABLE IF EXISTS dim_customer;
        CREATE TABLE dim_customer (
            customer_key VARCHAR(50) PRIMARY KEY,
            customer_unique_id VARCHAR(50),      
            customer_zip_code_prefix VARCHAR(10),
            customer_city VARCHAR(100),
            customer_state CHAR(2)
        );
        INSERT OR IGNORE INTO dim_customer
        SELECT DISTINCT
            customer_id, customer_unique_id, customer_zip_code_prefix, customer_city, customer_state
        FROM olist_customers_dataset;
    """
    )

    # 3. Dim Product
    cursor.executescript(
        """
        DROP TABLE IF EXISTS dim_product;
        CREATE TABLE dim_product (
            product_key VARCHAR(50) PRIMARY KEY,
            category_name VARCHAR(100),
            category_name_english VARCHAR(100),
            weight_g INT,
            length_cm INT,
            height_cm INT,
            width_cm INT
        );
        INSERT OR IGNORE INTO dim_product
        SELECT DISTINCT 
            p.product_id, p.product_category_name, t.product_category_name_english,
            p.product_weight_g, p.product_length_cm, p.product_height_cm, p.product_width_cm
        FROM olist_products_dataset p
        LEFT JOIN product_category_name_translation t 
            ON p.product_category_name = t.product_category_name;
    """
    )

    # 4. Fact Sales
    cursor.executescript(
        """
        DROP TABLE IF EXISTS fact_sales;
        CREATE TABLE fact_sales (
            fact_id INTEGER PRIMARY KEY AUTOINCREMENT, 
            date_key INT,
            delivered_date_key INT,
            order_id VARCHAR(50),
            order_status VARCHAR(20),
            product_key VARCHAR(50),
            quantity INT,
            unit_price DECIMAL(10, 2),
            revenue DECIMAL(10, 2),
            customer_key VARCHAR(50)
        );
        INSERT INTO fact_sales (
            date_key, order_id, order_status, product_key, quantity, 
            delivered_date_key, unit_price, revenue, customer_key 
        )
        SELECT 
            CAST(strftime('%Y%m%d', t1.order_purchase_timestamp) AS INT) as date_key,
            t1.order_id, t1.order_status, t2.product_id, 1 as quantity,
            CASE 
                WHEN t1.order_delivered_customer_date IS NOT NULL 
                THEN CAST(strftime('%Y%m%d', t1.order_delivered_customer_date) AS INT)
                ELSE NULL 
            END AS delivered_date_key,
            t2.price,
            (t2.price + t2.freight_value) * 1 as revenue,
            t1.customer_id
        FROM olist_orders_dataset t1
        JOIN olist_order_items_dataset t2 ON t1.order_id = t2.order_id
        WHERE t1.order_purchase_timestamp IS NOT NULL;
    """
    )
    conn.commit()


# --- Main App ---

st.set_page_config(layout="wide", page_title="Daily Operational Monitor")

conn = init_connection()
load_csv_data(conn)
# Run ETL only if tables don't exist in our memory DB (which is always true on restart)
# In a real app we might check if 'fact_sales' exists.
run_etl_pipeline(conn)

st.title("DAILY OPERATIONAL MONITOR")
st.markdown(f"**Data Date:** {CURRENT_DATE} | **Scope:** Last 7 days")

# --- Queries (Converted to SQLite) ---

# Query 1: Daily Revenue & Growth (KPIs & Chart)
df_daily = pd.read_sql(
    f"""
    SELECT 
        d.full_date,
        SUM(f.revenue) AS daily_revenue,
        COUNT(DISTINCT f.order_id) AS total_orders,
        LAG(SUM(f.revenue), 1) OVER (ORDER BY d.full_date) AS previous_day_revenue
    FROM fact_sales f
    JOIN dim_date d ON f.date_key = d.date_key
    WHERE d.full_date BETWEEN date('{CURRENT_DATE}', '-7 days') AND '{CURRENT_DATE}'
    GROUP BY d.full_date
    ORDER BY d.full_date ASC;
""",
    conn,
)

# Calculate Growth for the most recent day in the range (Yesterday relative to Current Date?)
# The prompt says 'Yesterday' vs 'Day before'.
# Assuming the last row in df_daily is "Yesterday" (the latest date in the 7 day window)
if not df_daily.empty:
    latest_metrics = df_daily.iloc[-1]
    prev_metrics = df_daily.iloc[-2] if len(df_daily) > 1 else None

    revenue_yesterday = latest_metrics["daily_revenue"]
    orders_yesterday = latest_metrics["total_orders"]

    if prev_metrics is not None:
        revenue_prev = prev_metrics["daily_revenue"]
        growth_pct = (
            ((revenue_yesterday - revenue_prev) / revenue_prev * 100)
            if revenue_prev != 0
            else 0
        )
    else:
        growth_pct = 0
else:
    revenue_yesterday = 0
    orders_yesterday = 0
    growth_pct = 0

# Query 2: Bottleneck Orders (Action Required KPI & Table)
df_bottleneck = pd.read_sql(
    f"""
    SELECT 
        f.order_id,
        d.full_date AS order_date,
        f.order_status,
        CAST(julianday('{CURRENT_DATE}') - julianday(d.full_date) AS INT) AS days_pending
    FROM fact_sales f
    JOIN dim_date d ON f.date_key = d.date_key
    WHERE f.order_status IN ('processing', 'invoiced', 'shipped')
      AND CAST(julianday('{CURRENT_DATE}') - julianday(d.full_date) AS INT) > 10
    ORDER BY days_pending DESC
    LIMIT 20;
""",
    conn,
)

action_required_count = len(df_bottleneck)

# Query 3: Top Products (Chart)
df_products = pd.read_sql(
    f"""
    SELECT 
        p.category_name,
        SUM(f.revenue) AS revenue_this_week
    FROM fact_sales f
    JOIN dim_date d ON f.date_key = d.date_key
    JOIN dim_product p ON f.product_key = p.product_key
    WHERE d.full_date BETWEEN date('{CURRENT_DATE}', '-7 days') AND '{CURRENT_DATE}'
    GROUP BY p.category_name
    ORDER BY revenue_this_week DESC
    LIMIT 10;
""",
    conn,
)

# Query 4: Order Status Distribution (Chart)
df_status = pd.read_sql(
    f"""
    SELECT 
        order_status,
        COUNT(DISTINCT order_id) as count
    FROM fact_sales f
    JOIN dim_date d ON f.date_key = d.date_key
    WHERE d.full_date BETWEEN date('{CURRENT_DATE}', '-7 days') AND '{CURRENT_DATE}'
    GROUP BY order_status
""",
    conn,
)


# --- Dashboard Layout ---

# 1. Headline KPIs
col1, col2, col3 = st.columns(3)

with col1:
    st.metric(
        label="Yesterday Revenue",
        value=f"${revenue_yesterday:,.0f}",
        delta=f"{growth_pct:.1f}%",
    )

with col2:
    st.metric(label="New Orders", value=f"{orders_yesterday:,}")

with col3:
    st.metric(
        label="Action Required (Delayed > 10 days)",
        value=f"{action_required_count}",
        delta_color="inverse",  # Red if high is bad? Default is green for positive delta. We want to highlight the number.
    )

st.divider()

# 2. Charts
col_chart1, col_chart2 = st.columns(2)

with col_chart1:
    st.subheader("Last 7 Days Revenue")
    if not df_daily.empty:
        st.bar_chart(df_daily.set_index("full_date")["daily_revenue"])
    else:
        st.write("No data available.")

with col_chart2:
    st.subheader("Order Status Distribution")
    if not df_status.empty:
        # Streamlit doesn't have a native pie chart, use plotly or altair if available, or just a bar for now or st.dataframe
        # Using a simple bar chart for status for simplicity without extra deps,
        # but let's try to make it look like a distribution.
        st.bar_chart(df_status.set_index("order_status"))
    else:
        st.write("No data available.")

st.divider()

# 3. Detail Table
st.subheader("⚠️ Delayed Orders (Bottleneck Alert)")
st.dataframe(
    df_bottleneck,
    column_config={
        "order_id": "Order ID",
        "order_date": "Date",
        "order_status": st.column_config.TextColumn("Status"),
        "days_pending": st.column_config.NumberColumn("Days Pending", format="%d days"),
    },
    use_container_width=True,
    hide_index=True,
)

st.markdown("---")
st.caption("Dashboard generated on simulated date: 2018-05-01 using Olist Dataset.")

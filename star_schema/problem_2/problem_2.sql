-- 1. Tạo bảng Dimension: dim_date
DROP TABLE IF EXISTS dim_date;
CREATE TABLE dim_date (
    date_key INT PRIMARY KEY,       -- Ví dụ: 20170801
    full_date DATE,                 -- Ví dụ: 2017-08-01
    year INT,                       -- Ví dụ: 2017
    quarter CHAR(2),                -- Ví dụ: Q3
    month INT,                      -- Ví dụ: 8
    month_name VARCHAR(10),         -- Ví dụ: August
    day INT,                        -- Ngày trong tháng (DD)
    week_no INT,                    -- Tuần thứ mấy trong năm (0-53)
    yyyymm VARCHAR(6),              -- Mã năm tháng (YYYYMM)
    mtd_flag BOOLEAN                -- Cờ MTD
);

-- 2. Tạo bảng Fact: fact_sales
DROP TABLE IF EXISTS fact_sales;
CREATE TABLE fact_sales (
    fact_id INTEGER PRIMARY KEY AUTOINCREMENT, 
    date_key INT,                          -- Ngày mua (FK)
    delivered_date_key INT,                -- Ngày giao hàng (FK)
    order_id VARCHAR(50),                  -- Mã đơn hàng
    product_id VARCHAR(50),                -- Mã sản phẩm
    quantity INT,                          -- Số lượng
    unit_price DECIMAL(10, 2),             -- Đơn giá
    revenue DECIMAL(10, 2),                -- Doanh thu (Price + Freight) * Qty

    FOREIGN KEY (date_key) REFERENCES dim_date(date_key)
);

-- 3. Đổ dữ liệu vào bảng dim_date 
INSERT OR IGNORE INTO dim_date (
    date_key, full_date, year, quarter, month, month_name,
    day, week_no, yyyymm, mtd_flag
)
SELECT DISTINCT 
    CAST(strftime('%Y%m%d', order_purchase_timestamp) AS INT) as date_key, -- date_key: 20170801
    date(order_purchase_timestamp) as full_date, -- full_date: 2017-08-01
    CAST(strftime('%Y', order_purchase_timestamp) AS INT) as year, -- year: 2017
    'Q' || ((CAST(strftime('%m', order_purchase_timestamp) AS INT) + 2) / 3) as quarter, -- quarter: Q3
    CAST(strftime('%m', order_purchase_timestamp) AS INT) as month, -- month: 8
    CASE strftime('%m', order_purchase_timestamp)   
        WHEN '01' THEN 'January' WHEN '02' THEN 'February' WHEN '03' THEN 'March'
        WHEN '04' THEN 'April'   WHEN '05' THEN 'May'      WHEN '06' THEN 'June'
        WHEN '07' THEN 'July'    WHEN '08' THEN 'August'   WHEN '09' THEN 'September'
        WHEN '10' THEN 'October' WHEN '11' THEN 'November' WHEN '12' THEN 'December'
    END as month_name, -- month_name: August
    CAST(strftime('%d', order_purchase_timestamp) AS INT) as day,   -- day: 1
    CAST(strftime('%W', order_purchase_timestamp) AS INT) as week_no, -- week_no: 31
    strftime('%Y%m', order_purchase_timestamp) as yyyymm,   -- yyyymm: 201708
    1 as mtd_flag -- mtd_flag: 1
FROM olist_orders_dataset
WHERE order_purchase_timestamp IS NOT NULL;

-- 4. Đổ dữ liệu vào bảng fact_sales
INSERT INTO fact_sales (
    date_key, order_id, product_id, quantity, 
    delivered_date_key, unit_price, revenue
)
SELECT 
    CAST(strftime('%Y%m%d', t1.order_purchase_timestamp) AS INT) as date_key,
    t1.order_id,
    t2.product_id,
    1 as quantity,
    
    -- delivered_date_key
    CASE 
        WHEN t1.order_delivered_customer_date IS NOT NULL 
        THEN CAST(strftime('%Y%m%d', t1.order_delivered_customer_date) AS INT)
        ELSE NULL 
    END AS delivered_date_key,
    
    -- unit_price
    t2.price,
    
    -- revenue = (price + freight) * qty
    (t2.price + t2.freight_value) * 1 as revenue

FROM olist_orders_dataset t1
JOIN olist_order_items_dataset t2 ON t1.order_id = t2.order_id
WHERE t1.order_status = 'delivered' AND t1.order_purchase_timestamp IS NOT NULL;

-- 5. Truy vấn báo cáo (Cập nhật tên cột snake_case)
SELECT 
    d.year,
    d.quarter,
    SUM(f.unit_price) AS total_sales_amount,
    SUM(f.revenue) AS total_revenue,
    COUNT(DISTINCT f.order_id) AS total_orders
FROM fact_sales f
JOIN dim_date d ON f.date_key = d.date_key
GROUP BY d.year, d.quarter
ORDER BY d.year, d.quarter;

-- 6. Truy vấn YoY Growth
WITH monthly_sales AS (
    SELECT 
        d.year,
        d.month,
        d.month_name,
        d.quarter,
        SUM(f.revenue) AS revenue
    FROM fact_sales f
    JOIN dim_date d ON f.date_key = d.date_key
    GROUP BY d.year, d.month, d.month_name, d.quarter
)
SELECT 
    current_year.year,
    current_year.month,
    current_year.quarter,
    current_year.revenue AS revenue_this_year,
    last_year.revenue AS revenue_last_year,
    ROUND(
        (current_year.revenue - last_year.revenue) / NULLIF(last_year.revenue, 0) * 100
    , 2) || '%' AS yoy_growth_percent
FROM monthly_sales current_year
LEFT JOIN monthly_sales last_year 
    ON current_year.year = last_year.year + 1 
    AND current_year.month = last_year.month
ORDER BY current_year.year, current_year.month;

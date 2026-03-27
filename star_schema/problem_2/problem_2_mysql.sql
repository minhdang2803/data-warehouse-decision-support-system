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
    week_no INT,                    -- Tuần thứ mấy trong năm
    yyyymm VARCHAR(6),              -- Mã năm tháng (YYYYMM)
    mtd_flag BOOLEAN,               -- Cờ MTD
    day_of_week VARCHAR(20),        -- Thứ trong tuần (Monday, Tuesday...)
    is_weekend BOOLEAN              -- Cờ báo cuối tuần (0 - trong tuần/1 - cuối tuần)
);

-- 2. Tạo bảng Dimension: dim_customer
DROP TABLE IF EXISTS dim_customer;
CREATE TABLE dim_customer (
    customer_key VARCHAR(50) PRIMARY KEY, -- Maps to customer_id
    customer_unique_id VARCHAR(50),      
    customer_zip_code_prefix VARCHAR(10),
    customer_city VARCHAR(100),
    customer_state CHAR(2)
);

-- 3. Tạo bảng Dimension: dim_product
DROP TABLE IF EXISTS dim_product;
CREATE TABLE dim_product (
    product_key VARCHAR(50) PRIMARY KEY, -- Maps to product_id
    category_name VARCHAR(100),
    category_name_english VARCHAR(100),
    weight_g INT,
    length_cm INT,
    height_cm INT,
    width_cm INT
);

-- 4. Tạo bảng Fact: fact_sales
DROP TABLE IF EXISTS fact_sales;
CREATE TABLE fact_sales (
    fact_id INT PRIMARY KEY AUTO_INCREMENT, 
    date_key INT,                          -- Ngày mua (FK)
    delivered_date_key INT,                -- Ngày giao hàng 
    order_id VARCHAR(50),                  -- Mã đơn hàng
    order_status VARCHAR(20),              -- Trạng thái
    product_key VARCHAR(50),               -- Mã sản phẩm
    quantity INT,                          -- Số lượng
    unit_price DECIMAL(10, 2),             -- Đơn giá
    revenue DECIMAL(10, 2),                -- Doanh thu (Price + Freight) * Qty
    customer_key VARCHAR(50),              -- Khách hàng (FK)

    FOREIGN KEY (date_key) REFERENCES dim_date(date_key),
    FOREIGN KEY (product_key) REFERENCES dim_product(product_key),
    FOREIGN KEY (customer_key) REFERENCES dim_customer(customer_key)
);

-- 5. Đổ dữ liệu vào bảng dim_date
-- MySQL sử dụng DATE_FORMAT, YEAR, MONTH, DAY, QUARTER...
INSERT IGNORE INTO dim_date (
    date_key, full_date, year, quarter, month, month_name,
    day, week_no, yyyymm, mtd_flag,
    day_of_week, is_weekend
)
SELECT DISTINCT 
    CAST(DATE_FORMAT(order_purchase_timestamp, '%Y%m%d') AS UNSIGNED) as date_key,
    DATE(order_purchase_timestamp) as full_date,
    YEAR(order_purchase_timestamp) as year,
    CONCAT('Q', QUARTER(order_purchase_timestamp)) as quarter,
    MONTH(order_purchase_timestamp) as month,
    DATE_FORMAT(order_purchase_timestamp, '%M') as month_name,
    DAY(order_purchase_timestamp) as day,
    WEEK(order_purchase_timestamp, 1) as week_no,
    DATE_FORMAT(order_purchase_timestamp, '%Y%m') as yyyymm,
    1 as mtd_flag,
    DAYNAME(order_purchase_timestamp) as day_of_week,
    CASE 
        WHEN DAYOFWEEK(order_purchase_timestamp) IN (1, 7) THEN 1 
        ELSE 0 
    END as is_weekend
FROM olist_orders_dataset
WHERE order_purchase_timestamp IS NOT NULL;

-- 6. Đổ dữ liệu vào bảng dim_customer
INSERT IGNORE INTO dim_customer (
    customer_key, customer_unique_id, 
    customer_zip_code_prefix, customer_city, customer_state
)
SELECT DISTINCT
    customer_id,
    customer_unique_id,
    customer_zip_code_prefix,
    customer_city,
    customer_state
FROM olist_customers_dataset;

-- 7. Đổ dữ liệu vào bảng dim_product
INSERT IGNORE INTO dim_product (
    product_key, category_name, category_name_english,
    weight_g, length_cm, height_cm, width_cm
)
SELECT DISTINCT 
    p.product_id,
    p.product_category_name,
    t.product_category_name_english,
    p.product_weight_g,
    p.product_length_cm,
    p.product_height_cm,
    p.product_width_cm
FROM olist_products_dataset p
LEFT JOIN product_category_name_translation t 
    ON p.product_category_name = t.product_category_name;


-- 8. Đổ dữ liệu vào bảng fact_sales (Lưu ý: product_id tr mapped vào product_key)
INSERT INTO fact_sales (
    date_key, order_id, order_status, product_key, quantity, 
    delivered_date_key, unit_price, revenue,
    customer_key 
)
SELECT 
    CAST(DATE_FORMAT(t1.order_purchase_timestamp, '%Y%m%d') AS UNSIGNED) as date_key,
    t1.order_id,
    t1.order_status,
    t2.product_id, -- product_key
    1 as quantity,
    
    -- delivered_date_key
    CASE 
        WHEN t1.order_delivered_customer_date IS NOT NULL 
        THEN CAST(DATE_FORMAT(t1.order_delivered_customer_date, '%Y%m%d') AS UNSIGNED)
        ELSE NULL 
    END AS delivered_date_key,
    
    -- unit_price
    t2.price,
    
    -- revenue = (price + freight) * qty
    (t2.price + t2.freight_value) * 1 as revenue,

    -- customer_key
    t1.customer_id

FROM olist_orders_dataset t1
JOIN olist_order_items_dataset t2 ON t1.order_id = t2.order_id
WHERE t1.order_purchase_timestamp IS NOT NULL;

-- 9. Truy vấn báo cáo
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

-- 10. Truy vấn YoY Growth
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
    CONCAT(ROUND(
        (current_year.revenue - last_year.revenue) / NULLIF(last_year.revenue, 0) * 100
    , 2), '%') AS yoy_growth_percent
FROM monthly_sales current_year
LEFT JOIN monthly_sales last_year 
    ON current_year.year = last_year.year + 1 
    AND current_year.month = last_year.month
ORDER BY current_year.year, current_year.month;

-- 11. Roll-up & Drill-down
-- Drill-down Level 1: Xem chi tiết từng tháng của năm 2017
SELECT 
    d.month,
    d.month_name,
    SUM(f.revenue) AS monthly_revenue,
    COUNT(f.order_id) AS total_orders
FROM fact_sales f
JOIN dim_date d ON f.date_key = d.date_key
WHERE d.year = 2017
GROUP BY d.month, d.month_name
ORDER BY d.month;

-- Drill-down Level 2: Xem chi tiết từng ngày của Tháng 9/2017
SELECT 
    d.full_date,
    d.day,
    SUM(f.revenue) AS daily_revenue
FROM fact_sales f
JOIN dim_date d ON f.date_key = d.date_key
WHERE d.year = 2017 AND d.month = 9
GROUP BY d.full_date, d.day
ORDER BY d.full_date;

-- 12. Truy vấn theo thứ trong tuần
SELECT 
    d.day_of_week,
    d.is_weekend,      
    COUNT(DISTINCT f.order_id) AS total_orders,
    SUM(f.revenue) AS total_revenue,
    ROUND(SUM(f.revenue) / NULLIF(COUNT(DISTINCT f.order_id), 0), 2) AS avg_order_value
FROM fact_sales f
JOIN dim_date d ON f.date_key = d.date_key
GROUP BY d.day_of_week, d.is_weekend
ORDER BY total_revenue DESC; 

-- 13. Kịch bản 1: Daily Operational Monitoring (Growth Chart)
SELECT 
    d.full_date,
    SUM(f.revenue) AS daily_revenue,
    COUNT(DISTINCT f.order_id) AS total_orders,
    LAG(SUM(f.revenue), 1) OVER (ORDER BY d.full_date) AS previous_day_revenue,
    ROUND(
        (SUM(f.revenue) - LAG(SUM(f.revenue), 1) OVER (ORDER BY d.full_date)) * 100.0 / 
        NULLIF(LAG(SUM(f.revenue), 1) OVER (ORDER BY d.full_date), 0), 2
    ) AS daily_growth_percent
FROM fact_sales f
JOIN dim_date d ON f.date_key = d.date_key
-- MySQL DATE_SUB
WHERE d.full_date BETWEEN DATE_SUB('2018-05-01', INTERVAL 7 DAY) AND '2018-05-01'
GROUP BY d.full_date
ORDER BY d.full_date DESC;

-- 14. Kịch bản 2: Cảnh báo "Điểm nghẽn" Vận hành (Bottleneck Alert)
SELECT 
    f.order_id,
    d.full_date AS order_date,
    f.order_status,
    DATEDIFF('2018-05-01', d.full_date) AS days_pending
FROM fact_sales f
JOIN dim_date d ON f.date_key = d.date_key
WHERE f.order_status IN ('processing', 'invoiced', 'shipped') 
  AND DATEDIFF('2018-05-01', d.full_date) > 10 
ORDER BY days_pending DESC
LIMIT 20;

-- 15. Kịch bản 3: Top Sản phẩm "Hot Trend" tuần này
SELECT 
    p.category_name,
    SUM(f.quantity) AS units_sold_this_week,
    SUM(f.revenue) AS revenue_this_week
FROM fact_sales f
JOIN dim_date d ON f.date_key = d.date_key
JOIN dim_product p ON f.product_key = p.product_key
WHERE d.full_date BETWEEN DATE_SUB('2018-05-01', INTERVAL 7 DAY) AND '2018-05-01'
GROUP BY p.category_name
ORDER BY units_sold_this_week DESC
LIMIT 10;

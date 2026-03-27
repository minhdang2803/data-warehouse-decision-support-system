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
    fact_id INTEGER PRIMARY KEY AUTOINCREMENT, 
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

-- 4. Đổ dữ liệu vào bảng dim_date
INSERT OR IGNORE INTO dim_date (
    date_key, full_date, year, quarter, month, month_name,
    day, week_no, yyyymm, mtd_flag,
    day_of_week, is_weekend
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
    1 as mtd_flag, -- mtd_flag: 1
    
    -- day_of_week
    CASE strftime('%w', order_purchase_timestamp)
        WHEN '0' THEN 'Sunday'
        WHEN '1' THEN 'Monday'
        WHEN '2' THEN 'Tuesday'
        WHEN '3' THEN 'Wednesday'
        WHEN '4' THEN 'Thursday'
        WHEN '5' THEN 'Friday'
        WHEN '6' THEN 'Saturday'
    END as day_of_week,

    -- is_weekend (0=Sunday, 6=Saturday)
    CASE 
        WHEN strftime('%w', order_purchase_timestamp) IN ('0', '6') THEN 1 
        ELSE 0 
    END as is_weekend

FROM olist_orders_dataset
WHERE order_purchase_timestamp IS NOT NULL;

-- 5. Đổ dữ liệu vào bảng dim_customer
INSERT OR IGNORE INTO dim_customer (
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

-- 6. Đổ dữ liệu vào bảng dim_product
INSERT OR IGNORE INTO dim_product (
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


-- 7. Đổ dữ liệu vào bảng fact_sales (Lưu ý: product_id tr mapped vào product_key)
INSERT INTO fact_sales (
    date_key, order_id, order_status, product_key, quantity, 
    delivered_date_key, unit_price, revenue,
    customer_key 
)
SELECT 
    CAST(strftime('%Y%m%d', t1.order_purchase_timestamp) AS INT) as date_key,
    t1.order_id,
    t1.order_status,
    t2.product_id, -- product_key
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
    (t2.price + t2.freight_value) * 1 as revenue,

    -- customer_key
    t1.customer_id

FROM olist_orders_dataset t1
JOIN olist_order_items_dataset t2 ON t1.order_id = t2.order_id
WHERE t1.order_purchase_timestamp IS NOT NULL;

-- 8. Truy vấn báo cáo (Cập nhật tên cột snake_case)
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

-- 9. Truy vấn YoY Growth
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

-- 10. Drill-down & Roll-up

-- Drill-down Level 1: Xem chi tiết từng tháng của năm 2017
SELECT 
    d.month,
    d.month_name,
    SUM(f.revenue) AS monthly_revenue,
    COUNT(f.order_id) AS total_orders
FROM fact_sales f
JOIN dim_date d ON f.date_key = d.date_key
WHERE d.year = 2017 -- ĐK lọc: Chỉ xem năm 2017
GROUP BY d.month, d.month_name
ORDER BY d.month;

-- Drill-down Level 2: Xem chi tiết từng ngày của Tháng 9/2017
SELECT 
    d.full_date,
    d.day,
    SUM(f.revenue) AS daily_revenue
FROM fact_sales f
JOIN dim_date d ON f.date_key = d.date_key
WHERE d.year = 2017 AND d.month = 9 -- ĐK lọc: Khoanh vùng Tháng 9 năm 2017
GROUP BY d.full_date, d.day
ORDER BY d.full_date;
-- Drill-down Level 3: Xem chi tiết trong một ngày cụ thể
SELECT 
    f.order_id,
    f.product_key,
    f.revenue,
    f.quantity,
    -- Giả sử join thêm bảng Dim_Customer để biết ai mua
    c.customer_city
FROM fact_sales f
JOIN dim_date d ON f.date_key = d.date_key
-- Join thêm bảng Customer nếu cần
LEFT JOIN dim_customer c ON f.customer_key = c.customer_key 
WHERE d.full_date = '2017-01-09';

-- Roll up
SELECT 
    d.year,
    d.quarter,
    SUM(f.revenue) AS total_revenue,
    COUNT(DISTINCT f.order_id) AS total_orders
FROM fact_sales f
JOIN dim_date d ON f.date_key = d.date_key
GROUP BY d.year, d.quarter
ORDER BY d.year, d.quarter;

SELECT 
    d.day_of_week,      -- Thứ trong tuần (Monday, Tuesday...)
    -- Cờ báo cuối tuần (0 - trong tuần/1 - cuối tuần)
    d.is_weekend,      
    COUNT(DISTINCT f.order_id) AS total_orders,
    SUM(f.revenue) AS total_revenue,
    -- Tính giá trị trung bình mỗi đơn hàng (AOV) theo thứ
    ROUND(SUM(f.revenue) / COUNT(DISTINCT f.order_id), 2) AS avg_order_value
FROM fact_sales f
JOIN dim_date d ON f.date_key = d.date_key
GROUP BY d.day_of_week, d.is_weekend
-- Sắp xếp để thấy thứ nào doanh thu cao nhất lên đầu
ORDER BY total_revenue DESC; 


SELECT 
    d.full_date,
    -- Doanh thu ngày
    SUM(f.revenue) AS daily_revenue,
    -- Số lượng đơn hàng
    COUNT(DISTINCT f.order_id) AS total_orders,
    -- So sánh với ngày hôm trước (Daily Growth)
    LAG(SUM(f.revenue), 1) OVER (ORDER BY d.full_date) AS previous_day_revenue,
    -- Tính % tăng trưởng ngày
    ROUND(
        (SUM(f.revenue) - LAG(SUM(f.revenue), 1) OVER (ORDER BY d.full_date)) * 100.0 / 
        NULLIF(LAG(SUM(f.revenue), 1) OVER (ORDER BY d.full_date), 0), 2
    ) AS daily_growth_percent
FROM fact_sales f
JOIN dim_date d ON f.date_key = d.date_key
-- Lọc dữ liệu: Chỉ lấy 7 ngày gần nhất tính từ "Hôm nay"
WHERE d.full_date BETWEEN date('2018-05-01', '-7 days') AND '2018-05-01'
GROUP BY d.full_date
ORDER BY d.full_date DESC;


SELECT 
    f.order_id,
    d.full_date AS order_date,
    f.order_status,
    -- Tính số ngày đã trôi qua kể từ khi đặt (Age of Order)
    CAST(julianday('2018-05-01') - julianday(d.full_date) AS INT) AS days_pending
FROM fact_sales f
JOIN dim_date d ON f.date_key = d.date_key
WHERE f.order_status IN ('processing', 'invoiced', 'shipped') -- Các trạng thái chưa hoàn thành
  AND CAST(julianday('2018-05-01') - julianday(d.full_date) AS INT) > 10 -- Cảnh báo: Đã qua 10 ngày chưa xong
ORDER BY days_pending DESC
LIMIT 20;


SELECT 
    p.category_name,
    SUM(f.quantity) AS units_sold_this_week,
    SUM(f.revenue) AS revenue_this_week
FROM fact_sales f
JOIN dim_date d ON f.date_key = d.date_key
JOIN dim_product p ON f.product_key = p.product_key
WHERE d.full_date BETWEEN date('2018-05-01', '-7 days') AND '2018-05-01'
GROUP BY p.category_name
ORDER BY units_sold_this_week DESC
LIMIT 10;


SELECT 
    p.category_name,
    SUM(f.quantity) AS units_sold_this_week,
    SUM(f.revenue) AS revenue_this_week
FROM fact_sales f
JOIN dim_date d ON f.date_key = d.date_key
JOIN dim_product p ON f.product_key = p.product_key
WHERE d.full_date BETWEEN date('2018-05-01', '-7 days') AND '2018-05-01'
GROUP BY p.category_name
ORDER BY units_sold_this_week DESC
LIMIT 10;


SELECT 
    d.Full_Date,
    -- Doanh thu ngày
    SUM(f.Sales_Amount) AS Daily_Revenue,
    -- Số lượng đơn hàng
    COUNT(DISTINCT f.Order_ID) AS Total_Orders,
    -- So sánh với ngày hôm trước (Daily Growth)
    LAG(SUM(f.Sales_Amount), 1) OVER (ORDER BY d.Full_Date) AS Previous_Day_Revenue,
    -- Tính % tăng trưởng ngày
    ROUND(
        (SUM(f.Sales_Amount) - LAG(SUM(f.Sales_Amount), 1) OVER (ORDER BY d.Full_Date)) * 100.0 / 
        NULLIF(LAG(SUM(f.Sales_Amount), 1) OVER (ORDER BY d.Full_Date), 0), 2
    ) AS Daily_Growth_Percent
FROM Fact_Sales f
JOIN Dim_Date d ON f.Date_Key = d.Date_Key
-- Lọc dữ liệu: Chỉ lấy 7 ngày gần nhất tính từ "Hôm nay"
WHERE d.Full_Date BETWEEN DATEADD(day, -7, '2018-05-01') AND '2018-05-01'
GROUP BY d.Full_Date
ORDER BY d.Full_Date DESC;

    Giá trị mang lại: Giúp quản lý thấy ngay biểu đồ đường đi xuống bất thường để tìm nguyên nhân.

-- Kịch bản 2: Cảnh báo "Điểm nghẽn" Vận hành (Bottleneck Alert)

-- Mục đích: Lọc ra danh sách các đơn hàng đang bị "ngâm" quá lâu chưa giao xong.
-- SQL

SELECT TOP 20
    f.Order_ID,
    d.Full_Date AS Order_Date,
    f.Order_Status,
    -- Tính số ngày đã trôi qua kể từ khi đặt (Age of Order)
    DATEDIFF(day, d.Full_Date, '2018-05-01') AS Days_Pending
FROM Fact_Sales f
JOIN Dim_Date d ON f.Date_Key = d.Date_Key
WHERE f.Order_Status IN ('processing', 'invoiced', 'shipped') -- Các trạng thái chưa hoàn thành
  AND DATEDIFF(day, d.Full_Date, '2018-05-01') > 10           -- Cảnh báo: Đã qua 10 ngày chưa xong
ORDER BY Days_Pending DESC;

    Hành động: Quản lý xuất danh sách này gửi cho bộ phận Logistics/CSKH để xử lý gấp.

-- Kịch bản 3: Top Sản phẩm "Hot Trend" tuần này

-- Mục đích: Nhập hàng gấp cho các món đang bán chạy đột biến trong tuần.
-- SQL

SELECT TOP 10
    p.Category_Name,
    SUM(f.Quantity) AS Units_Sold_This_Week,
    SUM(f.Sales_Amount) AS Revenue_This_Week
FROM Fact_Sales f
JOIN Dim_Date d ON f.Date_Key = d.Date_Key
JOIN Dim_Product p ON f.Product_Key = p.Product_Key
WHERE d.Full_Date BETWEEN DATEADD(day, -7, '2018-05-01') AND '2018-05-01'
GROUP BY p.Category_Name
ORDER BY Units_Sold_This_Week DESC;

-- 4. Thiết kế Dashboard & Trực quan hóa (Visualization)

-- Đối với yêu cầu số 8, bạn hãy mô tả một Dashboard trên Power BI với bố cục như sau:

-- Tiêu đề: DAILY OPERATIONAL MONITOR (GIÁM SÁT VẬN HÀNH HÀNG NGÀY)

--     Hàng trên cùng (KPI Cards - Các chỉ số nhanh):

--         Doanh thu hôm qua: [Giá trị] (Kèm mũi tên Xanh/Đỏ so với hôm kia).

--         Số đơn hàng mới: [Giá trị].

--         Đơn hàng cần xử lý gấp: [Số lượng] (Là kết quả của Kịch bản 2).

--     Khu vực giữa (Charts - Xu hướng ngắn):

--         Biểu đồ cột (Bar Chart): Doanh thu 7 ngày gần nhất. Giúp trả lời: Tuần này phong độ có ổn định không?

--         Biểu đồ tròn (Donut Chart): Tỷ lệ trạng thái đơn hàng hiện tại (Bao nhiêu % đã giao, bao nhiêu % đang giao, bao nhiêu % bị hủy).

--     Hàng dưới cùng (Detail Table - Chi tiết hành động):

--         Bảng danh sách "Các đơn hàng chậm trễ" (Drill-through details). Cho phép quản lý click vào để xem mã đơn, mã khách hàng để liên hệ.
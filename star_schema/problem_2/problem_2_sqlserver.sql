-- 1. Tạo bảng Dimension: Dim_Date
IF OBJECT_ID('Dim_Date', 'U') IS NOT NULL DROP TABLE Dim_Date;
CREATE TABLE Dim_Date (
    Date_Key INT PRIMARY KEY,       -- Ví dụ: 20170801
    Full_Date DATE,                 -- Ví dụ: 2017-08-01
    Year INT,                       -- Ví dụ: 2017
    Quarter CHAR(2),                -- Ví dụ: Q3
    Month INT,                      -- Ví dụ: 8
    Month_Name VARCHAR(10)          -- Ví dụ: August
);

-- 2. Tạo bảng Fact: Fact_Sales
IF OBJECT_ID('Fact_Sales', 'U') IS NOT NULL DROP TABLE Fact_Sales;
CREATE TABLE Fact_Sales (
    Fact_ID INT IDENTITY(1,1) PRIMARY KEY, -- Tự động tăng (SQL Server dùng IDENTITY)
    Date_Key INT,                          -- Khóa ngoại
    Order_ID VARCHAR(50),
    Product_ID VARCHAR(50),
    Sales_Amount DECIMAL(10, 2),           -- Tiền (Price)
    Quantity INT,
    
    -- CÁC CỘT BỔ SUNG THEO YÊU CẦU
    Delivered_Date_Key INT,                -- Ngày giao hàng (FK)
    Unit_Price DECIMAL(10, 2),             -- Đơn giá
    Revenue DECIMAL(10, 2),                -- Doanh thu (Price + Freight) * Qty

    FOREIGN KEY (Date_Key) REFERENCES Dim_Date(Date_Key),
    -- FOREIGN KEY (Delivered_Date_Key) REFERENCES Dim_Date(Date_Key) -- Tạm bỏ ràng buộc vì có thể có NULL
);

-- 3. Đổ dữ liệu vào bảng Dim_Date 
INSERT INTO Dim_Date (Date_Key, Full_Date, Year, Quarter, Month, Month_Name)
SELECT DISTINCT 
    CAST(FORMAT(order_purchase_timestamp, 'yyyyMMdd') AS INT) as Date_Key,
    CAST(order_purchase_timestamp AS DATE) as Full_Date,
    YEAR(order_purchase_timestamp) as Year,
    'Q' + CAST(DATEPART(QUARTER, order_purchase_timestamp) AS VARCHAR(1)) as Quarter,
    MONTH(order_purchase_timestamp) as Month,
    DATENAME(MONTH, order_purchase_timestamp) as Month_Name
FROM olist_orders_dataset
WHERE order_purchase_timestamp IS NOT NULL
AND NOT EXISTS (SELECT 1 FROM Dim_Date d WHERE d.Date_Key = CAST(FORMAT(order_purchase_timestamp, 'yyyyMMdd') AS INT));

-- Đổ thêm dữ liệu ngày giao hàng vào Dim_Date nếu chưa có
INSERT INTO Dim_Date (Date_Key, Full_Date, Year, Quarter, Month, Month_Name)
SELECT DISTINCT 
    CAST(FORMAT(order_delivered_customer_date, 'yyyyMMdd') AS INT) as Date_Key,
    CAST(order_delivered_customer_date AS DATE) as Full_Date,
    YEAR(order_delivered_customer_date) as Year,
    'Q' + CAST(DATEPART(QUARTER, order_delivered_customer_date) AS VARCHAR(1)) as Quarter,
    MONTH(order_delivered_customer_date) as Month,
    DATENAME(MONTH, order_delivered_customer_date) as Month_Name
FROM olist_orders_dataset
WHERE order_delivered_customer_date IS NOT NULL
AND NOT EXISTS (SELECT 1 FROM Dim_Date d WHERE d.Date_Key = CAST(FORMAT(order_delivered_customer_date, 'yyyyMMdd') AS INT));


-- 4. Đổ dữ liệu vào bảng Fact_Sales (Cập nhật cột mới)
INSERT INTO Fact_Sales (
    Date_Key, Order_ID, Product_ID, Sales_Amount, Quantity, 
    Delivered_Date_Key, Unit_Price, Revenue
)
SELECT 
    CAST(FORMAT(t1.order_purchase_timestamp, 'yyyyMMdd') AS INT) as Date_Key,
    t1.order_id,
    t2.product_id,
    t2.price as Sales_Amount,
    1 as Quantity,
    
    -- Cột mới Delivered_Date_Key
    CASE 
        WHEN t1.order_delivered_customer_date IS NOT NULL 
        THEN CAST(FORMAT(t1.order_delivered_customer_date, 'yyyyMMdd') AS INT)
        ELSE NULL 
    END,
    
    -- Cột mới Unit_Price
    t2.price as Unit_Price,
    
    -- Cột mới Revenue
    (t2.price + t2.freight_value) * 1 as Revenue

FROM olist_orders_dataset t1
JOIN olist_order_items_dataset t2 ON t1.order_id = t2.order_id
WHERE t1.order_status = 'delivered' AND t1.order_purchase_timestamp IS NOT NULL;


-- 5. Truy vấn dữ liệu để vẽ biểu đồ
SELECT 
    d.Year,
    d.Quarter,
    SUM(f.Sales_Amount) AS Total_Sales_Amount,
    SUM(f.Revenue) AS Total_Revenue, -- Revenue đã bao gồm phí ship
    COUNT(DISTINCT f.Order_ID) AS Total_Orders
FROM Fact_Sales f
JOIN Dim_Date d ON f.Date_Key = d.Date_Key
GROUP BY d.Year, d.Quarter
ORDER BY d.Year, d.Quarter;

-- 6. Truy vấn YoY Growth dựa trên Doanh thu (Revenue)
WITH Monthly_Sales AS (
    SELECT 
        d.Year,
        d.Month,
        d.Month_Name,
        d.Quarter,
        SUM(f.Revenue) AS Revenue -- Sử dụng Revenue thay vì Sales_Amount
    FROM Fact_Sales f
    JOIN Dim_Date d ON f.Date_Key = d.Date_Key
    GROUP BY d.Year, d.Month, d.Month_Name, d.Quarter
)

SELECT 
    Current_Year.Year,
    Current_Year.Quarter,
    Current_Year.Month_Name,
    
    Current_Year.Revenue AS Revenue_This_Year,
    Last_Year.Revenue AS Revenue_Last_Year,

    CAST(ROUND(
        (Current_Year.Revenue - Last_Year.Revenue) / NULLIF(Last_Year.Revenue, 0) * 100
    , 2) AS VARCHAR(20)) + '%' AS YoY_Growth_Percent

FROM Monthly_Sales Current_Year
LEFT JOIN Monthly_Sales Last_Year 
    ON Current_Year.Year = Last_Year.Year + 1 
    AND Current_Year.Month = Last_Year.Month
    
ORDER BY Current_Year.Year, Current_Year.Month;

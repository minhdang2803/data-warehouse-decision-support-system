-- Đây là file mẫu chứa các câu lệnh SQL để chạy với script csv_to_sqlite.py
-- Bạn có thể viết nhiều câu lệnh, ngăn cách bởi dấu chấm phẩy (;)

-- 1. Đếm số lượng khách hàng theo bang (Top 10)
SELECT customer_state, COUNT(*) as so_luong_khach
FROM olist_customers_dataset
GROUP BY customer_state
ORDER BY so_luong_khach DESC
LIMIT 10;

-- 2. Liệt kê 5 thành phố có nhiều người bán nhất
SELECT seller_city, seller_state, COUNT(*) as so_luong_nguoi_ban
FROM olist_sellers_dataset
GROUP BY seller_city, seller_state
ORDER BY so_luong_nguoi_ban DESC
LIMIT 5;

-- 3. Xem danh mục sản phẩm (Limit 5)
SELECT product_category_name, product_category_name_english
FROM product_category_name_translation
LIMIT 5;

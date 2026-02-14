import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os

# Set style
sns.set_theme(style="whitegrid")
plt.rcParams['figure.figsize'] = [12, 6]

def load_and_visualize():
    # 1. Kết nối và load dữ liệu như script cũ
    conn = sqlite3.connect(':memory:')
    cursor = conn.cursor()
    
    # Load Dim_Date và Fact_Sales từ file SQL
    # Lưu ý: Script này giả định bạn đã có logic load CSV -> tạo bảng
    # Để đơn giản, tôi sẽ import lại hàm từ csv_to_sqlite nếu có thể, 
    # nhưng ở đây tôi sẽ viết lại đoạn load nhanh để đảm bảo độc lập.
    
    print("⏳ Đang tải dữ liệu (có thể mất vài giây)...")
    
    # Load CSVs cần thiết
    files_needed = {
        'olist_orders_dataset': 'olist_orders_dataset.csv',
        'olist_order_items_dataset': 'olist_order_items_dataset.csv'
    }
    
    for table, file in files_needed.items():
        if os.path.exists(file):
            df = pd.read_csv(file, encoding='utf-8')
            df.to_sql(table, conn, index=False, if_exists='replace')
        else:
            print(f"❌ Không tìm thấy file {file}")
            return

    # Chạy script tạo Star Schema
    sql_file = 'star_schema/problem_2/problem_2.sql'
    if os.path.exists(sql_file):
        with open(sql_file, 'r', encoding='utf-8') as f:
            script = f.read()
            cursor.executescript(script)
    else:
        print(f"❌ Không tìm thấy file {sql_file}")
        return

    print("✅ Đã chuẩn bị xong dữ liệu trong InMemory DB.")

    # ---------------------------------------------------------
    # 2. Truy vấn dữ liệu để vẽ biểu đồ (Theo Quý)
    # ---------------------------------------------------------
    query = """
    SELECT 
        d.Year,
        d.Quarter,
        SUM(f.Sales_Amount) AS Total_Revenue,
        COUNT(DISTINCT f.Order_ID) AS Total_Orders
    FROM Fact_Sales f
    JOIN Dim_Date d ON f.Date_Key = d.Date_Key
    GROUP BY d.Year, d.Quarter
    ORDER BY d.Year, d.Quarter;
    """
    
    print("⏳ Đang chạy query tổng hợp dữ liệu...")
    df_result = pd.read_sql_query(query, conn)
    
    if df_result.empty:
        print("⚠️ Không có dữ liệu trả về từ query.")
        conn.close()
        return

    print("\nKết quả tổng hợp:")
    print(df_result)

    # Tạo cột 'Period' để hiển thị trục X
    df_result['Period'] = df_result['Year'].astype(str) + '-' + df_result['Quarter']

    # 3. Vẽ biểu đồ kết hợp (Combo Chart)
    fig, ax1 = plt.subplots(figsize=(14, 7))

    # Vẽ cột Doanh thu (Bar Chart) - Trục Y bên trái
    color_revenue = '#3498db'
    sns.barplot(data=df_result, x='Period', y='Total_Revenue', ax=ax1, color=color_revenue, alpha=0.6, label='Revenue')
    ax1.set_ylabel('Total Revenue (BRL)', fontsize=12, color=color_revenue, fontweight='bold')
    ax1.tick_params(axis='y', labelcolor=color_revenue)
    
    # Định dạng trục Y trái thành tiền tệ
    current_values = ax1.get_yticks()
    ax1.set_yticklabels(['{:,.0f}'.format(x) for x in current_values])

    # Vẽ đường Số lượng đơn hàng (Line Chart) - Trục Y bên phải
    ax2 = ax1.twinx()
    color_orders = '#e74c3c'
    sns.lineplot(data=df_result, x='Period', y='Total_Orders', ax=ax2, color=color_orders, marker='o', linewidth=3, label='Orders')
    ax2.set_ylabel('Total Orders', fontsize=12, color=color_orders, fontweight='bold')
    ax2.tick_params(axis='y', labelcolor=color_orders)

    # Trang trí chung
    plt.title('Sales Performance by Quarter (Revenue vs Orders)', fontsize=16, fontweight='bold', pad=20)
    ax1.set_xlabel('Quarter', fontsize=12)
    ax1.grid(True, linestyle='--', alpha=0.5)

    # Thêm chú thích giá trị lên cột
    for index, row in df_result.iterrows():
        ax1.text(index, row.Total_Revenue, f'{row.Total_Revenue:,.0f}', color='black', ha="center", va="bottom", fontsize=10)

    # Lưu biểu đồ
    output_file = 'revenue_quarterly_chart.png'
    plt.tight_layout()
    plt.savefig(output_file)
    print(f"📊 Đã lưu biểu đồ thành công: {output_file}")

    conn.close()

if __name__ == "__main__":
    load_and_visualize()

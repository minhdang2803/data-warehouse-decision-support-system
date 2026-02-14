import sqlite3
import csv
import os
import sys

def load_data(conn):
    """Load tất cả file CSV trong thư mục hiện tại vào SQLite in-memory"""
    cursor = conn.cursor()
    csv_files = [f for f in os.listdir('.') if f.endswith('.csv')]

    if not csv_files:
        print("Không tìm thấy file CSV nào trong thư mục hiện tại.")
        return

    print(f"Đang tải {len(csv_files)} file CSV vào database...")

    for csv_file in csv_files:
        table_name = os.path.splitext(csv_file)[0]
        try:
            with open(csv_file, 'r', encoding='utf-8-sig') as f:
                reader = csv.reader(f)
                try:
                    headers = next(reader)
                except StopIteration:
                    print(f"File {csv_file} rỗng, bỏ qua.")
                    continue

                # Làm sạch tên cột
                sanitized_headers = [h.strip().replace(' ', '_').replace('-', '_').replace('.', '_') for h in headers]
                
                # Tạo bảng
                columns_def = ', '.join([f'"{h}" TEXT' for h in sanitized_headers])
                create_sql = f'CREATE TABLE "{table_name}" ({columns_def})'
                cursor.execute(create_sql)
                
                # Chèn dữ liệu
                placeholders = ', '.join(['?'] * len(sanitized_headers))
                insert_sql = f'INSERT INTO "{table_name}" VALUES ({placeholders})'
                
                rows_to_insert = []
                for row in reader:
                    if len(row) == len(sanitized_headers):
                        rows_to_insert.append(row)
                
                cursor.executemany(insert_sql, rows_to_insert)
                conn.commit()
                print(f"✅ Đã tải: {table_name} ({len(rows_to_insert)} dòng)")
                
        except Exception as e:
            print(f"❌ Lỗi khi tải {csv_file}: {e}")
    print("---------------------------------------------------\n")

def run_query(conn, query):
    """Thực thi một câu lệnh SQL và in kết quả"""
    cursor = conn.cursor()
    try:
        cursor.execute(query)
        # Kiểm tra nếu là câu lệnh SELECT hoặc WITH (thường trả về dữ liệu)
        q_lower = query.strip().lower()
        if q_lower.startswith("select") or q_lower.startswith("pragma") or q_lower.startswith("with"):
            rows = cursor.fetchall()
            if cursor.description:
                col_names = [d[0] for d in cursor.description]
                print(f"\nQUERY: {query.strip()}")
                print(f"{' | '.join(col_names)}")
                print("-" * (len(' | '.join(col_names))))
            for row in rows:
                print(row)
            print(f"({len(rows)} rows)")
        else:
            conn.commit()
            print(f"\nEXECUTED: {query.strip()}")
    except sqlite3.Error as e:
        print(f"Lỗi SQL: {e}")

def run_sql_file(conn, file_path):
    """Đọc file SQL và thực thi từng lệnh"""
    print(f"Đang chạy script từ file: {file_path}")
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
            
        # Tách lệnh bằng dấu ;
        commands = sql_content.split(';')
        for cmd in commands:
            if cmd.strip():
                run_query(conn, cmd)
    except FileNotFoundError:
        print(f"File không tồn tại: {file_path}")
    except Exception as e:
        print(f"Lỗi khi đọc file: {e}")

def interactive_mode(conn):
    """Chế độ tương tác nhập lệnh trực tiếp (Hỗ trợ multi-line)"""
    cursor = conn.cursor()
    print("Database in-memory đã sẵn sàng!")
    print("Các lệnh hỗ trợ:")
    print("  - tables:               Liệt kê danh sách bảng")
    print("  - schema <tên_bảng>:    Xem cấu trúc bảng")
    print("  - show <tên_bảng> [N]:  Xem N dòng dữ liệu đầu tiên")
    print("  - exit:                 Thoát chương trình")
    print("  - <SQL>;                Nhập lệnh SQL (Kết thúc bằng dấu chấm phẩy ; để chạy)")
    
    sql_buffer = ""

    while True:
        try:
            # Đổi prompt tùy theo đang nhập mới hay nhập tiếp
            if not sql_buffer:
                prompt = "\nSQL> "
            else:
                prompt = "   > "

            line = input(prompt)
            line_stripped = line.strip()

            # Nếu dòng trống
            if not line_stripped:
                # Nếu đang trong buffer mà nhập 2 lần enter thì nhắc nhở (hoặc có thể bỏ qua)
                continue
            
            # Xử lý các lệnh đặc biệt (chỉ khi chưa có gì trong buffer)
            if not sql_buffer:
                cmd = line_stripped.lower()
                
                if cmd == 'exit':
                    break
                    
                elif cmd == 'tables':
                    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
                    print("\nDanh sách bảng:")
                    for t in cursor.fetchall():
                        print(f"- {t[0]}")
                    continue
                    
                elif cmd.startswith('schema '):
                    parts = line_stripped.split()
                    if len(parts) > 1:
                        t_name = parts[1]
                        cursor.execute(f"PRAGMA table_info(\"{t_name}\")")
                        cols = cursor.fetchall()
                        if cols:
                            print(f"\nCấu trúc bảng '{t_name}':")
                            print(f"{'CID':<5} {'Name':<35} {'Type':<10}")
                            print("-" * 50)
                            for col in cols:
                                print(f"{col[0]:<5} {col[1]:<35} {col[2]:<10}")
                        else:
                            print(f"Không tìm thấy bảng '{t_name}'")
                    else:
                        print("Cú pháp: schema <tên_bảng>")
                    continue
                    
                elif cmd.startswith('show '):
                    parts = line_stripped.split()
                    if len(parts) > 1:
                        t_name = parts[1]
                        limit = 10
                        if len(parts) > 2 and parts[2].isdigit():
                            limit = int(parts[2])
                        run_query(conn, f'SELECT * FROM "{t_name}" LIMIT {limit}')
                    else:
                        print("Cú pháp: show <tên_bảng> [số_dòng]")
                    continue

            # Cộng dồn vào buffer
            if sql_buffer:
                sql_buffer += "\n" + line
            else:
                sql_buffer = line

            # Kiểm tra xem đã kết thúc bằng dấu chấm phẩy chưa
            if sql_buffer.strip().endswith(';'):
                run_query(conn, sql_buffer)
                sql_buffer = "" # Reset buffer sau khi chạy
            
        except KeyboardInterrupt:
            print("\nĐã hủy lệnh.")
            sql_buffer = ""
        except Exception as e:
            print(f"Lỗi: {e}")
            sql_buffer = ""

def main():
    conn = sqlite3.connect(':memory:')
    load_data(conn)

    if len(sys.argv) > 1:
        sql_file = sys.argv[1]
        run_sql_file(conn, sql_file)
        print("\n!!! Đã chạy xong script, chuyển sang chế độ tương tác !!!\n")

    interactive_mode(conn)

    conn.close()

if __name__ == "__main__":
    main()

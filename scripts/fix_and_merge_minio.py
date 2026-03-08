import pandas as pd
import os
import sys

# ===========================================================
# CẤU HÌNH MINIO
# ===========================================================
MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
BUCKET_NAME = "warehouse"

# Đường dẫn file
LOCAL_BAD_FILE = "itviec_jobs_tu_hdfs.csv"
S3_PATH = f"s3://{BUCKET_NAME}/raw/itviec_jobs_full.csv"

# Cấu hình Storage Options cho Pandas
MINIO_OPTIONS = {
    "key": MINIO_ACCESS_KEY,
    "secret": MINIO_SECRET_KEY,
    "client_kwargs": {
        "endpoint_url": MINIO_ENDPOINT
    }
}

# ===========================================================
# HÀM SỬA LỖI FONT (MAGIC FIX)
# ===========================================================
def fix_mojibake(text):
    """
    Sửa lỗi font: Chuỗi bị đọc nhầm thành CP437 -> Trả về UTF-8 đúng.
    Ví dụ: 'Chuy├¬n' -> 'Chuyên'
    """
    if not isinstance(text, str):
        return text
    
    # Chỉ sửa nếu thấy ký tự lạ đặc trưng của lỗi này (├, ╗, ┐...)
    # Để tránh làm hỏng các từ đã đúng
    if not any(c in text for c in ['├', '╗', '┐', '╞', 'ß']):
        return text

    try:
        # Cơ chế: Encode về bytes CP437 gốc, sau đó Decode lại bằng UTF-8
        return text.encode('cp437').decode('utf-8')
    except Exception:
        return text

# ===========================================================
# QUY TRÌNH XỬ LÝ
# ===========================================================
if __name__ == "__main__":
    print(f"1. Đang đọc file lỗi: {LOCAL_BAD_FILE}...")
    
    df_bad = None
    # Danh sách các encoding cần thử (Ưu tiên UTF-16 vì lỗi 0xff)
    encodings_to_try = ['utf-16', 'utf-8-sig', 'utf-8', 'latin1', 'cp1252']
    
    for enc in encodings_to_try:
        try:
            print(f"   - Đang thử đọc với encoding: {enc}...")
            # delimiter=None để python engine tự đoán dấu phẩy hoặc tab
            df_bad = pd.read_csv(LOCAL_BAD_FILE, dtype=str, encoding=enc, engine='python')
            print(f"   => THÀNH CÔNG với encoding: {enc}")
            break
        except Exception as e:
            print(f"   => Thất bại ({e})")
            continue
            
    if df_bad is None:
        print("ERROR: Không thể đọc file với bất kỳ encoding nào.")
        sys.exit(1)

    # --- SỬA TÊN CỘT ---
    print("   - Đang chuẩn hóa tên cột...")
    new_cols = []
    for col in df_bad.columns:
        # Sửa lỗi font trong tên cột và xóa các ký tự BOM rác
        fixed_col = fix_mojibake(str(col)).replace('\ufeff', '').replace('∩╗┐', '').strip()
        new_cols.append(fixed_col)
    df_bad.columns = new_cols
    
    print(f"   - Các cột sau khi sửa: {list(df_bad.columns)}")

    # --- SỬA DỮ LIỆU ---
    print("   - Đang sửa lỗi font từng dòng dữ liệu...")
    for col in df_bad.columns:
        df_bad[col] = df_bad[col].apply(fix_mojibake)
        
    print(f"   - Ví dụ dữ liệu sau khi sửa:")
    print(df_bad.head(3))

    # -------------------------------------------------------
    print(f"\n2. Đang kết nối MinIO để đọc dữ liệu cũ ({S3_PATH})...")
    try:
        df_old = pd.read_csv(S3_PATH, storage_options=MINIO_OPTIONS, dtype=str)
        print(f"   - Tìm thấy file cũ trên S3 với {len(df_old)} dòng.")
        df_final = pd.concat([df_old, df_bad], ignore_index=True)
    except FileNotFoundError:
        print("   - Chưa có file trên S3, sẽ tạo mới.")
        df_final = df_bad
    except Exception as e:
        print(f"   - Lỗi đọc S3 (bucket trống?): {e}")
        print("   - Tạo file mới từ dữ liệu vừa sửa.")
        df_final = df_bad

    # -------------------------------------------------------
    print("\n3. Xử lý trùng lặp và lưu lại...")
    if 'job_link' in df_final.columns:
        before_dedup = len(df_final)
        df_final.drop_duplicates(subset=['job_link'], keep='last', inplace=True)
        print(f"   - Đã loại bỏ {before_dedup - len(df_final)} dòng trùng lặp.")
    
    # -------------------------------------------------------
    print(f"\n4. Upload ngược lên MinIO: {S3_PATH}")
    try:
        # Lưu encoding utf-8-sig để Excel mở không bị lỗi font tiếng Việt sau này
        df_final.to_csv(S3_PATH, index=False, encoding='utf-8-sig', storage_options=MINIO_OPTIONS)
        print("   - ✅ THÀNH CÔNG! Đã cập nhật file trên Data Lake.")
        print(f"   - Tổng số dòng hiện tại: {len(df_final)}")
    except Exception as e:
        print(f"   - ❌ LỖI UPLOAD: {e}")
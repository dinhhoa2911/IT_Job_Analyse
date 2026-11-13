from spark_config import load_settings

job_settings = load_settings()

from hdfs import InsecureClient
import pandas as pd
import os

# ======================================================
# CONFIGURATION
# ======================================================
HDFS_HOST = job_settings.hdfs_host
HDFS_PORT = job_settings.hdfs_port
HDFS_USER = job_settings.hdfs_user

LOCAL_FILE = "/opt/spark/scripts/itviec_jobs_full.csv"
HDFS_PATH = "/data_lake/raw/itviec_jobs_full.csv"
TMP_HDFS_LOCAL = "/opt/spark/scripts/hdfs_old.csv"

print("🚀 Bắt đầu cập nhật file ITviec trên HDFS (chỉ thêm record mới)...")

try:
    hdfs_url = f"http://{HDFS_HOST}:{HDFS_PORT}"
    client = InsecureClient(hdfs_url, user=HDFS_USER)

    # ======================================================
    #  Tải file HDFS hiện tại (nếu có)
    # ======================================================
    if client.status(HDFS_PATH, strict=False):
        print(f"📥 Đang tải file cũ từ HDFS: {HDFS_PATH}")
        with client.read(HDFS_PATH, encoding="utf-8") as reader:
            old_df = pd.read_csv(reader)
        print(f"📄 File cũ có {len(old_df)} dòng.")
    else:
        print("🆕 Không có file cũ trên HDFS.")
        old_df = pd.DataFrame()

    # ======================================================
    #  Đọc file mới local
    # ======================================================
    if not os.path.exists(LOCAL_FILE):
        raise FileNotFoundError(f"Không tìm thấy file local: {LOCAL_FILE}")
    new_df = pd.read_csv(LOCAL_FILE)
    print(f"🆕 File mới có {len(new_df)} dòng.")

    # ======================================================
    #  Gộp + loại trùng theo job_link
    # ======================================================
    if not old_df.empty:
        combined_df = pd.concat([old_df, new_df], ignore_index=True)
        combined_df.drop_duplicates(subset=["job_link"], inplace=True)
        print(f"📊 Sau khi gộp và loại trùng: {len(combined_df)} dòng.")
    else:
        combined_df = new_df

    # ======================================================
    #  Lưu tạm ra local rồi upload ngược lên HDFS
    # ======================================================
    combined_df.to_csv(TMP_HDFS_LOCAL, index=False, encoding="utf-8-sig")

    client.makedirs(os.path.dirname(HDFS_PATH))
    client.upload(HDFS_PATH, TMP_HDFS_LOCAL, overwrite=True)

    print(f" Đã cập nhật thành công: {HDFS_PATH}")
    print(f" Tổng số record hiện tại: {len(combined_df)}")
except Exception as e:
    print(f" Lỗi cập nhật HDFS: {e}")

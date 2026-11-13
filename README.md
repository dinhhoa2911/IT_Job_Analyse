# IT Job Analyse

**Phân tích thị trường tuyển dụng CNTT và hỗ trợ tìm kiếm việc làm thông minh ứng dụng AI.**

## Mục tiêu

Dự án xây dựng nền tảng Data Lakehouse để tổ chức dữ liệu tuyển dụng ITViec, phân tích nhu cầu nhân lực và kỹ năng, đồng thời cung cấp trợ lý hỗ trợ tìm việc và định hướng nghề nghiệp dựa trên dữ liệu.

Lakehouse/ETL do chủ dự án phát triển; phần RAG do thành viên khác phát triển. README mô tả các thành phần có trong repository; 

## Kiến trúc

<img width="1355" height="675" alt="image" src="https://github.com/user-attachments/assets/7810100c-a0cc-4ced-9d24-f050e0e11786" />


Luồng chính: dữ liệu tuyển dụng → MinIO → Iceberg Bronze/Silver/Gold qua Spark; Airflow điều phối, Trino phục vụ truy vấn và Superset phục vụ BI. Backend FastAPI kết hợp tìm kiếm Milvus và phân tích SQL để cung cấp kết quả cho giao diện React.

**Lưu ý:** ảnh thể hiện nhánh vector hóa từ Gold, nhưng [script hiện tại](scripts/Vectorize_To_Milvus.py) đọc **`iceberg.silver.it_jobs_clean`**. Gold phục vụ phân tích có cấu trúc qua Trino. Ảnh là sơ đồ thiết kế; các nhãn như LangChain không thay thế bằng chứng triển khai trong code. Xem [đối chiếu kiến trúc và triển khai](docs/thesis-code-mapping.md).

## Công nghệ

| Nhóm | Công nghệ |
|---|---|
| Lưu trữ và metadata | MinIO, Apache Iceberg, Hive Metastore, PostgreSQL |
| Xử lý và điều phối | Apache Spark, Apache Airflow, Redis |
| Truy vấn và BI | Trino, Apache Superset |
| Tìm kiếm và AI | Milvus, OpenAI Embedding, Claude, BM25, RRF, cross-encoder rerank |
| Backend và CV | FastAPI, pdfplumber, PyMuPDF, PaddleOCR |
| Frontend và tài khoản | React, Tailwind CSS, Chart.js, Firebase, EmailJS |
| Dự báo và nhánh lưu trữ khác | Prophet; HDFS còn được một số script tham chiếu |

## Chức năng chính

Các nhóm sau có mã triển khai hoặc cấu hình trong repository, chưa được nghiệm thu tích hợp:

- **Xử lý dữ liệu:** nạp Bronze, làm sạch Silver, xây dựng Star Schema Gold.
- **Phân tích thị trường:** truy vấn theo kỹ năng, địa điểm, doanh nghiệp và thời gian; NL2SQL và biểu đồ.
- **Tìm kiếm việc làm:** kết hợp tìm kiếm ngữ nghĩa, từ khóa và xếp hạng lại kết quả.
- **Hỗ trợ ứng viên:** trích xuất CV PDF/OCR, so khớp việc làm, phân tích khoảng cách kỹ năng và gợi ý lộ trình học tập.
- **Giao diện tương tác:** chat, hiển thị phân tích, tài khoản và quản lý CV.

Repository cũng có cấu hình Superset và các module Prophet; nhánh dự báo chưa hoàn thiện luồng tới chat/UI.

## Cấu trúc thư mục

```text
chatbot-api/              # Backend FastAPI và các dịch vụ RAG/NL2SQL/CV
frontend-chatbot-main/    # Giao diện React
chatbot/                 # Compose cho API và UI
scripts/                 # ETL, vector hóa, Prophet và tiện ích dữ liệu
orchestration/           # Compose Airflow và DAG được mount
dags/                    # Các bản DAG khác còn trong repo
spark_iceberg/           # Spark image, cấu hình và Compose
minio/                   # MinIO và các cấu hình/script HDFS còn lưu
metastore/               # Hive Metastore và PostgreSQL
trino/                   # Trino và catalog
milvus/                  # Milvus, etcd và Attu
superset/                # Cấu hình BI
data/, models/           # Dữ liệu và model artifact có trong repo
tests/, tools/           # Kiểm tra offline và công cụ kiểm tra cú pháp
docs/                    # Kiến trúc, hướng dẫn và báo cáo kỹ thuật
```

## Hướng dẫn chạy

1. Chuẩn bị Docker/Compose và network external `system_network`; kiểm tra subnet phù hợp với IP MinIO được cấu hình.
2. Chuẩn bị hai file môi trường riêng từ [mẫu backend](chatbot-api/.env.example) và [mẫu Spark](scripts/.env.example), thay placeholder bằng giá trị hợp lệ. Không ghi đè `.env` đã có hoặc commit credentials.
3. Kiểm tra dữ liệu, bảng và collection cần thiết, rồi khởi động từng stack theo [hướng dẫn môi trường](docs/setup-guide.md). Ví dụ, **chỉ khi các dịch vụ phụ thuộc và dữ liệu đã sẵn sàng**, từ root repo có thể gọi stack API/UI bằng:

   ```powershell
   docker compose -f chatbot/docker-compose.yml up -d
   ```

4. Giao diện React ở `http://localhost:3000`, API docs ở `http://localhost:8100/docs`. Các cổng khác, mount và thứ tự phụ thuộc nằm trong hướng dẫn chi tiết.
<img width="1921" height="877" alt="image" src="https://github.com/user-attachments/assets/e9fa0790-8267-4dce-ab5d-6d3220d891e2" />

Xem [cấu hình từng thành phần](docs/configuration.md) để cấp credentials MinIO bắt buộc cho API và Spark, cùng quy tắc ưu tiên biến môi trường. Không chạy crawl hoặc embedding chỉ để kiểm tra khởi động.

## Tài liệu tham khảo
- [Hướng dẫn môi trường và khởi động](docs/setup-guide.md), [cấu hình](docs/configuration.md).
- [Kiểm kê hiện trạng](docs/current-state.md), [đối chiếu khóa luận–code](docs/thesis-code-mapping.md).
- [Báo cáo tương thích giữa Lakehouse và RAG](docs/integration-review.md).
- [Baseline kiểm tra và giới hạn bằng chứng](docs/validation-baseline.md).
- [Kế hoạch và backlog](docs/refactor-plan.md), [lịch sử công việc](TASKS.md).

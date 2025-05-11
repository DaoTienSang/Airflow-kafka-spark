# Stock Market Analysis Pipeline

Dự án phân tích cổ phiếu sử dụng công nghệ Docker, Airflow, Spark và Kafka để thu thập, xử lý và trực quan hóa dữ liệu thị trường chứng khoán.

## Kiến trúc

- **Apache Airflow**: Điều phối công việc, lập lịch thu thập dữ liệu
- **Apache Kafka**: Hệ thống message broker lưu trữ dữ liệu
- **Apache Spark**: Xử lý dữ liệu và tính toán các chỉ số kỹ thuật
- **Streamlit**: Trực quan hóa dữ liệu cổ phiếu và các chỉ số kỹ thuật

## Luồng dữ liệu

1. **Thu thập dữ liệu** (Airflow):
   - DAG lấy dữ liệu giá cổ phiếu hiện tại và lịch sử từ vnstock
   - Dữ liệu được gửi đến Kafka topic "stock-history-topic"

2. **Xử lý dữ liệu** (Spark):
   - Đọc dữ liệu từ "stock-history-topic"
   - Tính toán các chỉ số kỹ thuật: MA (5, 20), RSI, MACD
   - Phân tích xu hướng và đưa ra đề xuất mua/bán
   - Ghi dữ liệu đã xử lý vào Kafka topic "stock-processed-topic"

3. **Trực quan hóa** (Streamlit):
   - Đọc dữ liệu đã xử lý từ "stock-processed-topic"
   - Hiển thị biểu đồ và các chỉ số kỹ thuật
   - Hiển thị đề xuất mua/bán và phân tích

## Cách chạy

1. Clone repository:
```bash
git clone <repository-url>
cd docker-airflow-spark
```

2. Khởi chạy các dịch vụ:
```bash
cd stock_pipeline/airflow
docker-compose up -d
```

3. Truy cập các ứng dụng:
   - Airflow UI: http://localhost:8080 (username/password: admin/admin)
   - Kafka UI: http://localhost:8180
   - Spark UI: http://localhost:4041
   - Streamlit Dashboard: http://localhost:8501

## Các tính năng

1. **Thu thập dữ liệu cổ phiếu**:
   - Dữ liệu giá hiện tại
   - Dữ liệu lịch sử từ năm 2000 đến nay

2. **Tính toán chỉ số kỹ thuật**:
   - Moving Averages (MA 5, MA 20)
   - Relative Strength Index (RSI)
   - Moving Average Convergence Divergence (MACD)

3. **Đề xuất đầu tư**:
   - Phân tích xu hướng thị trường dựa trên chỉ số kỹ thuật
   - Đưa ra đề xuất mua/bán/giữ cổ phiếu
   - Cung cấp lý do chi tiết cho đề xuất

4. **Trực quan hóa**:
   - Biểu đồ nến và các chỉ số kỹ thuật
   - Dashboard tương tác

## Cấu trúc thư mục

```
stock_pipeline/
├── airflow/           # Cấu hình và DAGs Airflow
│   ├── dags/          # DAG scripts
│   ├── Dockerfile     # Dockerfile cho Airflow
│   └── docker-compose.yml  # Docker Compose cho toàn bộ hệ thống
├── spark/             # Ứng dụng Spark 
│   └── stock_analysis.py  # Script phân tích cổ phiếu
└── streamlit/         # Ứng dụng dashboard
    ├── app.py         # Streamlit application
    └── Dockerfile     # Dockerfile cho Streamlit
```

## Hướng dẫn sử dụng

1. **Khởi chạy Airflow DAG**:
   - Truy cập Airflow UI tại http://localhost:8080
   - Kích hoạt DAG "fetch_stock_data_to_kafka"

2. **Xem dữ liệu trong Kafka**:
   - Truy cập Kafka UI tại http://localhost:8180
   - Kiểm tra các topics: "stock-topic", "stock-history-topic", "stock-processed-topic"

3. **Theo dõi Spark processing**:
   - Xem Spark UI tại http://localhost:4041
   - Kiểm tra logs: `docker logs -f spark-stock-analysis`

4. **Xem Dashboard**:
   - Truy cập Streamlit tại http://localhost:8501
   - Chọn mã cổ phiếu và xem các phân tích kỹ thuật và đề xuất 
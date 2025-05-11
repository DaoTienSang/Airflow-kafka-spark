# Stock Market Analysis Dashboard

Ứng dụng Streamlit hiển thị dữ liệu và phân tích kỹ thuật cho các mã cổ phiếu được thu thập qua pipeline Apache Airflow, Kafka và Spark.

## Tính năng

- Hiển thị biểu đồ nến (Candlestick) cho dữ liệu lịch sử cổ phiếu
- Hiển thị các chỉ số kỹ thuật:
  - Moving Averages (MA 5, MA 20)
  - Relative Strength Index (RSI)
  - Moving Average Convergence Divergence (MACD)
- Hiển thị đề xuất đầu tư dựa trên phân tích kỹ thuật
- Cung cấp phân tích chi tiết về lý do đằng sau đề xuất
- Hiển thị dữ liệu chi tiết của từng mã cổ phiếu

## Luồng dữ liệu

1. Spark xử lý dữ liệu từ Kafka topic "stock-history-topic"
2. Tính toán các chỉ số kỹ thuật và phân tích xu hướng
3. Ghi dữ liệu đã xử lý vào Kafka topic "stock-processed-topic"
4. Ứng dụng Streamlit đọc dữ liệu từ "stock-processed-topic"
5. Hiển thị biểu đồ và phân tích cho người dùng

## Cách chạy

### Chạy với Docker
```bash
cd stock_pipeline/airflow
docker-compose up -d
```

Truy cập ứng dụng tại: http://localhost:8501

### Các thành phần
- **Airflow**: Thu thập dữ liệu cổ phiếu (http://localhost:8080)
- **Kafka**: Lưu trữ dữ liệu (quản lý tại http://localhost:8180)
- **Spark**: Xử lý và tính toán các chỉ số (UI tại http://localhost:4041)
- **Streamlit**: Hiển thị và trực quan hóa (http://localhost:8501)

## Cách sử dụng

1. Chọn mã cổ phiếu từ menu bên trái
2. Xem biểu đồ nến và các chỉ số kỹ thuật
3. Đọc đề xuất đầu tư và lý do đề xuất
4. Xem chi tiết về MA, RSI và MACD để hiểu rõ hơn về xu hướng

## Lưu ý

- Ứng dụng cần kết nối được với Kafka để nhận dữ liệu đã xử lý từ Spark
- Đảm bảo toàn bộ pipeline đang chạy: Airflow, Kafka và Spark
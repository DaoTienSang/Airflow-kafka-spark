from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_json, struct, when, lit, expr, collect_list, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, ArrayType, TimestampType
import pandas as pd
import numpy as np
from datetime import datetime
import time

# Khởi tạo SparkSession với timeout và tham số để tăng tính ổn định
spark = SparkSession.builder \
    .appName("Stock Analysis") \
    .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoint") \
    .config("spark.network.timeout", "800s") \
    .config("spark.executor.heartbeatInterval", "120s") \
    .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "4g") \
    .config("spark.memory.fraction", "0.8") \
    .getOrCreate()

# Log level
spark.sparkContext.setLogLevel("WARN")

print("Spark session created successfully")

# Định nghĩa schema cho dữ liệu JSON từ Kafka
schema = StructType([
    StructField("symbol", StringType()),
    StructField("current_price", DoubleType()),
    StructField("historical_data", ArrayType(
        StructType([
            StructField("time", StringType()),
            StructField("open", DoubleType()),
            StructField("high", DoubleType()),
            StructField("low", DoubleType()),
            StructField("close", DoubleType()),
            StructField("volume", DoubleType())
        ])
    ))
])

# Dùng địa chỉ network của container Kafka trong Docker
kafka_bootstrap_servers = "airflow-kafka-1:9092"

# Đọc dữ liệu từ Kafka
kafka_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", "stock-history-topic") \
    .option("startingOffsets", "earliest") \
    .option("maxOffsetsPerTrigger", 20000) \
    .option("failOnDataLoss", "false") \
    .load()

# Parse JSON từ Kafka
parsed_df = kafka_df \
    .selectExpr("CAST(value AS STRING) as json_data") \
    .select(from_json(col("json_data"), schema).alias("data")) \
    .select("data.*")

# Explode array để xử lý từng record lịch sử
exploded_df = parsed_df \
    .select(
        col("symbol"),
        col("current_price"),
        expr("explode(historical_data) as history")
    ) \
    .select(
        col("symbol"),
        col("current_price"),
        col("history.time").alias("time"),
        col("history.open").alias("open"),
        col("history.high").alias("high"),
        col("history.low").alias("low"),
        col("history.close").alias("close"),
        col("history.volume").alias("volume")
    )

# Chuyển timestamp string thành timestamp thực và chuyển thành string date
# Để tránh lỗi datetime64 khi chuyển sang pandas
exploded_df = exploded_df \
    .withColumn("timestamp", to_timestamp(col("time"), "yyyy-MM-dd")) \
    .withColumn("date_str", expr("date_format(timestamp, 'yyyy-MM-dd')"))

# Biến toàn cục để lưu trữ SparkContext state
spark_active = True

# Xử lý dữ liệu theo batch thay vì sử dụng window functions
def process_batch(batch_df, batch_id):
    global spark_active
    
    try:
        # Kiểm tra SparkContext còn hoạt động không
        if not spark_active or spark.sparkContext._jsc.sc().isStopped():
            print(f"SparkContext đã bị shutdown, không thể xử lý batch {batch_id}")
            return
            
        # Kiểm tra batch có dữ liệu không
        try:
            if batch_df.isEmpty():
                print(f"Batch {batch_id} không có dữ liệu, bỏ qua")
                return
        except Exception as e:
            print(f"Không thể kiểm tra batch rỗng: {e}")
            # Nếu không thể kiểm tra isEmpty, giả định là có dữ liệu và tiếp tục
        
        # Xóa cột timestamp trước khi chuyển sang Pandas để tránh lỗi
        # Và sử dụng date_str thay thế
        batch_df_no_ts = batch_df.drop("timestamp")
        
        # Lấy danh sách các symbol riêng biệt trong batch này để xử lý
        symbols_in_batch = batch_df_no_ts.select("symbol").distinct().rdd.flatMap(lambda x: x).collect()
        print(f"🔍 Batch {batch_id} có {len(symbols_in_batch)} mã cổ phiếu: {symbols_in_batch}")
        
        # Nếu batch này không có dữ liệu nhiều, đợi thêm thời gian để tích lũy dữ liệu
        if len(symbols_in_batch) < 10:
            print(f"Batch {batch_id} chỉ có {len(symbols_in_batch)} mã, đợi thêm dữ liệu...")
            if batch_id < 3:  # Chỉ đợi thêm trong các batch đầu tiên
                time.sleep(60)  # Đợi thêm 60 giây
        
        # Giới hạn số lượng dữ liệu nếu quá lớn để tránh quá tải
        count = batch_df_no_ts.count()
        print(f"Batch {batch_id} có {count} records")
        
        if count > 20000:
            print(f"Batch {batch_id} quá lớn ({count} records), phân chia thành các batch nhỏ hơn")
            # Phân chia thành nhiều batch nhỏ hơn
            batches = batch_df_no_ts.randomSplit([0.2, 0.2, 0.2, 0.2, 0.2])
            for i, small_batch in enumerate(batches):
                try:
                    process_small_batch(small_batch, f"{batch_id}_{i}")
                except Exception as e:
                    print(f"Lỗi khi xử lý sub-batch {batch_id}_{i}: {e}")
        else:
            # Xử lý batch nhỏ
            process_small_batch(batch_df_no_ts, batch_id)
    
    except Exception as e:
        import traceback
        print(f"Lỗi trong process_batch {batch_id}: {e}")
        print(traceback.format_exc())

def process_small_batch(batch_df, batch_id):
    """Xử lý một batch nhỏ dữ liệu"""
    
    if spark.sparkContext._jsc.sc().isStopped():
        print(f"SparkContext đã bị shutdown, không thể xử lý batch {batch_id}")
        return
        
    try:
        # Chuyển sang Pandas để dễ tính toán các chỉ số kỹ thuật
        batch_pd = batch_df.toPandas()
        
        if batch_pd.empty:
            print(f"Batch {batch_id} không có dữ liệu sau khi chuyển sang pandas")
            return
        
        # Chuyển date_str thành datetime nếu cần
        batch_pd['date'] = pd.to_datetime(batch_pd['date_str'])
        
        # Nhóm theo symbol và sắp xếp theo thời gian
        symbols = batch_pd['symbol'].unique()
        result_dfs = []
        
        for symbol in symbols:
            # Lọc dữ liệu cho mỗi mã cổ phiếu
            symbol_df = batch_pd[batch_pd['symbol'] == symbol].sort_values('date')
            
            # Nếu có đủ dữ liệu thì tính các chỉ số
            if len(symbol_df) > 0:
                # Tính MA5, MA20
                symbol_df['ma5'] = symbol_df['close'].rolling(window=5).mean()
                symbol_df['ma20'] = symbol_df['close'].rolling(window=20).mean()
                
                # Tính RSI
                delta = symbol_df['close'].diff()
                gain = delta.where(delta > 0, 0).rolling(window=14).mean()
                loss = -delta.where(delta < 0, 0).rolling(window=14).mean()
                
                # Xử lý tránh chia cho 0
                rs = pd.Series(np.where(loss == 0, 0, gain / loss), index=loss.index)
                symbol_df['rsi'] = 100 - (100 / (1 + rs))
                
                # Tính MACD
                ema12 = symbol_df['close'].ewm(span=12, adjust=False).mean()
                ema26 = symbol_df['close'].ewm(span=26, adjust=False).mean()
                
                symbol_df['macd_line'] = ema12 - ema26
                symbol_df['macd_signal'] = symbol_df['macd_line'].ewm(span=9, adjust=False).mean()
                symbol_df['macd_histogram'] = symbol_df['macd_line'] - symbol_df['macd_signal']
                
                # Đưa ra đề xuất dựa trên các chỉ số
                conditions = [
                    # BUY: Xu hướng tăng (MA5 > MA20), RSI chưa quá mua, MACD tích cực
                    (symbol_df['ma5'] > symbol_df['ma20']) & 
                    (symbol_df['rsi'] < 70) & 
                    (symbol_df['macd_line'] > symbol_df['macd_signal']),
                    
                    # HOLD/SELL: Xu hướng tăng nhưng RSI quá mua
                    (symbol_df['ma5'] > symbol_df['ma20']) & 
                    (symbol_df['rsi'] >= 70),
                    
                    # WATCH/BUY: Xu hướng giảm nhưng RSI quá bán và MACD tích cực
                    (symbol_df['ma5'] < symbol_df['ma20']) & 
                    (symbol_df['rsi'] <= 30) & 
                    (symbol_df['macd_line'] > symbol_df['macd_signal']),
                    
                    # SELL/AVOID: Xu hướng giảm và MACD tiêu cực
                    (symbol_df['ma5'] < symbol_df['ma20']) & 
                    (symbol_df['macd_line'] <= symbol_df['macd_signal'])
                ]
                
                choices = ['BUY', 'HOLD/SELL', 'WATCH/BUY', 'SELL/AVOID']
                symbol_df['suggestion'] = np.select(conditions, choices, default='HOLD')
                
                # Thêm lý do
                reasons = [
                    "Xu hướng tăng (MA5 > MA20). RSI chưa quá mua. MACD tích cực (MACD > Signal). Đề xuất: MUA - Xu hướng tăng, RSI chưa quá mua, MACD tích cực.",
                    "Xu hướng tăng (MA5 > MA20). Quá mua (RSI > 70). Đề xuất: CÂN NHẮC BÁN - Thị trường có dấu hiệu quá mua.",
                    "Xu hướng giảm (MA5 < MA20). Quá bán (RSI < 30). MACD tích cực (MACD > Signal). Đề xuất: THEO DÕI/MUA - Thị trường đang quá bán, có dấu hiệu đảo chiều.",
                    "Xu hướng giảm (MA5 < MA20). MACD tiêu cực (MACD < Signal). Đề xuất: BÁN/TRÁNH - Xu hướng giảm, MACD tiêu cực."
                ]
                symbol_df['reason'] = np.select(conditions, reasons, default="Xu hướng trung tính. Đề xuất: GIỮ - Chờ tín hiệu rõ ràng hơn.")
                
                # Thêm vào kết quả
                result_dfs.append(symbol_df)
        
        # Kết hợp tất cả kết quả
        if result_dfs:
            result_df = pd.concat(result_dfs)
            
            # Kiểm tra lại SparkContext trước khi tiếp tục
            if spark.sparkContext._jsc.sc().isStopped():
                print(f"SparkContext đã bị shutdown, không thể tiếp tục xử lý batch {batch_id}")
                global spark_active
                spark_active = False
                return
                
            # Chuyển Pandas DataFrame trở lại Spark DataFrame
            processed_df = spark.createDataFrame(result_df)
            
            # Chuẩn bị dữ liệu để ghi vào Kafka
            output_df = processed_df.select(
                col("symbol"),
                col("time"),
                col("date_str"),
                col("open"),
                col("high"),
                col("low"),
                col("close"),
                col("volume"),
                col("current_price"),
                col("ma5"),
                col("ma20"),
                col("rsi"),
                col("macd_line"),
                col("macd_signal"),
                col("macd_histogram"),
                col("suggestion"),
                col("reason")
            ).withColumn("value", to_json(struct(
                col("symbol"), col("time"), col("open"), col("high"), col("low"), 
                col("close"), col("volume"), col("current_price"),
                col("ma5"), col("ma20"), col("rsi"), 
                col("macd_line"), col("macd_signal"), col("macd_histogram"),
                col("suggestion"), col("reason")
            )))
            
            # Hiển thị một số dữ liệu mẫu để debug
            try:
                output_df.select("symbol", "time", "close", "ma5", "ma20", "rsi", "macd_line", "suggestion").show(5, False)
            except Exception as e:
                print(f"Không thể hiển thị dữ liệu mẫu: {e}")
            
            # Ghi dữ liệu vào Kafka
            try:
                output_df.selectExpr("symbol AS key", "value") \
                    .write \
                    .format("kafka") \
                    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
                    .option("topic", "stock-processed-topic") \
                    .save()
                
                print(f"Đã xử lý và ghi thành công {output_df.count()} records từ batch {batch_id}")
            except Exception as e:
                print(f"Lỗi khi ghi dữ liệu vào Kafka: {e}")
                
    except Exception as e:
        import traceback
        print(f"Lỗi trong process_small_batch {batch_id}: {e}")
        print(traceback.format_exc())

# Thiết lập trigger để xử lý theo từng batch với khoảng thời gian
query = exploded_df \
    .writeStream \
    .foreachBatch(process_batch) \
    .trigger(processingTime="10 minutes") \
    .outputMode("update") \
    .start()

print(f"Bắt đầu ghi dữ liệu đã xử lý vào Kafka topic: stock-processed-topic qua {kafka_bootstrap_servers}")
print("Đang chờ dữ liệu...")

# Thêm xử lý để đóng Spark gracefully khi cần
import signal

def handle_sigterm(sig, frame):
    """Xử lý tín hiệu SIGTERM để đóng Spark gracefully"""
    print("Nhận tín hiệu để dừng ứng dụng, đóng Spark gracefully...")
    global spark_active
    spark_active = False
    if query is not None and query.isActive:
        query.stop()
    if not spark.sparkContext._jsc.sc().isStopped():
        spark.stop()
    print("Đã dừng Spark thành công")
    
# Đăng ký xử lý tín hiệu
signal.signal(signal.SIGTERM, handle_sigterm)
signal.signal(signal.SIGINT, handle_sigterm)

# Chờ query kết thúc
try:
    query.awaitTermination()
except Exception as e:
    print(f"Lỗi trong quá trình streaming: {e}")
    # Thử restart query nếu có lỗi
    if not spark.sparkContext._jsc.sc().isStopped():
        print("Thử restart query...")
        spark_active = True
        query = exploded_df \
            .writeStream \
            .foreachBatch(process_batch) \
            .trigger(processingTime="10 minutes") \
            .outputMode("update") \
            .start()
        query.awaitTermination()
finally:
    # Đảm bảo dừng gracefully khi kết thúc
    if not spark.sparkContext._jsc.sc().isStopped():
        spark.stop()
    print("Ứng dụng kết thúc.") 
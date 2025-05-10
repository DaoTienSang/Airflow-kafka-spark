from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, from_json, explode, avg, lit, to_date, lag
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, DoubleType, TimestampType, IntegerType
import pandas as pd
import numpy as np

def calculate_ma(df, window_size):
    """Tính Moving Average"""
    window_spec = Window.partitionBy("symbol").orderBy("date").rowsBetween(-window_size+1, 0)
    return df.withColumn(f"MA_{window_size}", 
                         avg("close").over(window_spec))

def calculate_rsi(df, window_size=14):
    """Tính RSI (Relative Strength Index)"""
    # Tính price change bằng lag function
    window_spec = Window.partitionBy("symbol").orderBy("date")
    
    # Sử dụng lag để lấy giá đóng cửa của phiên trước đó
    df = df.withColumn("prev_close", lag("close", 1).over(window_spec))
    df = df.withColumn("price_change", col("close") - col("prev_close"))
    
    # Tính gain và loss
    df = df.withColumn("gain", expr("CASE WHEN price_change > 0 THEN price_change ELSE 0 END"))
    df = df.withColumn("loss", expr("CASE WHEN price_change < 0 THEN ABS(price_change) ELSE 0 END"))
    
    # Tính average gain và average loss
    df = df.withColumn("avg_gain", 
                       avg("gain").over(window_spec.rowsBetween(-window_size+1, 0)))
    df = df.withColumn("avg_loss", 
                       avg("loss").over(window_spec.rowsBetween(-window_size+1, 0)))
    
    # Tính RS và RSI
    df = df.withColumn("rs", 
                       expr("CASE WHEN avg_loss = 0 THEN 100 ELSE avg_gain/avg_loss END"))
    df = df.withColumn("rsi", 
                       expr("CASE WHEN avg_loss = 0 THEN 100 ELSE 100 - (100 / (1 + rs)) END"))
    
    return df

def calculate_macd(df, fast=12, slow=26, signal=9):
    """Tính MACD (Moving Average Convergence Divergence)"""
    # Giải pháp thay thế vì Spark không có sẵn EMA
    # Sử dụng MA (Moving Average) thay thế
    
    # Calculate MAs thay cho EMAs
    window_spec_fast = Window.partitionBy("symbol").orderBy("date").rowsBetween(-fast+1, 0)
    fast_ma = df.withColumn(f"ma_{fast}", 
                       avg("close").over(window_spec_fast))
    
    window_spec_slow = Window.partitionBy("symbol").orderBy("date").rowsBetween(-slow+1, 0)
    slow_ma = df.withColumn(f"ma_{slow}", 
                       avg("close").over(window_spec_slow))
    
    # Join các DataFrame
    df = fast_ma.join(slow_ma.select("symbol", "date", f"ma_{slow}"), ["symbol", "date"])
    
    # Calculate MACD line
    df = df.withColumn("macd_line", col(f"ma_{fast}") - col(f"ma_{slow}"))
    
    # Calculate signal line (9-day MA of MACD line)
    window_spec = Window.partitionBy("symbol").orderBy("date").rowsBetween(-signal+1, 0)
    df = df.withColumn("signal_line", 
                        avg("macd_line").over(window_spec))
    
    # Calculate MACD histogram
    df = df.withColumn("macd_histogram", col("macd_line") - col("signal_line"))
    
    return df

def process_batch(df, epoch_id):
    """Xử lý từng batch dữ liệu"""
    if df.count() > 0:
        # In dữ liệu thô để kiểm tra
        print(f"Batch {epoch_id} - Dữ liệu thô:")
        df.show(10, truncate=False)
        
        # Nhóm dữ liệu theo symbol và sắp xếp theo date để chuẩn bị cho việc tính toán
        df = df.orderBy("symbol", "date")
        
        # Chỉ tính các chỉ số kỹ thuật nếu có đủ dữ liệu
        # Phân tích số lượng dữ liệu theo từng mã cổ phiếu
        symbol_counts = df.groupBy("symbol").count()
        print("Số lượng dữ liệu theo mã:")
        symbol_counts.show()
        
        # Tính Moving Averages nếu có đủ dữ liệu
        try:
            df = calculate_ma(df, 5)   # 5-day MA
            df = calculate_ma(df, 20)  # 20-day MA
            
            # Tính RSI
            df = calculate_rsi(df)
            
            # Tính MACD
            df = calculate_macd(df)
            
            # In kết quả sau xử lý
            print(f"Batch {epoch_id} - Sau khi tính toán:")
            df.show(20, truncate=False)
        except Exception as e:
            print(f"Lỗi khi tính toán chỉ số: {e}")
            print("Hiển thị dữ liệu đơn giản:")
            df.show(20, truncate=False)

if __name__ == "__main__":
    # Tạo Spark session
    spark = SparkSession.builder \
        .appName("Stock Technical Analysis") \
        .config("spark.sql.session.timeZone", "UTC") \
        .getOrCreate()
    
    # Định nghĩa schema cho dữ liệu historical
    historical_schema = ArrayType(
        StructType([
            StructField("time", StringType(), True),  # Thay đổi từ "date" thành "time" để khớp với dữ liệu
            StructField("open", DoubleType(), True), 
            StructField("high", DoubleType(), True),
            StructField("low", DoubleType(), True),
            StructField("close", DoubleType(), True),
            StructField("volume", DoubleType(), True),
            StructField("ticker", StringType(), True)
        ])
    )
    
    # Định nghĩa schema cho message từ Kafka
    kafka_schema = StructType([
        StructField("symbol", StringType(), True),
        StructField("current_price", DoubleType(), True),
        StructField("historical_data", historical_schema, True)
    ])
    
    # Đọc dữ liệu từ Kafka
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "stock-history-topic") \
        .option("startingOffsets", "earliest") \
        .load()
    
    # Parse dữ liệu JSON từ Kafka
    parsed_df = df.select(
        from_json(col("value").cast("string"), kafka_schema).alias("data")
    ).select("data.*")
    
    # In ra schema để debug
    print("Schema sau khi parse JSON:")
    parsed_df.printSchema()
    
    # Làm phẳng mảng historical_data
    flattened_df = parsed_df.select(
        col("symbol"),
        col("current_price"),
        explode("historical_data").alias("hist_data")
    ).select(
        col("symbol"),
        col("current_price"),
        col("hist_data.time").alias("date_str"),  # Tham chiếu đến trường time thay vì date
        col("hist_data.open").alias("open"),
        col("hist_data.high").alias("high"),
        col("hist_data.low").alias("low"),
        col("hist_data.close").alias("close"),
        col("hist_data.volume").alias("volume")
    )
    
    # In ra schema sau khi flatten
    print("Schema sau khi flatten:")
    flattened_df.printSchema()
    
    # Tạo cột date từ date_str
    flattened_df = flattened_df.withColumn("date", to_date(col("date_str"), "yyyy-MM-dd"))
    
    # Xử lý từng batch sử dụng foreachBatch thay vì xử lý trực tiếp trên streaming DataFrame
    query = flattened_df \
        .writeStream \
        .foreachBatch(process_batch) \
        .outputMode("update") \
        .start()
    
    query.awaitTermination() 
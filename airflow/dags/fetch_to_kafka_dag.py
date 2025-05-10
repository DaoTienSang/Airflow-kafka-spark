from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from kafka import KafkaProducer
from vnstock import Vnstock
import json
import pandas as pd
import numpy as np
import time

# Class giúp xử lý numpy và pandas datatype trong json
class NpEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        if isinstance(obj, np.floating):
            return float(obj)
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        if isinstance(obj, pd.Timestamp):
            return obj.strftime('%Y-%m-%d %H:%M:%S')
        return super(NpEncoder, self).default(obj)

def fetch_and_push_to_kafka():
    vnstock_instance = Vnstock()
    stock = vnstock_instance.stock(symbol='VN30', source='VCI')
    symbols_df = stock.listing.all_symbols()
    symbols = symbols_df['symbol'].tolist()
    price_board = stock.trading.price_board(symbols_list=symbols)

    if isinstance(price_board.columns, pd.MultiIndex):
        price_board.columns = ['_'.join(map(str, col)).strip() for col in price_board.columns.values]

    snapshot = {'time': datetime.now().isoformat()}
    for symbol in symbols:
        try:
            row = price_board[price_board['listing_symbol'] == symbol]
            snapshot[symbol] = row['match_match_price'].values[0] if not row.empty else None
        except Exception:
            snapshot[symbol] = None

    producer = KafkaProducer(
        bootstrap_servers='kafka:9092',
        value_serializer=lambda v: json.dumps(v, cls=NpEncoder).encode('utf-8')
    )

    # Gửi dữ liệu giá hiện tại vào Kafka
    producer.send('stock-topic', snapshot)
    print("✅ Đã đẩy dữ liệu giá hiện tại vào Kafka topic `stock-topic`")
    
    # Lấy và gửi dữ liệu lịch sử vào Kafka
    # Lấy nhiều mã cổ phiếu hơn theo yêu cầu
    sample_symbols = symbols[:100]  # Lấy 100 mã đầu tiên
    
    for symbol in sample_symbols:
        try:
            print(f"🔄 Đang lấy dữ liệu lịch sử cho {symbol}...")
            # Lấy dữ liệu lịch sử từ 2000 đến hiện tại
            historical_data = stock.quote.history(
                symbol=symbol,
                start='2020-01-01', 
                end=datetime.now().strftime('%Y-%m-%d'),
                interval='1D'
            )
            
            # Lấy giá hiện tại của cổ phiếu
            current_price = snapshot.get(symbol)
            
            # Chuyển thành dictionary để serialize - xử lý đúng kiểu dữ liệu
            if not historical_data.empty:
                # Convert DataFrame to dict with Python native types 
                records = []
                for _, row in historical_data.iterrows():
                    record = {}
                    for col, val in row.items():
                        if isinstance(val, (np.integer, np.int64, np.int32)): 
                            record[col] = int(val)
                        elif isinstance(val, (np.floating, np.float64, np.float32)):
                            record[col] = float(val)
                        elif isinstance(val, pd.Timestamp):
                            record[col] = val.strftime('%Y-%m-%d')
                        else:
                            record[col] = val
                    records.append(record)
                
                historical_dict = {
                    'symbol': symbol,
                    'current_price': current_price,
                    'historical_data': records
                }
                
                # Gửi vào Kafka
                producer.send('stock-history-topic', historical_dict)
                print(f"✅ Đã đẩy dữ liệu lịch sử cho {symbol} vào Kafka topic `stock-history-topic`")
                
                # Thêm delay để tránh giới hạn API - tăng thời gian chờ để xử lý nhiều mã
                time.sleep(10)  # chờ 10 giây giữa các lần gọi API
            
        except Exception as e:
            print(f"❌ Lỗi khi lấy dữ liệu lịch sử cho {symbol}: {str(e)}")
            # Nếu gặp lỗi giới hạn API, chờ thêm để tránh bị block
            if "quá nhiều request" in str(e).lower():
                print("⏱ Đang chờ 30 giây để tránh giới hạn API...")
                time.sleep(30)
    
    producer.flush()
    print("✅ Hoàn thành việc đẩy dữ liệu vào Kafka")

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    'start_date': datetime(2025, 4, 17),
}

with DAG(
    dag_id='fetch_stock_to_kafka',
    default_args=default_args,
    schedule_interval='*/10 * * * *',
    catchup=False,
    tags=['stock', 'vnstock', 'kafka'],
) as dag:

    task = PythonOperator(
        task_id='fetch_push_kafka',
        python_callable=fetch_and_push_to_kafka
    )

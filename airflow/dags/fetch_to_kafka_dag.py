from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from kafka import KafkaProducer
from vnstock import Vnstock
import json
import pandas as pd
import numpy as np

def convert_numpy(obj):
    if isinstance(obj, np.generic):
        return obj.item()
    raise TypeError(f"Type {type(obj)} not serializable")

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
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    clean_snapshot = json.loads(json.dumps(snapshot, default=convert_numpy))
    producer.send('stock-topic', clean_snapshot)
    producer.flush()
    print("✅ Đã đẩy dữ liệu vào Kafka topic `stock-topic`")

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    'start_date': datetime(2025, 4, 17),
}

with DAG(
    dag_id='fetch_stock_to_kafka',
    default_args=default_args,
    schedule_interval='*/3 * * * *',
    catchup=False,
    tags=['stock', 'vnstock', 'kafka'],
) as dag:

    task = PythonOperator(
        task_id='fetch_push_kafka',
        python_callable=fetch_and_push_to_kafka
    )

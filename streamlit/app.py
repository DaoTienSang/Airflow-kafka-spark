import streamlit as st
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from kafka import KafkaConsumer
import json
from datetime import datetime
import time

# Cấu hình trang
st.set_page_config(
    page_title="Stock Market Analysis Dashboard",
    page_icon="📈",
    layout="wide"
)

# Thiết lập style cho seaborn
sns.set(style="darkgrid")
plt.rcParams.update({'font.size': 10})

# Hàm để kết nối và lấy dữ liệu từ Kafka
@st.cache_resource
def get_kafka_consumer():
    consumer = KafkaConsumer(
        'stock-processed-topic',  # Đọc từ topic đã xử lý
        bootstrap_servers=['kafka:9092'],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='streamlit-consumer',
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        # Tăng thời gian chờ và số lượng records lấy mỗi lần poll
        fetch_max_bytes=52428800,  # Tăng kích thước fetch lên 50MB
        max_partition_fetch_bytes=10485760  # Tăng lên 10MB mỗi partition
    )
    return consumer

# Hàm để xử lý dữ liệu từ Kafka và chuyển thành DataFrame
def process_kafka_data(max_messages=10000, timeout=300):
    consumer = get_kafka_consumer()
    
    messages = []
    start_time = time.time()
    unique_symbols = set()
    
    # Kiểm tra số lượng symbols duy nhất để đảm bảo đọc đủ 100 mã
    target_symbols = 100  # Mục tiêu là có 100 mã cổ phiếu
    
    st.info("Đang tải dữ liệu từ Kafka...vui lòng chờ (~2-3 phút)")
    progress_bar = st.progress(0)
    
    # Poll cho đến khi có đủ 100 mã hoặc hết thời gian chờ
    while len(unique_symbols) < target_symbols and time.time() - start_time < timeout and len(messages) < max_messages:
        msg_pack = consumer.poll(timeout_ms=10000, max_records=1000)
        
        if not msg_pack:
            # Nếu không có messages mới, chờ một chút và thử lại
            time.sleep(1)
            continue
            
        for _, msgs in msg_pack.items():
            for msg in msgs:
                messages.append(msg.value)
                symbol = msg.value.get('symbol')
                if symbol:
                    unique_symbols.add(symbol)
                
                # Cập nhật progress bar
                if len(unique_symbols) > 0:
                    progress_percent = min(len(unique_symbols) / target_symbols, 1.0)
                    progress_bar.progress(progress_percent)
                    
                if len(messages) >= max_messages:
                    break
        
        # Hiển thị số lượng symbol đã nhận được
        st.info(f"Đã nhận dữ liệu của {len(unique_symbols)}/{target_symbols} mã cổ phiếu. Tiếp tục chờ...")
    
    progress_bar.empty()
    
    if not messages:
        st.error("Không nhận được dữ liệu từ Kafka")
        return None, None
    
    # Hiển thị thông tin số lượng mã cổ phiếu nhận được
    st.success(f"Đã nhận dữ liệu của {len(unique_symbols)} mã cổ phiếu: {', '.join(sorted(list(unique_symbols)))}")
    
    # Xử lý dữ liệu từ Kafka
    stock_data = {}
    
    for msg in messages:
        symbol = msg.get('symbol')
        
        if symbol:
            if symbol not in stock_data:
                stock_data[symbol] = []
            
            # Thêm dữ liệu đã xử lý vào dict
            stock_data[symbol].append({
                'date': msg.get('time'),
                'open': msg.get('open'),
                'high': msg.get('high'),
                'low': msg.get('low'),
                'close': msg.get('close'),
                'volume': msg.get('volume'),
                'current_price': msg.get('current_price'),
                'ma5': msg.get('ma5'),
                'ma20': msg.get('ma20'),
                'rsi': msg.get('rsi'),
                'macd_line': msg.get('macd_line'),
                'macd_signal': msg.get('macd_signal'),
                'macd_histogram': msg.get('macd_histogram'),
                'suggestion': msg.get('suggestion'),
                'reason': msg.get('reason')
            })
    
    # Chuyển đổi thành DataFrame
    dataframes = {}
    current_prices = {}
    for symbol, data in stock_data.items():
        if data:
            df = pd.DataFrame(data)
            df['date'] = pd.to_datetime(df['date'])
            df = df.sort_values('date')
            
            # Lưu giá hiện tại
            if not df.empty and 'current_price' in df.columns:
                current_prices[symbol] = df['current_price'].iloc[-1]
            
            dataframes[symbol] = df
    
    return dataframes, current_prices

# Hiển thị biểu đồ bằng seaborn
def plot_seaborn_charts(df, symbol):
    # Đảm bảo cột date có định dạng datetime đúng
    df = df.copy()
    if 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date'])
    
    # Tạo figure với kích thước lớn
    fig, axes = plt.subplots(4, 1, figsize=(14, 16), gridspec_kw={'height_ratios': [3, 1, 1, 1]})
    
    # 1. Biểu đồ giá và MA
    ax1 = axes[0]
    # Vẽ biểu đồ giá đóng cửa
    sns.lineplot(x=df['date'].values, y='close', data=df, color='black', label='Giá đóng cửa', ax=ax1)
    
    # Vẽ MA5 và MA20
    if 'ma5' in df.columns and df['ma5'].notna().any():
        sns.lineplot(x=df['date'].values, y='ma5', data=df, color='blue', label='MA5', ax=ax1)
    
    if 'ma20' in df.columns and df['ma20'].notna().any():
        sns.lineplot(x=df['date'].values, y='ma20', data=df, color='red', label='MA20', ax=ax1)
    
    ax1.set_title(f"{symbol} - Biểu đồ giá", fontsize=15)
    ax1.set_ylabel('Giá', fontsize=12)
    ax1.tick_params(axis='x', rotation=30)
    ax1.legend()
    
    # 2. Biểu đồ Volume
    ax2 = axes[1]
    # Dùng lineplot thay vì barplot cho volume theo yêu cầu
    sns.lineplot(x=df['date'].values, y='volume', data=df, color='blue', label='Volume', ax=ax2)
    ax2.set_title('Volume', fontsize=12)
    ax2.set_ylabel('Volume', fontsize=10)
    ax2.tick_params(axis='x', rotation=45, labelsize=8)
    ax2.fill_between(df['date'].values, df['volume'], alpha=0.2, color='blue')
    # Hiển thị grid để dễ đọc giá trị
    ax2.grid(True, alpha=0.3)
    
    # 3. Biểu đồ MACD
    ax3 = axes[2]
    if all(col in df.columns for col in ['macd_line', 'macd_signal']) and df['macd_line'].notna().any():
        sns.lineplot(x=df['date'].values, y='macd_line', data=df, color='blue', label='MACD Line', ax=ax3)
        sns.lineplot(x=df['date'].values, y='macd_signal', data=df, color='red', label='Signal Line', ax=ax3)
        
        # Vẽ histogram bằng cách dùng lineplot và fill_between
        if 'macd_histogram' in df.columns and df['macd_histogram'].notna().any():
            # Tạo baseline ở y=0
            ax3.axhline(y=0, color='gray', linestyle='--', alpha=0.5)
            
            # Vẽ đường histogram
            sns.lineplot(x=df['date'].values, y='macd_histogram', data=df, color='green', alpha=0.5, ax=ax3, label='Histogram')
            
            # Tô màu vùng trên và dưới 0
            positive_hist = df.copy()
            negative_hist = df.copy()
            positive_hist.loc[positive_hist['macd_histogram'] <= 0, 'macd_histogram'] = 0
            negative_hist.loc[negative_hist['macd_histogram'] >= 0, 'macd_histogram'] = 0
            
            # Tô màu vùng dương
            ax3.fill_between(positive_hist['date'].values, 0, positive_hist['macd_histogram'], 
                           alpha=0.3, color='green', label='Positive')
            
            # Tô màu vùng âm
            ax3.fill_between(negative_hist['date'].values, 0, negative_hist['macd_histogram'], 
                           alpha=0.3, color='red', label='Negative')
    
    ax3.set_title('MACD', fontsize=12)
    ax3.set_ylabel('Value', fontsize=10)
    ax3.tick_params(axis='x', rotation=45, labelsize=8)
    ax3.legend()
    ax3.grid(True, alpha=0.3)
    
    # 4. Biểu đồ RSI
    ax4 = axes[3]
    if 'rsi' in df.columns and df['rsi'].notna().any():
        sns.lineplot(x=df['date'].values, y='rsi', data=df, color='purple', label='RSI', ax=ax4)
        
        # Thêm đường tham chiếu 70 và 30
        ax4.axhline(y=70, color='red', linestyle='--', alpha=0.7)
        ax4.axhline(y=30, color='green', linestyle='--', alpha=0.7)
        
        # Tô màu vùng quá mua và quá bán
        ax4.fill_between(df['date'].values, 70, 100, alpha=0.1, color='red', label='Overbought')
        ax4.fill_between(df['date'].values, 0, 30, alpha=0.1, color='green', label='Oversold')
    
    ax4.set_title('RSI', fontsize=12)
    ax4.set_ylabel('RSI', fontsize=10)
    ax4.tick_params(axis='x', rotation=45, labelsize=8)
    ax4.set_ylim(0, 100)  # RSI từ 0-100
    ax4.grid(True, alpha=0.3)
    ax4.legend()
    
    # Điều chỉnh không gian giữa các subplots
    plt.tight_layout()
    
    return fig

# Thêm hàm vẽ biểu đồ dự phòng sử dụng matplotlib trực tiếp
def plot_matplotlib_charts(df, symbol):
    if df.empty:
        st.error("Không có dữ liệu để vẽ biểu đồ")
        return None

    # Đảm bảo cột date có định dạng datetime đúng
    df = df.copy()
    if 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date'])
    
    # Tạo figure với kích thước lớn
    fig, axes = plt.subplots(4, 1, figsize=(14, 16), gridspec_kw={'height_ratios': [3, 1, 1, 1]})
    
    # 1. Biểu đồ giá và MA
    ax1 = axes[0]
    ax1.plot(df['date'], df['close'], color='black', label='Giá đóng cửa')
    
    # Vẽ MA5 và MA20
    if 'ma5' in df.columns and df['ma5'].notna().any():
        ax1.plot(df['date'], df['ma5'], color='blue', label='MA5')
    
    if 'ma20' in df.columns and df['ma20'].notna().any():
        ax1.plot(df['date'], df['ma20'], color='red', label='MA20')
    
    ax1.set_title(f"{symbol} - Biểu đồ giá", fontsize=15)
    ax1.set_ylabel('Giá', fontsize=12)
    ax1.tick_params(axis='x', rotation=30)
    ax1.legend()
    
    # 2. Biểu đồ Volume
    ax2 = axes[1]
    ax2.plot(df['date'], df['volume'], color='blue', label='Volume')
    ax2.fill_between(df['date'], df['volume'], alpha=0.2, color='blue')
    ax2.set_title('Volume', fontsize=12)
    ax2.set_ylabel('Volume', fontsize=10)
    ax2.tick_params(axis='x', rotation=45, labelsize=8)
    ax2.grid(True, alpha=0.3)
    
    # 3. Biểu đồ MACD
    ax3 = axes[2]
    if all(col in df.columns for col in ['macd_line', 'macd_signal']) and df['macd_line'].notna().any():
        ax3.plot(df['date'], df['macd_line'], color='blue', label='MACD Line')
        ax3.plot(df['date'], df['macd_signal'], color='red', label='Signal Line')
        
        # Vẽ histogram bằng cách dùng fill_between
        if 'macd_histogram' in df.columns and df['macd_histogram'].notna().any():
            # Tạo baseline ở y=0
            ax3.axhline(y=0, color='gray', linestyle='--', alpha=0.5)
            
            # Tô màu vùng trên và dưới 0
            positive_mask = df['macd_histogram'] > 0
            negative_mask = df['macd_histogram'] <= 0
            
            # Tô màu vùng dương và âm
            ax3.fill_between(df.loc[positive_mask, 'date'], 0, df.loc[positive_mask, 'macd_histogram'], 
                           alpha=0.3, color='green', label='Positive')
            ax3.fill_between(df.loc[negative_mask, 'date'], 0, df.loc[negative_mask, 'macd_histogram'], 
                           alpha=0.3, color='red', label='Negative')
    
    ax3.set_title('MACD', fontsize=12)
    ax3.set_ylabel('Value', fontsize=10)
    ax3.tick_params(axis='x', rotation=45, labelsize=8)
    ax3.legend()
    ax3.grid(True, alpha=0.3)
    
    # 4. Biểu đồ RSI
    ax4 = axes[3]
    if 'rsi' in df.columns and df['rsi'].notna().any():
        ax4.plot(df['date'], df['rsi'], color='purple', label='RSI')
        
        # Thêm đường tham chiếu 70 và 30
        ax4.axhline(y=70, color='red', linestyle='--', alpha=0.7)
        ax4.axhline(y=30, color='green', linestyle='--', alpha=0.7)
        
        # Tô màu vùng quá mua và quá bán
        ax4.fill_between(df['date'], 70, 100, alpha=0.1, color='red', label='Overbought')
        ax4.fill_between(df['date'], 0, 30, alpha=0.1, color='green', label='Oversold')
    
    ax4.set_title('RSI', fontsize=12)
    ax4.set_ylabel('RSI', fontsize=10)
    ax4.tick_params(axis='x', rotation=45, labelsize=8)
    ax4.set_ylim(0, 100)  # RSI từ 0-100
    ax4.grid(True, alpha=0.3)
    ax4.legend()
    
    # Điều chỉnh không gian giữa các subplots
    plt.tight_layout()
    
    return fig

# Giao diện chính
st.title("📊 Stock Market Technical Analysis Dashboard")

# Hiển thị loading spinner
with st.spinner("Đang tải dữ liệu..."):
    stock_dataframes, current_prices = process_kafka_data()

if stock_dataframes:
    # Sidebar để chọn mã cổ phiếu
    stocks = list(stock_dataframes.keys())
    selected_stock = st.sidebar.selectbox("Chọn mã cổ phiếu", stocks)
    
    if selected_stock and selected_stock in stock_dataframes:
        st.header(f"{selected_stock} - Phân tích kỹ thuật")
        
        # Hiển thị giá hiện tại
        if selected_stock in current_prices:
            st.metric(
                label="Giá hiện tại", 
                value=f"{current_prices[selected_stock]:,.2f} VND"
            )
        
        # Lấy DataFrame cho cổ phiếu đã chọn
        df = stock_dataframes[selected_stock]
        
        # Thử vẽ biểu đồ bằng seaborn, nếu lỗi thì dùng matplotlib
        try:
            seaborn_chart = plot_seaborn_charts(df, selected_stock)
            st.pyplot(seaborn_chart)
        except Exception as e:
            st.warning(f"Không thể vẽ biểu đồ bằng Seaborn: {str(e)}")
            try:
                matplotlib_chart = plot_matplotlib_charts(df, selected_stock)
                st.pyplot(matplotlib_chart)
            except Exception as e2:
                st.error(f"Không thể vẽ biểu đồ: {str(e2)}")
        
        # Thêm thông tin tóm tắt
        with st.expander("Chi tiết dữ liệu"):
            st.dataframe(df)
        
        # Lấy dữ liệu mới nhất
        if not df.empty:
            last_row = df.iloc[-1]
            
            # Hiển thị đề xuất trong expander
            with st.expander("Đề xuất đầu tư", expanded=True):
                suggestion = last_row.get('suggestion', 'KHÔNG CÓ ĐỀ XUẤT')
                reason = last_row.get('reason', 'Không có đủ dữ liệu')
                
                # Đặt màu cho đề xuất
                if 'BUY' in suggestion:
                    suggestion_color = 'green'
                elif 'SELL' in suggestion:
                    suggestion_color = 'red'
                else:
                    suggestion_color = 'orange'
                    
                # Hiển thị đề xuất
                st.markdown(f"<h3 style='color: {suggestion_color};'>{suggestion}</h3>", unsafe_allow_html=True)
                st.write(reason)
                
                # Hiển thị các chỉ số kỹ thuật mới nhất
                col1, col2, col3, col4 = st.columns(4)
                
                with col1:
                    ma5_value = last_row.get('ma5', 'N/A')
                    if ma5_value != 'N/A':
                        st.metric(label="MA5", value=f"{ma5_value:.2f}")
                    else:
                        st.metric(label="MA5", value="N/A")
                
                with col2:
                    ma20_value = last_row.get('ma20', 'N/A')
                    if ma20_value != 'N/A':
                        st.metric(label="MA20", value=f"{ma20_value:.2f}")
                    else:
                        st.metric(label="MA20", value="N/A")
                
                with col3:
                    rsi_value = last_row.get('rsi', 'N/A')
                    if rsi_value != 'N/A':
                        if rsi_value > 70:
                            st.metric(label="RSI (Quá mua > 70)", value=f"{rsi_value:.2f}")
                        elif rsi_value < 30:
                            st.metric(label="RSI (Quá bán < 30)", value=f"{rsi_value:.2f}")
                        else:
                            st.metric(label="RSI", value=f"{rsi_value:.2f}")
                    else:
                        st.metric(label="RSI", value="N/A")
                
                with col4:
                    macd = last_row.get('macd_line', 'N/A')
                    signal = last_row.get('macd_signal', 'N/A')
                    
                    if macd != 'N/A' and signal != 'N/A':
                        diff = macd - signal
                        # Sửa lỗi delta_color, chỉ dùng các giá trị hợp lệ: 'normal', 'inverse' hoặc 'off'
                        delta_color = "normal" if diff > 0 else "inverse"
                        st.metric(
                            label="MACD", 
                            value=f"{macd:.4f}", 
                            delta=f"{diff:.4f}", 
                            delta_color=delta_color
                        )
                    else:
                        st.metric(label="MACD", value="N/A")
    else:
        st.warning("Vui lòng chọn một mã cổ phiếu từ sidebar.")
else:
    st.error("Không thể kết nối đến Kafka hoặc không có dữ liệu. Vui lòng đảm bảo các services đang chạy và có dữ liệu trong topic.")
    
    # Thêm button để thử lại
    if st.button("Thử lại"):
        st.experimental_rerun()

# Hiển thị thông tin ứng dụng
st.sidebar.markdown("---")
st.sidebar.info(
    """
    **Về ứng dụng**  
    Dashboard trực quan hóa dữ liệu cổ phiếu và phân tích kỹ thuật.
    """
) 
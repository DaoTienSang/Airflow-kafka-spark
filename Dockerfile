# Dockerfile đa chức năng cho Stock Pipeline
# Kết hợp Airflow, Spark và Streamlit trong một Dockerfile

# ==================== BASE IMAGE ====================
FROM python:3.9-slim AS base

# Cài đặt các dependencies cơ bản
RUN apt-get update && apt-get install -y --no-install-recommends \
    wget \
    curl \
    build-essential \
    openjdk-17-jdk \
    git \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Thiết lập JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# ==================== AIRFLOW ====================
FROM base AS airflow

# Thiết lập biến môi trường Airflow
ENV AIRFLOW_HOME=/opt/airflow
ENV PYTHONPATH=${PYTHONPATH}:${AIRFLOW_HOME}

# Cài đặt Airflow và các dependencies
RUN pip install --no-cache-dir apache-airflow==2.7.3 kafka-python==2.0.2 vnstock pandas

# Tạo thư mục cho DAGs và plugins
RUN mkdir -p ${AIRFLOW_HOME}/dags ${AIRFLOW_HOME}/plugins

# ==================== SPARK ====================
FROM base AS spark

# Thiết lập Spark
ENV SPARK_VERSION=3.5.0
ENV HADOOP_VERSION=3
ENV SPARK_HOME=/opt/spark

# Sử dụng bitnami mirror thay vì Apache download
RUN mkdir -p ${SPARK_HOME} && \
    curl -L -o spark.tgz "https://dlcdn.apache.org/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" && \
    tar -xf spark.tgz -C /tmp && \
    mv /tmp/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}/* ${SPARK_HOME}/ && \
    rm spark.tgz

# Thiết lập PATH
ENV PATH=${PATH}:${SPARK_HOME}/bin:${SPARK_HOME}/sbin

# Cài đặt các dependencies Python
RUN pip install --no-cache-dir kafka-python==2.0.2 numpy pandas pyarrow==14.0.1 finplot

# Thiết lập tham số Spark
ENV PYSPARK_SUBMIT_ARGS="--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 pyspark-shell"
ENV PYTHONUNBUFFERED=1
ENV JAVA_OPTS="-Xmx4g -XX:+UseG1GC"

# ==================== STREAMLIT ====================
FROM base AS streamlit

WORKDIR /app

# Cài đặt các dependencies Streamlit
RUN pip install --no-cache-dir streamlit==1.29.0 pandas==2.0.3 numpy==1.24.3 matplotlib==3.7.2 plotly==5.18.0 kafka-python==2.0.2 seaborn==0.12.2

# ==================== FINAL IMAGE ====================
FROM base AS final

# Cài đặt tất cả dependencies
COPY --from=airflow /usr/local/lib/python3.9/site-packages /usr/local/lib/python3.9/site-packages
COPY --from=spark /usr/local/lib/python3.9/site-packages /usr/local/lib/python3.9/site-packages
COPY --from=streamlit /usr/local/lib/python3.9/site-packages /usr/local/lib/python3.9/site-packages

# Sao chép Spark từ Spark stage
COPY --from=spark /opt/spark /opt/spark

# Sao chép Airflow từ Airflow stage
COPY --from=airflow /opt/airflow /opt/airflow

# Tạo các thư mục cần thiết
RUN mkdir -p /app /opt/app

# Thiết lập các biến môi trường
ENV AIRFLOW_HOME=/opt/airflow
ENV SPARK_HOME=/opt/spark
ENV PYTHONPATH=${PYTHONPATH}:${AIRFLOW_HOME}:${SPARK_HOME}/python
ENV PATH=${PATH}:${SPARK_HOME}/bin:${SPARK_HOME}/sbin
ENV JAVA_OPTS="-Xmx4g -XX:+UseG1GC"
ENV PYSPARK_SUBMIT_ARGS="--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 pyspark-shell"

# Tạo thư mục làm việc
WORKDIR /workspace

# Sao chép code vào container
# Các file sẽ được mount từ host thông qua volumes trong docker-compose

# Tạo script khởi động
RUN echo '#!/bin/bash\n\
\n\
# Chức năng cần chạy từ tham số dòng lệnh\n\
# Ví dụ: docker run -it my-image airflow\n\
service=$1\n\
\n\
case $service in\n\
  airflow)\n\
    echo "Starting Airflow..."\n\
    cd /opt/airflow\n\
    airflow standalone\n\
    ;;\n\
  spark)\n\
    echo "Starting Spark application..."\n\
    cd /opt/app\n\
    spark-submit --master local[*] --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 stock_analysis.py\n\
    ;;\n\
  streamlit)\n\
    echo "Starting Streamlit application..."\n\
    cd /app\n\
    streamlit run app.py --server.port=8501 --server.address=0.0.0.0\n\
    ;;\n\
  *)\n\
    echo "Please specify which service to start: airflow, spark, or streamlit"\n\
    exit 1\n\
    ;;\n\
esac' > /workspace/start.sh

# Đảm bảo script có quyền thực thi
RUN chmod +x /workspace/start.sh

# Mở các cổng cần thiết
EXPOSE 8080 8501 4040

# Mục nhập mặc định
ENTRYPOINT ["/workspace/start.sh"]

# Mặc định là hiển thị trợ giúp
CMD ["help"] 
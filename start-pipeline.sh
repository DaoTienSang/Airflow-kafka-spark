#!/bin/bash

# Script khởi động Stock Pipeline

echo -e "\033[1;36m======== STOCK PIPELINE STARTER ========\033[0m"
echo -e "\033[1;36mKhởi động hệ thống Stock Pipeline...\033[0m"
echo ""

# Kiểm tra xem Docker đã chạy chưa
echo -e "\033[1;33mKiểm tra Docker...\033[0m"
if ! docker ps > /dev/null 2>&1; then
    echo -e "\033[1;31mDocker chưa được khởi động! Vui lòng khởi động Docker trước.\033[0m"
    exit 1
fi
echo -e "\033[1;32m✓ Docker đang chạy\033[0m"

# Build và khởi động các container
echo ""
echo -e "\033[1;33mĐang dựng Docker image...\033[0m"
docker-compose down --remove-orphans
docker-compose build --no-cache stock-image

echo ""
echo -e "\033[1;33mKhởi động các dịch vụ...\033[0m"
docker-compose up -d

echo ""
echo -e "\033[1;33mĐợi các dịch vụ khởi động...\033[0m"
sleep 15

# Hiển thị trạng thái
echo ""
echo -e "\033[1;33mTrạng thái các container:\033[0m"
docker-compose ps

# Hiển thị hướng dẫn
echo ""
echo -e "\033[1;36m======== SERVICES ========\033[0m"
echo -e "\033[1;32mAirflow UI: http://localhost:8080 (username: admin, password: admin)\033[0m"
echo -e "\033[1;32mStreamlit Dashboard: http://localhost:8501\033[0m"
echo -e "\033[1;32mKafka UI: http://localhost:8180\033[0m"
echo -e "\033[1;32mSpark UI: http://localhost:4040 (khi Spark đang xử lý)\033[0m"
echo ""
echo -e "\033[1;33mĐể xem logs:\033[0m"
echo -e "docker-compose logs -f [service_name]"
echo "" 
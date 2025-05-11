# Script khởi động Stock Pipeline

Write-Host "======== STOCK PIPELINE STARTER ========" -ForegroundColor Cyan
Write-Host "Khởi động hệ thống Stock Pipeline..." -ForegroundColor Cyan
Write-Host ""

# Kiểm tra xem Docker đã chạy chưa
Write-Host "Kiểm tra Docker..." -ForegroundColor Yellow
$dockerStatus = docker ps 2>&1
if ($LASTEXITCODE -ne 0) {
    Write-Host "Docker chưa được khởi động! Vui lòng khởi động Docker trước." -ForegroundColor Red
    exit 1
}
Write-Host "✓ Docker đang chạy" -ForegroundColor Green

# Build và khởi động các container
Write-Host ""
Write-Host "Đang dựng Docker image..." -ForegroundColor Yellow
docker-compose down --remove-orphans
docker-compose build --no-cache stock-image

Write-Host ""
Write-Host "Khởi động các dịch vụ..." -ForegroundColor Yellow
docker-compose up -d

Write-Host ""
Write-Host "Đợi các dịch vụ khởi động..." -ForegroundColor Yellow
Start-Sleep -Seconds 15

# Hiển thị trạng thái
Write-Host ""
Write-Host "Trạng thái các container:" -ForegroundColor Yellow
docker-compose ps

# Hiển thị hướng dẫn
Write-Host ""
Write-Host "======== SERVICES ========" -ForegroundColor Cyan
Write-Host "Airflow UI: http://localhost:8080 (username: admin, password: admin)" -ForegroundColor Green
Write-Host "Streamlit Dashboard: http://localhost:8501" -ForegroundColor Green
Write-Host "Kafka UI: http://localhost:8180" -ForegroundColor Green
Write-Host "Spark UI: http://localhost:4040 (khi Spark đang xử lý)" -ForegroundColor Green
Write-Host ""
Write-Host "Để xem logs:" -ForegroundColor Yellow
Write-Host "docker-compose logs -f [service_name]" -ForegroundColor White
Write-Host "" 
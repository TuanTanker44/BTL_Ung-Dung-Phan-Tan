# Windows PowerShell script to automate starting the Traffic Monitoring Pipeline

# Set console to UTF8 to print Vietnamese characters cleanly
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8

Write-Host "🚦 BẮT ĐẦU KHỞI ĐỘNG TRAFFIC MONITORING PIPELINE..." -ForegroundColor Cyan


# Luôn chạy trong thư mục chứa script để đường dẫn tương đối không bị sai
Set-Location $PSScriptRoot

function Fail($message) {
    Write-Host "❌ $message" -ForegroundColor Red
    Exit 1
}

function Test-Port($port) {
    try {
        $client = New-Object System.Net.Sockets.TcpClient
        $client.Connect("localhost", $port)
        $client.Close()
        return $true
    } catch {
        return $false
    }
}

function Wait-Port($port, $name, $timeoutSeconds = 90) {
    Write-Host "   Đang chờ $name (cổng $port)..." -ForegroundColor Gray
    $sw = [System.Diagnostics.Stopwatch]::StartNew()
    while ($sw.Elapsed.TotalSeconds -lt $timeoutSeconds) {
        if (Test-Port $port) {
            Write-Host "✅ $name sẵn sàng (cổng $port)." -ForegroundColor Green
            return
        }
        Start-Sleep -Seconds 2
    }
    Fail "$name không phản hồi trên cổng $port sau $timeoutSeconds giây. Kiểm tra bằng: docker-compose logs"
}

# 0. Tìm Python trong venv (thư mục dự án hoặc thư mục cha) và kiểm tra thư viện
Write-Host "`n0. Kiểm tra môi trường Python (venv)..." -ForegroundColor Blue
$venvPython = @("$PSScriptRoot\venv\Scripts\python.exe", "$PSScriptRoot\..\venv\Scripts\python.exe") |
    Where-Object { Test-Path $_ } | Select-Object -First 1
if (-not $venvPython) {
    Fail "Không tìm thấy venv. Hãy tạo bằng: python -m venv venv"
}
& $venvPython -c "import grpc, minio, pika, clickhouse_connect, streamlit, plotly, pandas" 2>$null
if ($LASTEXITCODE -ne 0) {
    Fail "Thiếu thư viện Python trong venv. Chạy: $venvPython -m pip install -r requirements.txt"
}
Write-Host "✅ venv hợp lệ: $venvPython" -ForegroundColor Green

# 1. Kiểm tra Docker Daemon có đang chạy không
Write-Host "`n1. Kiểm tra Docker..." -ForegroundColor Blue
docker info >$null 2>&1
if ($LASTEXITCODE -ne 0) {
    Write-Host "❌ Lỗi: Docker Desktop chưa được khởi chạy. Vui lòng mở Docker Desktop trước khi tiếp tục." -ForegroundColor Red
    Exit 1
}
Write-Host "✅ Docker đang hoạt động." -ForegroundColor Green

# 2. Khởi động Docker Compose
Write-Host "`n2. Khởi chạy các container nền (ClickHouse, MinIO, RabbitMQ, Spark)..." -ForegroundColor Blue
docker-compose up -d
if ($LASTEXITCODE -ne 0) { Fail "Lỗi khởi động docker-compose." }

# 3. Chờ các dịch vụ thực sự sẵn sàng (thay cho Start-Sleep cố định)
Write-Host "`n3. Chờ ClickHouse, RabbitMQ và MinIO sẵn sàng..." -ForegroundColor Blue
Wait-Port 8123 "ClickHouse"
Wait-Port 5672 "RabbitMQ"
Wait-Port 9000 "MinIO"

# 4. Cài đặt thư viện Python trong Spark Master
Write-Host "`n4. Cài đặt các thư viện cần thiết vào Spark Master container..." -ForegroundColor Blue
docker exec -u 0 -t image_pipeline_spark_master pip install -r /opt/spark-jobs/requirements.txt
if ($LASTEXITCODE -ne 0) { Fail "Thất bại khi cài đặt thư viện vào Spark container." }
Write-Host "✅ Cài đặt thư viện Spark thành công." -ForegroundColor Green

# 5. Khởi động gRPC Server trong nền (có log và kiểm tra cổng)
Write-Host "`n5. Đang chạy gRPC Server (ingestion/server.py) trong nền..." -ForegroundColor Blue
if (Test-Port 50051) {
    Write-Host "⚠️ Cổng 50051 đã được dùng (gRPC Server có thể đang chạy), bỏ qua bước này." -ForegroundColor Yellow
} else {
    $grpc = Start-Process -FilePath $venvPython -ArgumentList "ingestion/server.py" -WindowStyle Hidden -PassThru `
        -RedirectStandardOutput "$PSScriptRoot\ingestion_server.log" -RedirectStandardError "$PSScriptRoot\ingestion_server.err.log"
    Wait-Port 50051 "gRPC Server" 30
    Write-Host "   PID: $($grpc.Id) | Log: ingestion_server.err.log" -ForegroundColor Gray
}

# 6. Khởi động Web Dashboard
Write-Host "`n6. Đang chạy Web Dashboard (dashboard.py)..." -ForegroundColor Blue
if (Test-Port 8501) {
    Write-Host "⚠️ Cổng 8501 đã được dùng (Dashboard có thể đang chạy), bỏ qua bước này." -ForegroundColor Yellow
} else {
    $dash = Start-Process -FilePath $venvPython -ArgumentList "-m","streamlit","run","dashboard.py","--server.headless","true" -WindowStyle Hidden -PassThru `
        -RedirectStandardOutput "$PSScriptRoot\dashboard.log" -RedirectStandardError "$PSScriptRoot\dashboard.err.log"
    Wait-Port 8501 "Web Dashboard" 30
    Write-Host "   PID: $($dash.Id) | Log: dashboard.err.log" -ForegroundColor Gray
}
Start-Process "http://localhost:8501"

Write-Host "`n========================================================" -ForegroundColor Cyan
Write-Host "🎉 HỆ THỐNG ĐÃ SẴN SÀNG!" -ForegroundColor Green
Write-Host "Bây giờ bạn có thể:" -ForegroundColor White
Write-Host "  1. Chạy mô phỏng gửi ảnh từ camera: " -ForegroundColor White
Write-Host "     python ingestion/client.py" -ForegroundColor Yellow
Write-Host "  2. Kích hoạt Spark Job để xử lý dữ liệu: " -ForegroundColor White
Write-Host "     docker exec -e MINIO_ENDPOINT=minio:9000 -e RABBITMQ_HOST=rabbitmq -e CLICKHOUSE_HOST=clickhouse -t image_pipeline_spark_master spark-submit /opt/spark-jobs/spark_job.py" -ForegroundColor Yellow
Write-Host "========================================================" -ForegroundColor Cyan

#!/bin/bash

# Bash script to automate starting the Traffic Monitoring Pipeline

echo -e "\033[0;36m🚦 BẮT ĐẦU KHỞI ĐỘNG TRAFFIC MONITORING PIPELINE...\033[0m"


# Luôn chạy trong thư mục chứa script
cd "$(dirname "$0")" || exit 1

fail() { echo -e "\033[0;31m❌ $1\033[0m"; exit 1; }

port_open() { (echo > /dev/tcp/127.0.0.1/"$1") >/dev/null 2>&1; }

wait_port() {
    local port=$1 name=$2 timeout=${3:-90} waited=0
    echo "   Đang chờ $name (cổng $port)..."
    until port_open "$port"; do
        sleep 2
        waited=$((waited + 2))
        [ "$waited" -ge "$timeout" ] && fail "$name không phản hồi trên cổng $port sau ${timeout} giây. Kiểm tra: docker-compose logs"
    done
    echo -e "\033[0;32m✅ $name sẵn sàng (cổng $port).\033[0m"
}

# 0. Tìm Python trong venv (thư mục dự án hoặc thư mục cha)
PYTHON=""
for candidate in ./venv/bin/python ./venv/Scripts/python.exe ../venv/bin/python ../venv/Scripts/python.exe; do
    if [ -f "$candidate" ]; then PYTHON="$candidate"; break; fi
done
[ -z "$PYTHON" ] && fail "Không tìm thấy venv. Hãy tạo bằng: python -m venv venv"
"$PYTHON" -c "import grpc, minio, pika, clickhouse_connect, streamlit, plotly, pandas" 2>/dev/null \
    || fail "Thiếu thư viện Python trong venv. Chạy: $PYTHON -m pip install -r requirements.txt"
echo -e "\033[0;32m✅ venv hợp lệ: $PYTHON\033[0m"

# 1. Kiểm tra Docker
echo -e "\n\033[0;34m1. Kiểm tra Docker...\033[0m"
if ! docker info >/dev/null 2>&1; then
    echo -e "\033[0;31m❌ Lỗi: Docker Desktop chưa được khởi chạy. Vui lòng mở Docker Desktop.\033[0m"
    exit 1
fi
echo -e "\033[0;32m✅ Docker đang hoạt động.\033[0m"

# 2. Khởi động Docker Compose
echo -e "\n\033[0;34m2. Khởi chạy các container nền (ClickHouse, MinIO, RabbitMQ, Spark)...\033[0m"
docker-compose up -d || fail "Lỗi khởi động docker-compose."

# 3. Chờ các dịch vụ thực sự sẵn sàng
echo -e "\n\033[0;34m3. Chờ ClickHouse, RabbitMQ và MinIO sẵn sàng...\033[0m"
wait_port 8123 "ClickHouse"
wait_port 5672 "RabbitMQ"
wait_port 9000 "MinIO"

# 4. Cài đặt thư viện trong Spark Master
echo -e "\n\033[0;34m4. Cài đặt các thư viện cần thiết vào Spark Master container...\033[0m"
docker exec -u 0 -t image_pipeline_spark_master pip install -r /opt/spark-jobs/requirements.txt \
    || fail "Thất bại khi cài đặt thư viện vào Spark container."
echo -e "\033[0;32m✅ Cài đặt thư viện Spark thành công.\033[0m"

# 5. Khởi động gRPC Server
echo -e "\n\033[0;34m5. Đang chạy gRPC Server (ingestion/server.py) trong nền...\033[0m"
if port_open 50051; then
    echo -e "\033[0;33m⚠️ Cổng 50051 đã được dùng, bỏ qua bước này.\033[0m"
else
    "$PYTHON" ingestion/server.py > ingestion_server.log 2>&1 &
    echo "   PID: $! | Log: ingestion_server.log"
    wait_port 50051 "gRPC Server" 30
fi

# 6. Khởi động Web Dashboard
echo -e "\n\033[0;34m6. Đang chạy Web Dashboard (dashboard.py)...\033[0m"
if port_open 8501; then
    echo -e "\033[0;33m⚠️ Cổng 8501 đã được dùng, bỏ qua bước này.\033[0m"
else
    "$PYTHON" -m streamlit run dashboard.py --server.headless true > dashboard.log 2>&1 &
    echo "   PID: $! | Log: dashboard.log"
    wait_port 8501 "Web Dashboard" 30
fi

echo -e "\n\033[0;36m========================================================\033[0m"
echo -e "\033[0;32m🎉 HỆ THỐNG ĐÃ SẴN SÀNG!\033[0m"
echo -e "Bây giờ bạn có thể:"
echo -e "  1. Chạy mô phỏng gửi ảnh từ camera: "
echo -e "     \033[0;33mpython ingestion/client.py\033[0m"
echo -e "  2. Kích hoạt Spark Job để xử lý dữ liệu: "
echo -e "     \033[0;33mdocker exec -e MINIO_ENDPOINT=minio:9000 -e RABBITMQ_HOST=rabbitmq -e CLICKHOUSE_HOST=clickhouse -t image_pipeline_spark_master spark-submit /opt/spark-jobs/spark_job.py\033[0m"
echo -e "\033[0;36m========================================================\033[0m"

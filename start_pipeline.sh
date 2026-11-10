#!/bin/bash

# Bash script to automate starting the Traffic Monitoring Pipeline

echo -e "\033[0;36m🚦 BẮT ĐẦU KHỞI ĐỘNG TRAFFIC MONITORING PIPELINE...\033[0m"


# Luôn chạy trong thư mục chứa script
cd "$(dirname "$0")" || exit 1

fail() { echo -e "\033[0;31m❌ $1\033[0m"; exit 1; }

port_open() { (echo > /dev/tcp/127.0.0.1/"$1") >/dev/null 2>&1; }

# 0. Tìm Python trong venv (thư mục dự án hoặc thư mục cha)
PYTHON=""
for candidate in ./venv/bin/python ./venv/Scripts/python.exe ../venv/bin/python ../venv/Scripts/python.exe; do
    if [ -f "$candidate" ]; then PYTHON="$candidate"; break; fi
done
[ -z "$PYTHON" ] && fail "Không tìm thấy venv. Hãy tạo bằng: python -m venv venv"
"$PYTHON" -c "import grpc, grpc_health, minio, pika, clickhouse_connect, streamlit, streamlit_autorefresh, plotly, pandas" 2>/dev/null \
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
docker compose up -d || fail "Lỗi khởi động docker compose."

# 3. Kiểm tra trạng thái ứng dụng, Spark worker và Airflow scheduler
echo -e "\n\033[0;34m3. Chờ toàn bộ hạ tầng đạt trạng thái healthy...\033[0m"
"$PYTHON" scripts/verify_services.py infrastructure --timeout 240 \
    || fail "Hạ tầng chưa sẵn sàng. Kiểm tra: docker compose logs"

# 4. Áp dụng migration và kiểm tra dependency driver/worker
echo -e "\n\033[0;34m4. Áp dụng migration ClickHouse...\033[0m"
docker exec -i image_pipeline_clickhouse clickhouse-client --multiquery \
    < clickhouse/migrations/001_add_detection_mode.sql || fail "Migration 001 thất bại."
docker exec -i image_pipeline_clickhouse clickhouse-client --multiquery \
    < clickhouse/migrations/002_make_traffic_facts_idempotent.sql || fail "Migration 002 thất bại."
"$PYTHON" scripts/verify_dependencies.py \
    || fail "Dependency host/Spark/Airflow không tương thích."
echo -e "\033[0;32m✅ Migration và dependency hợp lệ.\033[0m"

# 5. Khởi động gRPC Server
echo -e "\n\033[0;34m5. Đang chạy gRPC Server (ingestion/server.py) trong nền...\033[0m"
if port_open 50051; then
    echo -e "\033[0;33m⚠️ Cổng 50051 đã được dùng, bỏ qua bước này.\033[0m"
else
    "$PYTHON" ingestion/server.py > ingestion_server.log 2>&1 &
    echo "   PID: $! | Log: ingestion_server.log"
fi

# 6. Khởi động Web Dashboard
echo -e "\n\033[0;34m6. Đang chạy Web Dashboard (dashboard.py)...\033[0m"
if port_open 8501; then
    echo -e "\033[0;33m⚠️ Cổng 8501 đã được dùng, bỏ qua bước này.\033[0m"
else
    "$PYTHON" -m streamlit run dashboard.py --server.headless true > dashboard.log 2>&1 &
    echo "   PID: $! | Log: dashboard.log"
fi

"$PYTHON" scripts/verify_services.py applications --timeout 60 \
    || fail "gRPC Server hoặc Dashboard chưa healthy."

echo -e "\n\033[0;36m========================================================\033[0m"
echo -e "\033[0;32m🎉 HỆ THỐNG ĐÃ SẴN SÀNG!\033[0m"
echo -e "Bây giờ bạn có thể:"
echo -e "  1. Chạy mô phỏng gửi ảnh từ camera: "
echo -e "     \033[0;33mpython ingestion/client.py\033[0m"
echo -e "  2. Kích hoạt Spark Job để xử lý dữ liệu: "
echo -e "     \033[0;33mdocker exec -e MINIO_ENDPOINT=minio:9000 -e RABBITMQ_HOST=rabbitmq -e CLICKHOUSE_HOST=clickhouse -t image_pipeline_spark_master spark-submit /opt/spark-jobs/spark_job.py\033[0m"
echo -e "\033[0;36m========================================================\033[0m"

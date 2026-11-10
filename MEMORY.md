# Bộ nhớ tiến độ sửa lỗi pipeline

Cập nhật: 10/11/2026 (Asia/Saigon).

Dự án: BTL_-Ung-Dung-Phan-Tan. Pipeline sử dụng ingestion, RabbitMQ, Spark, ClickHouse, Airflow và dashboard.

Nguồn trạng thái ban đầu: danh sách người dùng cung cấp ngày 10/11/2026. Các trạng thái ban đầu chưa được kiểm chứng trong lượt lưu đầu tiên; bằng chứng kiểm tra và sửa đổi sau đó được ghi riêng bên dưới.

## Danh sách theo dõi

| ID | Ưu tiên | Vấn đề / công việc | File liên quan | Trạng thái hiện tại | Commit / bằng chứng |
| --- | --- | --- | --- | --- | --- |
| 1 | P1 | ClickHouse và RabbitMQ thiếu volume lưu dữ liệu bền vững | `docker-compose.yml` | Đã sửa, đã push | `ba13c86` — theo người dùng |
| 2 | P1 | JSON/schema sai làm consumer thoát, requeue các message chưa ACK | `spark/spark_job.py` | Đã sửa, commit local; chưa ghi nhận đã push | `c6f8b8e` — theo người dùng |
| 3 | P1 | Ảnh rỗng/hỏng làm Spark thất bại; cần cô lập lỗi theo event và chuyển DLQ | `spark/spark_job.py`, `tests/test_image_processing.py`, `tests/test_rabbitmq_consumer.py` | Đã sửa, đã commit local; chưa push; chưa kiểm thử cụm thật | `7fd2e88`; 10/11/2026: 16/16 test riêng tại thời điểm commit đạt |
| 4 | P1 | Publish RabbitMQ không rõ thành công hay thất bại; rollback có thể xóa ảnh của event đã vào queue | `ingestion/server.py`, `tests/test_ingestion_publish.py` | Đã sửa, đã commit local; chưa push | `0a2d1ab`; ảnh được giữ lại khi confirm không rõ kết quả; test âm publisher confirm đạt |
| 5 | P2 | Chống ghi trùng chưa bảo đảm khi nhiều job xử lý cùng event đồng thời | `spark/spark_job.py`, `clickhouse/schema.sql`, migration 002 | Đã sửa logic và migration, đã commit local; chưa chạy migration trên cụm thật | `296e035`; `ReplacingMergeTree(event_id, vehicle_type_id)` và mọi lượt đọc fact dùng `FINAL` |
| 6 | P2 | Airflow webserver/scheduler chưa chờ `airflow-init` hoàn thành | `docker-compose.yml` | Đã sửa, đã commit local; YAML hợp lệ; chưa chạy Compose thật | `c802c16`; dùng `service_completed_successfully`, init idempotent |
| 7 | P2 | Dashboard không tự cập nhật; TTL cache không kích hoạt rerun | `dashboard.py`, `requirements.txt` | Đã sửa, đã commit local; kiểm thử contract đạt | `10ecbee`; rerun 10 giây mặc định, có công tắc sidebar |
| 8 | Cần xác minh | Dependencies giữa host, Spark và Airflow chưa được kiểm chứng đầy đủ về phiên bản và khả năng tương thích | Requirements, Dockerfile, `scripts/verify_dependencies.py` | Đã chuẩn hóa và thêm kiểm tra tự động; chưa xác minh runtime vì host hiện tại thiếu Docker/dependency | `c89a758`; test tĩnh đạt; runtime verifier báo thiếu `minio` trên host và không có Docker CLI |
| 9 | Cần xác minh | Độ chính xác và ý nghĩa KPI: thời gian trung bình, tổng lượt xe xuất hiện so với số xe duy nhất | `dashboard.py`, `dashboard_metrics.py` | Đã sửa, đã commit local; unit test công thức đạt | `bcb3e9a`; trung bình inference theo event; KPI ghi rõ là lượt phát hiện, không tuyên bố xe duy nhất |
| 10 | P2 | Script khởi động chủ yếu kiểm tra cổng; chưa xác minh đầy đủ ứng dụng, Spark worker và Airflow | Startup scripts, `scripts/verify_services.py`, gRPC health | Đã sửa, đã commit local; test tĩnh và cú pháp PowerShell/Python đạt; chưa chạy pipeline thật | `b3db1e4`; kiểm tra phản hồi ClickHouse/MinIO/RabbitMQ, Spark worker, Airflow, gRPC và Streamlit |
| 11 | Cần xác minh | Chưa có bằng chứng Spark phân phối task lên worker và pipeline hoạt động end-to-end | `scripts/e2e_smoke_test.py`, README | Đã có smoke test tạo bằng chứng, đã commit local; chưa chạy trên Codespace/cụm Docker | `0c3a1e9`; cần chạy script để sinh `artifacts/e2e-evidence.json` |

## Cách cập nhật tiến độ

- Giữ nguyên ID để tham chiếu qua các lượt làm việc.
- Phân biệt đã sửa, đã kiểm thử, đã commit local và đã push; không suy ra các bước sau từ bước trước.
- Khi cập nhật một mục, ghi ngày, commit nếu có, kiểm tra đã chạy, kết quả và phần còn thiếu.
- Với mục 8, 9 và 11, chỉ kết luận sau khi có bằng chứng kiểm chứng; ghi rõ môi trường chạy.
- Không đánh dấu mục 8 hoặc 11 là đã xác minh runtime cho đến khi trình kiểm tra chạy đạt trên cụm Docker/Codespace.

## Lịch sử cập nhật

- 10/11/2026: Lưu 11 mục từ danh sách người dùng. Mục 1 đã sửa và push; mục 2 đã sửa và commit local; 9 mục còn lại cần sửa, hoàn thiện hoặc xác minh. Lượt này chỉ lưu tiến độ, chưa sửa mã hoặc chạy kiểm thử.
- 10/11/2026: Sửa mục 3. Giải mã ảnh bằng Pillow (`verify`, mở lại, `load`, chuyển RGB) trước xử lý ở cả `mock` và `yolo`. Ảnh rỗng/hỏng/không tồn tại trả lỗi riêng từng event (`EMPTY_IMAGE`, `INVALID_IMAGE`, `IMAGE_NOT_FOUND`), không sinh KPI và được chuyển thẳng DLQ theo mã lỗi. Consumer giữ body/header gốc, thêm thông tin lỗi và chỉ ACK sau khi publish có xác nhận; nếu publish lỗi thì message gốc chưa ACK. Lỗi tải MinIO, quyền truy cập, thiếu thư viện/model hoặc lỗi YOLO vẫn làm task báo lỗi, không bị phân loại thành dữ liệu ảnh hỏng.
- Kiểm chứng mục 3 trên host Windows: 16/16 test đạt với lệnh `python -m unittest discover -s tests -v`; `git diff --check` đạt. Test dùng Pillow thật để kiểm tra ảnh hợp lệ, ảnh rỗng, byte không phải ảnh và JPEG bị cắt dữ liệu; Spark, MinIO, RabbitMQ và YOLO được giả lập. Có kiểm tra batch trộn ảnh tốt/xấu ở cả hai chế độ, giải phóng tài nguyên, chỉ ghi event tốt vào ClickHouse và không ACK khi publish DLQ thất bại. Chưa chạy Spark/RabbitMQ/MinIO/YOLO thực tế hoặc Codespace. Đã commit local tại `7fd2e88`, chưa push.
- 10/11/2026: Sửa và commit local mục 4–11 theo thứ tự: `0a2d1ab`, `296e035`, `c802c16`, `10ecbee`, `c89a758`, `bcb3e9a`, `b3db1e4`, `0c3a1e9`. Vòng kiểm tra cuối đạt 32/32 unit/contract test, `py_compile` cho các file Python thay đổi, parse PowerShell, parse `docker-compose.yml` bằng PyYAML và `git diff --check`. Host không có Bash nên chưa chạy `bash -n`.
- Giới hạn kiểm chứng ngày 10/11/2026: `scripts/verify_dependencies.py` không thể hoàn tất vì Python host thiếu `minio` và không có Docker CLI. Do đó chưa build image, chưa chạy migration trên ClickHouse thật, chưa xác nhận Python driver–worker, chưa chạy service readiness và chưa sinh bằng chứng E2E. Khi có Codespace/Docker, chạy startup script rồi `python scripts/e2e_smoke_test.py --output artifacts/e2e-evidence.json`; chỉ sau khi hai lệnh đạt mới chuyển mục 8 và 11 sang “đã xác minh runtime”.

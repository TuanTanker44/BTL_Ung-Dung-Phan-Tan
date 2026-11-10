# Bộ nhớ tiến độ sửa lỗi pipeline

Cập nhật: 10/11/2026 (Asia/Saigon).

Dự án: BTL_-Ung-Dung-Phan-Tan. Pipeline sử dụng ingestion, RabbitMQ, Spark, ClickHouse, Airflow và dashboard.

Nguồn trạng thái ban đầu: danh sách người dùng cung cấp ngày 10/11/2026. Các trạng thái ban đầu chưa được kiểm chứng trong lượt lưu đầu tiên; bằng chứng kiểm tra và sửa đổi sau đó được ghi riêng bên dưới.

## Danh sách theo dõi

| ID | Ưu tiên | Vấn đề / công việc | File liên quan | Trạng thái hiện tại | Commit / bằng chứng |
| --- | --- | --- | --- | --- | --- |
| 1 | P1 | ClickHouse và RabbitMQ thiếu volume lưu dữ liệu bền vững | `docker-compose.yml` | Đã sửa, đã push | `ba13c86` — theo người dùng |
| 2 | P1 | JSON/schema sai làm consumer thoát, requeue các message chưa ACK | `spark/spark_job.py` | Đã sửa, commit local; chưa ghi nhận đã push | `c6f8b8e` — theo người dùng |
| 3 | P1 | Ảnh rỗng/hỏng làm Spark thất bại; cần cô lập lỗi theo event và chuyển DLQ | `spark/spark_job.py`, `tests/test_image_processing.py`, `tests/test_rabbitmq_consumer.py` | Đã sửa; kiểm thử local đạt; chưa commit/push; chưa kiểm thử cụm thật | 10/11/2026: `python -m unittest discover -s tests -v` — 16/16 test đạt; `git diff --check` đạt |
| 4 | P1 | Publish RabbitMQ không rõ thành công hay thất bại; rollback có thể xóa ảnh của event đã vào queue | `ingestion/server.py` | Chưa sửa | Chưa có |
| 5 | P2 | Chống ghi trùng chưa bảo đảm khi nhiều job xử lý cùng event đồng thời | `spark/spark_job.py`, `clickhouse/schema.sql` | Chưa sửa | Chưa có |
| 6 | P2 | Airflow webserver/scheduler chưa chờ `airflow-init` hoàn thành | `docker-compose.yml` | Chưa sửa | Chưa có |
| 7 | P2 | Dashboard không tự cập nhật; TTL cache không kích hoạt rerun | `dashboard.py` | Chưa sửa | Chưa có |
| 8 | Cần xác minh | Dependencies giữa host, Spark và Airflow chưa được kiểm chứng đầy đủ về phiên bản và khả năng tương thích | Các file requirements và Dockerfile | Cần kiểm tra build/import và driver–worker | Chưa có |
| 9 | Cần xác minh | Độ chính xác và ý nghĩa KPI: thời gian trung bình, tổng lượt xe xuất hiện so với số xe duy nhất | `dashboard.py`, `spark/spark_job.py` | Cần rà công thức và đối chiếu dữ liệu mẫu | Chưa có |
| 10 | P2 | Script khởi động chủ yếu kiểm tra cổng; chưa xác minh đầy đủ ứng dụng, Spark worker và Airflow | `start_pipeline.ps1`, `start_pipeline.sh` | Chưa hoàn thiện | Chưa có |
| 11 | Cần xác minh | Chưa có bằng chứng Spark phân phối task lên worker và pipeline hoạt động end-to-end | Spark, Airflow, Docker Compose | Cần kiểm thử trên Codespace | Chưa có |

## Cách cập nhật tiến độ

- Giữ nguyên ID để tham chiếu qua các lượt làm việc.
- Phân biệt đã sửa, đã kiểm thử, đã commit local và đã push; không suy ra các bước sau từ bước trước.
- Khi cập nhật một mục, ghi ngày, commit nếu có, kiểm tra đã chạy, kết quả và phần còn thiếu.
- Với mục 8, 9 và 11, chỉ kết luận sau khi có bằng chứng kiểm chứng; ghi rõ môi trường chạy.
- Ưu tiên các mục P1 chưa sửa (3, 4) trước các mục P2, trừ khi người dùng điều chỉnh thứ tự.

## Lịch sử cập nhật

- 10/11/2026: Lưu 11 mục từ danh sách người dùng. Mục 1 đã sửa và push; mục 2 đã sửa và commit local; 9 mục còn lại cần sửa, hoàn thiện hoặc xác minh. Lượt này chỉ lưu tiến độ, chưa sửa mã hoặc chạy kiểm thử.
- 10/11/2026: Sửa mục 3. Giải mã ảnh bằng Pillow (`verify`, mở lại, `load`, chuyển RGB) trước xử lý ở cả `mock` và `yolo`. Ảnh rỗng/hỏng/không tồn tại trả lỗi riêng từng event (`EMPTY_IMAGE`, `INVALID_IMAGE`, `IMAGE_NOT_FOUND`), không sinh KPI và được chuyển thẳng DLQ theo mã lỗi. Consumer giữ body/header gốc, thêm thông tin lỗi và chỉ ACK sau khi publish có xác nhận; nếu publish lỗi thì message gốc chưa ACK. Lỗi tải MinIO, quyền truy cập, thiếu thư viện/model hoặc lỗi YOLO vẫn làm task báo lỗi, không bị phân loại thành dữ liệu ảnh hỏng.
- Kiểm chứng mục 3 trên host Windows: 16/16 test đạt với lệnh `python -m unittest discover -s tests -v`; `git diff --check` đạt. Test dùng Pillow thật để kiểm tra ảnh hợp lệ, ảnh rỗng, byte không phải ảnh và JPEG bị cắt dữ liệu; Spark, MinIO, RabbitMQ và YOLO được giả lập. Có kiểm tra batch trộn ảnh tốt/xấu ở cả hai chế độ, giải phóng tài nguyên, chỉ ghi event tốt vào ClickHouse và không ACK khi publish DLQ thất bại. Chưa chạy Spark/RabbitMQ/MinIO/YOLO thực tế hoặc Codespace; mục 11 vẫn chưa hoàn tất. Mục 3 chưa commit và chưa push. Còn 8 mục khác cần sửa, hoàn thiện hoặc xác minh.

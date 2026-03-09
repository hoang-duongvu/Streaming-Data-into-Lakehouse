# 🌊 Building Streaming Data Pipeline with Data Lakehouse

<p align="center">
  <img src="https://img.shields.io/badge/Apache_Flink-E6522C?style=for-the-badge&logo=apache-flink&logoColor=white" alt="Flink"/>
  <img src="https://img.shields.io/badge/Apache_Kafka-231F20?style=for-the-badge&logo=apache-kafka&logoColor=white" alt="Kafka"/>
  <img src="https://img.shields.io/badge/Apache_Iceberg-008CB1?style=for-the-badge&logo=apache&logoColor=white" alt="Iceberg"/>
  <img src="https://img.shields.io/badge/Trino-DD00A1?style=for-the-badge&logo=trino&logoColor=white" alt="Trino"/>
  <img src="https://img.shields.io/badge/MinIO-C7202C?style=for-the-badge&logo=minio&logoColor=white" alt="MinIO"/>
  <img src="https://img.shields.io/badge/Docker-2496ED?style=for-the-badge&logo=docker&logoColor=white" alt="Docker"/>
</p>

Xây dựng luồng xử lý dữ liệu thời gian thực (Real-time Streaming Pipeline) với **Apache Flink**, lưu trữ dữ liệu với kiến trúc định dạng **Data Lakehouse** thông qua nền tảng **Iceberg**, tương tác truy vấn qua **Trino** và dịch vụ lưu trữ Object Storage bằng **MinIO**.

---

## 🏛️ Kiến trúc hệ thống (High-Level System Architecture)
Kiến trúc tổng quan mô tả dòng dữ liệu từ khi sinh ra tại Source, truyền qua Kafka broker, được xử lý Streaming bởi Flink và lưu trữ phân lớp xuống Data Lakehouse.

![System Architecture](./image/architecture.png)

## 🔄 Logic xử lý luồng (Data Transformation Logic)
Mô hình diễn giải chi tiết các quá trình biến đổi dữ liệu sử  dụng các kĩ thuật Join, Window Aggregation bằng Flink SQL & Table API.

![Data Transformation Logic](./image/data_transformation.png)

---

## 🚀 Tính năng chính (Key Features)

- **Real-time Data Processing**: Xử lý luồng dữ liệu Event-driven tốc độ cao với độ trễ thấp bằng Apache Flink.
- **Data Enrichment**: Làm giàu dữ liệu tĩnh chứa (User, Product) trong PostgreSQL kết hợp với luồng dữ liệu liên tục trong Kafka.
- **Stream-to-Stream Join**: Kết nối hai luồng thông tin rời rạc Clicks và Checkouts để thiết lập mối quan hệ theo khung thời gian (Interval Join).
- **Window & Session Aggregation**: Gom nhóm dữ liệu và tạo báo cáo trực tiếp trong từng khoảng thời gian (Tumbling Windows) hoặc theo mốc hoạt động của người dùng (Session Windows).
- **Data Lakehouse Architecture**: Lưu trữ định dạng bảng Iceberg vào MinIO (với Nessie làm Catalog) giúp đảm bảo tính ACID và sẵn sàng cho các engine đọc nhanh như Trino.

---

## 🛠️ Hướng dẫn cài đặt và chạy dự án (Getting Started)

### 1. Khởi động hạ tầng hệ thống (Infrastructure)

Khởi động toàn bộ các thành phần hạ tầng thông qua Docker Compose (bao gồm Kafka, Flink, MinIO, Trino, v.v.):

```bash
make up
```
![Docker Containers](./image/setup_docker_services.png)

> **⚠️ LƯU Ý QUAN TRỌNG:** Để có đủ **Task Slots** cho các Flink Job chạy song song mà không bị kẹt, hệ thống cần được khởi chạy với số lượng Task Manager tối thiểu từ 4 trở lên (Đã cấu hình sẵn `--scale flink-taskmanager=4` trong file `Makefile`).

**Kết quả thành công:**
- Bucket gốc lưu data tại MinIO được cấp phát sẵn một cách tự động.
  ![Bucket được tạo trên MinIO](./image/setup_initial_bucket.png)
- Các cấu phần Task Manager đăng ký và nhận kết nối thành công với Job Manager.
  ![Các Task Manager kết nối thành công với Job Manager](./image/setup_registered_taskmanager.png)

### 2. Khởi động giả lập dữ liệu (Source Data Generator)

Sản sinh luồng dữ liệu sự kiện theo thời gian thực để tiến hành giả lập hệ thống:

```bash
make setup_source
```

**Kết quả thành công:**
- Các bản ghi dữ liệu được đẩy thành công vào các Topic tương ứng trên Kafka. Các bản ghi này được tuần tự hóa theo dạng Avro để giảm tài nguyên cần thiết để lưu trữ.
  ![Các bản ghi được gửi thành công vào Kafka](./image/setup_message_in_kafka.png)
- Cấu trúc lược đồ (Avro schema) được tải lên và lưu trữ thành công trên Schema Registry. Các lược đồ này được sử dụng bởi Flink để giải tuần tự hóa dữ liệu. Schema Registry cũng hỗ trợ Schema Evolution, hạn chế lỗi khi Data Source thay đổi Schema.
  ![Lược đồ dữ liệu được gửi thành công vào Schema Registry](./image/setup_schema_in_KSR.png)

### 3. Đệ trình tiến trình xử lý tính toán (Submit Flink Jobs)

Tiến hành đệ trình (`submit`) các Flink Streaming Jobs vào hệ thống cluster Flink:

```bash
make jobs
```

![Các Flink Job được khởi động thành công](./image/successfull_job_submits.png)

Khi Jobs chuyển sang trạng thái `RUNNING`, hệ thống sẽ liên tục đọc sự kiện Kafka và xử lý qua toán tử (operators) theo kiến trúc bên trên, kết xuất dữ liệu và được ghi theo định dạng Parquet/Iceberg tại MinIO (Storage của Data LakeHouse):

![Các thư mục dữ liệu trên MinIO](./image/table_dir_in_minio.png)

---

## 📊 Truy vấn Lakehouse (Validation & Querying)

Với **Trino**, hệ thống cho phép Data Analysts và Data Scientists có thể tận dụng kiến thức SQL cơ bản để truy xuất, kiểm chứng và phân tích dữ liệu trực tiếp từ Data Lakehouse.

**Bảng Enriched Checkouts:**
![Trino Enriched Checkouts](./image/trino_enriched_checkouts.png)

**Bảng Enriched Clicks:**
![Trino Enriched Clicks](./image/trino_enriched_clicks.png)

**Bảng thống kê hoạt động Window Aggregation (5 phút):**
![Trino Window Aggregation](./image/trino_window_agg.png)

**Bảng gộp Stream Join phân tích chuyển đổi Hành động:**
![Trino Stream Join](./image/trino_stream_join.png)

**Mô hình hóa dữ liệu Phiên người dùng (Session Aggregation):**
![Trino Session Agg](./image/trino_session.png)

---
<div align="center">
  <sub>Dương Vũ Hoàng</sub>
</div>

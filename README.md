# Gold Price Data Pipeline

Dự án Airflow để thu thập, xử lý và lưu trữ dữ liệu giá vàng từ website giavang.org.

## Mô tả

Dự án này sử dụng Apache Airflow để tạo một data pipeline hoàn chỉnh:

1. **Extract**: Thu thập dữ liệu giá vàng (tael và chi) từ website giavang.org
2. **Transform**: Làm sạch và chuẩn hóa dữ liệu
3. **Load**: Lưu trữ dữ liệu vào PostgreSQL database

## Cấu trúc dự án

```
gold price/
├── dags/
│   └── dags.py              # DAG chính để fetch và store gold prices
├── config/
│   └── airflow.cfg          # Cấu hình Airflow
├── docker-compose.yaml      # Docker Compose để chạy Airflow
├── logs/                    # Log files (được ignore bởi git)
└── plugins/                 # Custom plugins (nếu có)
```

## Cài đặt và chạy

### Yêu cầu

- Docker và Docker Compose
- PostgreSQL database

### Các bước thực hiện

1. Clone repository:

```bash
git clone <repository-url>
cd gold-price
```

2. Cấu hình database connection trong Airflow:

   - Tạo connection với ID: `gold_price_connection`
   - Type: PostgreSQL
   - Host: localhost (hoặc địa chỉ database)
   - Schema: tên database
   - Login: username
   - Password: password

3. Chạy Airflow với Docker Compose:

```bash
docker-compose up -d
```

4. Truy cập Airflow UI tại: http://localhost:8080

## DAG Schedule

DAG `fetch_and_store_gold_prices` được lên lịch chạy vào các giờ:

- 8:00, 11:00, 14:00, 17:00, 21:00, 23:00 hàng ngày

## Database Schema

### Bảng regions

- `region_id` (INTEGER, PRIMARY KEY)
- `region` (TEXT)

### Bảng tael_price

- `region` (INTEGER, FOREIGN KEY)
- `sys` (TEXT)
- `buy_price` (FLOAT)
- `sell_price` (FLOAT)
- `time` (TIMESTAMP)

### Bảng chi_price

- `region` (INTEGER, FOREIGN KEY)
- `sys` (TEXT)
- `buy_price` (FLOAT)
- `sell_price` (FLOAT)
- `time` (TIMESTAMP)


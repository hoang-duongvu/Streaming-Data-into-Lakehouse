## s2lh == streaming-to-lake-house

## Cài đặt dbt 

Cài thư viện `dbt-trino`

```
uv init 
uv add dbt-trino
```

Khởi tạo dbt project

```
uv run dbt init s2lh
```

Cấu hình kết nối đến Trino bằng cách chỉnh sửa file `profiles.yml`

Kiểm tra kết nối 

```uv run dbt debug```
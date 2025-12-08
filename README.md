# CVC - Docker Compose Setup




## Cài đặt

### 1. Clone repository

```bash
git clone https://github.com/nhthai173/cvc
cd cvc
```

### 2. Cấu hình Environment Variables

Copy file mẫu và chỉnh sửa:

```bash
cp .env.example .env
```

Mở file `.env` và cập nhật các giá trị

### 3. Tạo thư mục và set quyền (Linux/Debian)

```bash
# Tạo thư mục cho volumes
mkdir -p ~/emqx-data ~/emqx-log ~/cvc-postgres

# Set quyền cho EMQX
chown -R 1000:1000 ~/emqx-data ~/emqx-log

### 4. Khởi động services

```bash
# Khởi động tất cả services
docker compose up -d

# Xem logs
docker compose logs -f

# Kiểm tra trạng thái
docker compose ps
```

## Các lệnh hữu ích

```bash
# Dừng tất cả services
docker compose down

# Dừng và xóa volumes (⚠️ mất dữ liệu)
docker compose down -v

# Khởi động lại một service
docker compose restart emqx
docker compose restart postgres

# Xem logs của một service
docker compose logs -f emqx
docker compose logs -f postgres

# Rebuild và restart
docker compose up -d --build

# Vào shell của container
docker exec -it cvc-postgres bash
docker exec -it cvc-emqx sh
```

## Backup & Restore

### PostgreSQL

**Backup**:
```bash
docker exec cvc-postgres pg_dump -U cvcuser cvcdb > backup_$(date +%Y%m%d).sql
```

**Restore**:
```bash
docker exec -i cvc-postgres psql -U cvcuser cvcdb < backup_20241208.sql
```

### EMQX

**Backup volumes**:
```bash
# Backup data
tar -czf emqx-data-backup.tar.gz ~/emqx-data

# Backup logs
tar -czf emqx-log-backup.tar.gz ~/emqx-log
```

**Restore**:
```bash
tar -xzf emqx-data-backup.tar.gz -C ~/
```

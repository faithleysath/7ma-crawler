## 7ma Crawler

采集器会按固定点位扫 `7mate` 的周边车辆接口，把每次请求和每辆车原始命中都写进 PostgreSQL。

### 本地开发直接跑 PostgreSQL

```bash
docker compose up -d postgres
```

本地默认连接串：

```bash
export DATABASE_URL='postgresql://sevenma:sevenma@localhost:5432/sevenma'
```

### 初始化数据库

```bash
uv run python -m sevenma_crawler prepare-db
```

这一步会：
- 创建表和 `vehicle_latest` view
- 把 [南信大选点.json](/Users/laysath/proj/7ma-crawler/南信大选点.json) 里的 58 个点写入 `crawl_point`

### 跑一轮采集

```bash
uv run python -m sevenma_crawler run-once \
  --source-namespace local-dev \
  --collector-id macbook-dev
```

### 常驻运行

```bash
uv run python -m sevenma_crawler run-forever \
  --source-namespace local-dev \
  --collector-id macbook-dev \
  --interval-seconds 60 \
  --concurrency 8
```

### 查看当前最新车辆

```sql
select *
from vehicle_latest
where source_namespace = 'local-dev'
order by observed_at desc
limit 20;
```

### 启动监控大屏

```bash
uv run python -m sevenma_crawler serve-dashboard \
  --database-url 'postgresql://sevenma:sevenma@localhost:5432/sevenma' \
  --source-namespace local-dev \
  --amap-key "$AMAP_WEB_KEY" \
  --amap-security-js-code "$AMAP_SECURITY_JS_CODE" \
  --host 0.0.0.0 \
  --port 8000
```

然后打开 [http://127.0.0.1:8000](http://127.0.0.1:8000)。

### Docker Compose 一键编排

先准备环境变量：

```bash
cp .env.example .env
```

默认 `.env` 会启用 compose 内置 PostgreSQL，数据库地址也是内部地址：

```bash
DATABASE_URL=postgresql://sevenma:sevenma@postgres:5432/sevenma
COMPOSE_PROFILES=local-db
```

填好高德 key 后，直接启动整套服务：

```bash
docker compose up -d --build
```

包含的服务：
- `postgres`: 可选的本地 PostgreSQL，放在 `local-db` profile 里
- `bootstrap`: 一次性建表并导入 58 个点
- `collector`: 常驻采集器
- `dashboard`: 监控大屏

大屏地址：

```text
http://127.0.0.1:8000
```

### 改成远程数据库

如果你要把数据库切到云端或远程主机：

1. 在 `.env` 里把 `DATABASE_URL` 改成远程连接串
2. 把 `COMPOSE_PROFILES` 置空或删除，避免启动本地 PostgreSQL
3. 再执行 `docker compose up -d --build`

例如：

```bash
DATABASE_URL=postgresql://user:password@your-remote-host:5432/sevenma
COMPOSE_PROFILES=
```

### 主要表

- `crawl_point`: 固定采集点配置
- `crawl_sweep`: 某个采集器的一次完整扫描
- `point_fetch`: 某次扫描里某个点的一次实际请求
- `raw_observation`: 某次请求扫到的一辆车
- `vehicle_latest`: 从历史记录自动推导的最新状态 view

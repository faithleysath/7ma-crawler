# Runbook

## Compose startup

首次启动整套服务：

```bash
cp .env.example .env
docker compose up -d --build
```

只升级 schema：

```bash
uv run python -m sevenma_crawler migrate-db
```

查看服务状态：

```bash
docker compose ps
```

查看日志：

```bash
docker compose logs -f collector
docker compose logs -f dashboard
docker compose logs -f bootstrap
```

## Health checks

- Dashboard 健康检查：`http://127.0.0.1:8000/healthz`
- Dashboard bootstrap：`http://127.0.0.1:8000/api/dashboard/bootstrap`
- PostgreSQL：`docker compose exec postgres pg_isready -U sevenma -d sevenma`

## Common issues

`dashboard` 页面空白或地图不显示：
- 检查 `AMAP_WEB_KEY` 和 `AMAP_SECURITY_JS_CODE`
- 检查浏览器控制台是否报高德脚本加载错误

`collector` 一直失败：
- 先看 `docker compose logs -f collector`
- 检查 `DATABASE_URL` 是否能连通
- 检查是否出现大量 `HTTP 5xx` 或网络错误重试日志

Dashboard 显示 `stale`：
- 说明最近没有新的 sweep 或最新观测已超时
- 先检查 `collector` 日志
- 再检查 `bootstrap` 是否成功执行过迁移和点位导入

切换远程数据库：
- 更新 `.env` 里的 `DATABASE_URL`
- 清空 `COMPOSE_PROFILES`
- 执行 `docker compose up -d --build`

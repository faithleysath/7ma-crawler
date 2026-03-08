# 7ma Crawler Development Plan

这个清单用于跟踪下一阶段的工程化工作。每完成一项就直接在仓库里勾掉。

## Phase 0: Project Baseline

- [x] 把开发计划写入项目文档，并在 README 提供入口
- [x] 清理 `pyproject.toml` 元数据，拆分运行依赖和开发依赖
- [x] 在 README 补充开发命令
- [x] 增加 CI 工作流，默认执行 `ruff`、`pyright`、`pytest`

## Phase 1: Test Foundation

- [x] 增加 API 响应解析和错误路径单元测试
- [x] 增加 collector 失败路径单元测试
- [x] 增加数据库集成测试，覆盖 schema、写入和 `vehicle_latest`
- [x] 增加 dashboard bootstrap API 测试

## Phase 2: Collector Resilience

- [x] 为临时性网络错误和 HTTP 5xx 增加有限次重试与退避
- [x] 为重试相关行为补单元测试
- [x] 在日志中补充 point、attempt、latency、http status 等关键字段

## Phase 3: Database Evolution

- [x] 引入版本化迁移机制，避免只靠整份 `schema.sql`
- [x] 固化当前 schema 为初始迁移
- [x] 补充数据库升级和新库初始化文档

## Phase 4: Operability

- [x] 增加数据陈旧告警或 stale 状态判断
- [x] 展示最近 sweep 健康状态和失败摘要
- [x] 补充 compose 运行、排障和远程数据库切换文档

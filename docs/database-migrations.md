# Database Migrations

项目现在使用内置的版本化 SQL migration，而不是每次直接执行整份 schema。

## Migration layout

- 迁移目录：`sevenma_crawler/migrations`
- 命名规则：`<version>_<name>.sql`
- 已执行记录：`schema_migration`

当前初始迁移是 [0001_initial.sql](/Users/laysath/proj/7ma-crawler/sevenma_crawler/migrations/0001_initial.sql)。

## Commands

升级已有数据库：

```bash
uv run python -m sevenma_crawler migrate-db
```

初始化新数据库并导入默认点位：

```bash
uv run python -m sevenma_crawler prepare-db
```

## Adding a new migration

1. 在 `sevenma_crawler/migrations` 下新增一个更大版本号的 SQL 文件
2. 保持迁移可重复执行，或者只依赖 `schema_migration` 的顺序保证
3. 运行 `uv run python -m sevenma_crawler migrate-db`
4. 跑 `uv run pytest -q` 和集成测试

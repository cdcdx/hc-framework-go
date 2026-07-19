# Elasticsearch 迁移

Elasticsearch 是搜索引擎型 NoSQL 数据库，**不需要 SQL DDL 表结构迁移**。

## 为什么没有 .sql 迁移文件？

- Elasticsearch 的 Index（类似表）通过 **REST API 创建**，使用 JSON 定义 Mapping 和 Settings
- ES 不使用 SQL 语法，无法通过 golang-migrate 执行 DDL
- Index Mapping（字段类型定义）由应用层通过 ES Client 创建

## 初始化方式

当 `database.log.driver = "elasticsearch"` 时：
- 应用启动时通过 ES Client 的 `CreateIndex` API 创建索引
- Index 名称格式: `{database}_audit_logs`（如 `hc_logs_audit_logs`）
- Mapping 定义包含字段类型、分词器、时间序列配置等

## 参考

- Model 定义: `internal/model/log.go`
- 适配器: `internal/database/log/`

# MongoDB 迁移

MongoDB 是文档型 NoSQL 数据库，**不需要 SQL DDL 表结构迁移**。

## 为什么没有 .sql 迁移文件？

- MongoDB 的 Collection（类似表）在**首次写入数据时自动创建**，无需预先 `CREATE TABLE`
- MongoDB 没有固定的 Schema 约束，文档结构由应用层 Model 定义（`internal/model/user.go`）
- 索引（Index）通过应用启动时的 `EnsureIndex` 代码逻辑创建，不通过迁移脚本

## 初始化方式

当 `database.user.driver = "mongodb"` 时：
- 应用启动后首次对 `users` 集合写入文档时，MongoDB 自动创建该集合
- 索引在数据库适配器初始化时通过 Go 代码创建：
  - `user_id` (唯一索引)
  - `email` (唯一索引)
  - `username` (普通索引)
  - `status` (普通索引)

## 参考

- Model 定义: `internal/model/user.go`
- 适配器: `internal/database/user/`

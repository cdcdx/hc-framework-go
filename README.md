# HC Framework (Go) — go-zero 版

面向 C 端用户的通用高并发后台框架，由 gin 单体重构为 go-zero 架构。
当前采用**合并单进程部署**：`hc-server` 单进程承载 Gateway(REST) + 同进程后端 logic，
指标统一经 `:8080/metrics` 暴露。

> 文档导航：架构与启动见 [901_架构与启动指南.md](docs/901_架构与启动指南.md)、
> API 契约见 [903_API参考.md](docs/903_API参考.md)、配置见 [902_配置参考.md](docs/902_配置参考.md)、
> 变更记录见 [904_变更记录.md](docs/904_变更记录.md)、运维与监控见 [905_运维与监控重启.md](docs/905_运维与监控重启.md)。

## 架构

```
HTTP :8080 ──► hc-server (单进程)
                 ├─ Gateway (rest): JWT/验证码/IP限频/统一响应
                 │     └─ 进程内 LocalHcClient 直接调用后端 logic
                 ├─ 后端 (合并 user/idle/task/shop 四域)
                 └─ Metrics :8080/metrics (与 REST 共用端口)
```

- 服务发现：无需 etcd（单进程，进程内调用）
- 数据层：gorm（sqlite 零依赖起步，生产切 mysql/postgres）
- 域间协作：积分增减/任务进度为**进程内函数调用**
- 错误码：`common/errorx` 统一业务码，网关统一还原为 HTTP 响应

## 快速开始

前置：Go ≥ 1.22

```bash
# 1. 收敛依赖
go mod tidy

# 2. 启动 (无需 etcd，单进程)
make run-server
```

启动后访问：

- 健康检查：`http://localhost:8080/health`
- Prometheus 指标：`http://localhost:8080/metrics`
- Grafana 面板：`http://localhost:3000`（需先 `make mon-up`）

## 常用命令

```bash
make run-server     # 前台启动 hc-server
make build-server   # 编译到 bin/hc-server
make restart-server # 停止 → 编译 → 启动
make test           # go test ./...
make lint           # go vet + gofmt
make mon-up         # 启动监控栈 (Prometheus + Grafana)
make mon-status     # 全链路诊断
make help           # 全部命令
```

## 目录结构

```
.
├── app/
│   ├── server/cmd/main.go        # 单进程入口
│   ├── rpc/                       # 后端 logic (user/idle/task/shop)
│   │   ├── hc.proto               # 26 个 RPC 方法契约
│   │   ├── logic/                  # 业务逻辑
│   │   ├── svc/                    # 服务上下文 (DB/JWT/seed)
│   │   └── server/                 # LocalHcClient 进程内适配
│   └── gateway/api/               # REST 网关
│       ├── handler/routes.go       # 路由注册 + /metrics
│       ├── middleware/             # JWT/验证码/IP限频
│       ├── logic/                  # 网关逻辑 (薄层)
│       └── svc/                    # 网关上下文
├── common/                         # 公共库
│   ├── errorx/                     # 业务错误码
│   ├── jwt/                        # JWT 签发/校验
│   ├── captcha/                    # 验证码 (4 厂商)
│   ├── gormx/                      # GORM 连接工厂 + 池指标
│   ├── metrics/                    # 业务 Prometheus 指标
│   ├── model/                      # 数据模型
│   └── response/                   # 统一响应
├── config/server.yaml              # 单进程配置 (唯一有效配置)
├── deployments/                    # 监控栈 + 消息队列 compose
├── migrations/                     # 数据库迁移脚本
├── docs/                           # 文档 (900+ 为 go-zero 版)
└── Makefile
```

## 与原 gin 版的关系

- HTTP 契约（路径/字段/统一响应 `code/message/data/trace_id`）完全兼容
- 数据表结构不变，`migrations/` 可直接复用
- 核心业务闭环已迁移；三级缓存、MQ、WebSocket、管理端等暂未迁移
- 详见 [904_变更记录.md](docs/904_变更记录.md) §3 差异对照表

## License

MIT

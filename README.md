# HC Framework (Go) — go-zero 微服务版

面向 C 端用户的通用高并发后台框架,由 **gin 单体重构为 go-zero 微服务**:
rest 网关(HTTP 入口)+ 4 个领域 rpc(user / idle / task / shop),治理能力(链路追踪、Prometheus 指标、熔断)由 go-zero 内置。

> 详细设计、生成与启动步骤、与原版的差异对照见 [900_go-zero迁移说明.md](docs/900_go-zero迁移说明.md)。
> 完整业务文档(需求/架构/部署)仍见本仓库 docs/ 下其余文档。

## 架构

```
HTTP 请求
   │
   ▼
gateway-api (rest, :8080)   ← 鉴权/验证码/IP限频/统一响应(code+message+data+trace_id)
   │
   ├──► user-rpc  (:8001)   认证(注册/登录/Google/刷新/改密)+ 用户资料 + 积分账户
   ├──► idle-rpc  (:8002)   多设备挂机会话 + 时长结算(调 user-rpc 入账)
   ├──► task-rpc  (:8003)   任务定义/进度/领取(调 user-rpc 发奖,接收事件上报)
   └──► shop-rpc  (:8004)   商品/兑换/订单/定时抢购(调 user-rpc 扣款,上报 task-rpc)
```

- 服务发现:etcd(各 rpc 注册,网关按 Key 发现)
- 数据层:gorm(sqlite 零依赖起步,生产可切 mysql/postgres)
- 服务间:gRPC(zrpc),业务错误码跨 rpc 传递,网关统一转 HTTP

## 快速开始

前置:Go ≥ 1.22、etcd(可选,无 etcd 时改用 `Target: direct://127.0.0.1:8001` 直连模式见配置注释)、protoc 工具链(首次生成 pb)。

```bash
# 1. 生成 4 个 rpc 的 pb 代码(protoc + protoc-gen-go + protoc-gen-go-grpc)
make gen

# 2. 收敛依赖(首次克隆必做,生成 go.sum)
go mod tidy

# 3. 启动(需 etcd;或按 docs/900 改成直连模式)
make run
```

启动后访问:

- 网关: `http://localhost:8080/health`
- 各服务 Prometheus 指标: `:9100`(网关)/`:9101`~`:9104`(4 rpc)

## 常用命令

```bash
make gen        # protoc 生成 rpc pb 代码
make tidy       # go mod tidy
make build      # 编译 5 个服务到 bin/
make run        # 一键前台启动(user→idle→task→shop→gateway)
make test       # go test ./...
```

## 目录结构

```
.
├── app/
│   ├── gateway/api/          # rest 网关
│   │   ├── gateway.api       # 路由契约描述(goctl 源文件)
│   │   ├── etc/gateway.yaml
│   │   └── internal/{handler,logic,middleware,svc,config,types}
│   ├── user/rpc/             # 认证 + 用户 + 积分(user.proto)
│   ├── idle/rpc/             # 挂机(idle.proto)
│   ├── task/rpc/             # 任务(task.proto)
│   └── shop/rpc/             # 商城 + 抢购(shop.proto)
├── common/                   # 公共库(errorx 错误码/response/jwt/captcha/bcrypt/model/gormx)
├── migrations/               # 数据库迁移(沿用 gin 版)
├── docs/                     # 文档
└── Makefile
```

## 与原 gin 版的关系

- HTTP 契约(路径/请求字段/统一响应 `code/message/data/trace_id`、分页 `items/next_cursor/has_more`)完全兼容,客户端无需改动。
- 领域拆为 4 个 rpc,跨域积分与任务进度通过 gRPC 调用。
- 核心闭环已迁移;缓存三级、MQ 三件套、WebSocket、管理端、配置热更新等暂缓,详见 [900_go-zero迁移说明.md](docs/900_go-zero迁移说明.md)。

## License

MIT

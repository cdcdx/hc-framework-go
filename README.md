# HC Framework (Go) — go-zero 版

面向 C 端用户的通用高并发后台框架,由 gin 单体重构为 go-zero 微服务:
**rest 网关 + 单一领域 rpc**(合并 user / idle / task / shop 四域),治理能力(链路追踪、Prometheus 指标、熔断、优雅关闭)由 go-zero 内置。

> 文档导航:架构与启动见 [903_架构与启动指南.md](docs/903_架构与启动指南.md)、
> API 契约见 [901_API参考.md](docs/901_API参考.md)、配置见 [902_配置参考.md](docs/902_配置参考.md)、
> 变更记录见 [904_变更记录.md](docs/904_变更记录.md)、迁移演进说明见 [900_go-zero迁移说明.md](docs/900_go-zero迁移说明.md)。

## 架构

```
HTTP 请求
   │
   ▼
gateway-api (rest, :8080)   ← 鉴权/验证码/IP限频/统一响应(code+message+data+trace_id)
   │
   └──► hc-rpc (gRPC, :8001)  单一领域服务
          ├─ user 域  认证(注册/登录/Google/刷新/改密)+ 资料 + 积分账户
          ├─ idle 域  多设备挂机会话 + 时长结算(进程内入账)
          ├─ task 域  任务定义/进度/领取(进程内发奖,事件驱动进度)
          └─ shop 域  商品/兑换/订单/定时抢购(进程内扣款)
```

- 服务发现:etcd(hc-rpc 注册,网关按 Key 发现;无 etcd 可用 `Target: direct://127.0.0.1:8001` 直连)
- 数据层:gorm(sqlite 零依赖起步,生产可切 mysql/postgres)
- 域间协作:积分增减/任务进度为**进程内调用**(合并单 rpc 后不再跨进程)
- 错误码:rpc 内以 grpc status 编码业务错误码(common/errorx),网关统一还原为 HTTP 响应

## 快速开始

前置:Go ≥ 1.22、etcd(可选)、protoc 工具链(首次生成 pb)。

```bash
# 1. 生成 rpc 的 pb 代码(protoc + protoc-gen-go + protoc-gen-go-grpc)
make gen

# 2. 收敛依赖(首次克隆必做,生成 go.sum)
go mod tidy

# 3. 启动(需 etcd;或按 docs/903 改成直连模式)
make run
```

启动后访问:

- 网关: `http://localhost:8080/health`
- hc-rpc Prometheus 指标: `:9101`;网关指标: `:9100`

## 常用命令

```bash
make gen        # protoc 生成 rpc pb 代码
make tidy       # go mod tidy
make build      # 编译 2 个服务到 bin/(hc-rpc + gateway-api)
make run        # 前台启动(hc-rpc → gateway)
make test       # go test ./...
```

## 目录结构

```
.
├── app/
│   ├── rpc/                     # 单一领域 rpc(hc.proto 契约)
│   │   ├── hc.proto             # 26 个 rpc 方法(user/idle/task/shop)
│   │   ├── etc/hc.yaml
│   │   ├── hc/hc.go             # zrpc client 封装
│   │   └── internal/{server,logic,svc,config}
│   └── gateway/api/             # rest 网关
│       ├── gateway.api          # 路由契约描述(goctl 源文件)
│       ├── etc/gateway.yaml
│       └── internal/{handler,logic,middleware,svc,config,types}
├── common/                      # 公共库(errorx/response/jwt/captcha/bcrypt/model/gormx)
├── migrations/                  # 数据库迁移(沿用 gin 版表结构)
├── docs/                        # 文档(900~904 go-zero 系列)
└── Makefile
```

## 与原 gin 版的关系

- HTTP 契约(路径/请求字段/统一响应 `code/message/data/trace_id`、分页 `items/next_cursor/has_more`)完全兼容,客户端零改动。
- 数据表结构不变,migrations/ 可直接复用。
- 核心闭环已迁移;三级缓存、MQ、WebSocket、管理端、配置热更新等暂缓,详见 [900_go-zero迁移说明.md](docs/900_go-zero迁移说明.md)。

## License

MIT

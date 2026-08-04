# 900 · go-zero 迁移说明(gin → go-zero)

> 分支:`feature/go-zero-migration`(基于 `main` 的 gin 版重写)。
> 当前形态:**单进程 `hc-server`**(rest 网关 + 单一领域后端,同进程内调用,由早期 4-rpc / 双进程架构合并而来,见 904 变更记录)。
> 本文说明:如何生成缺失代码并跑起来、目录与契约设计、与原版差异与取舍。

## 1. 架构总览

```
   HTTP :8080 ────────► hc-server (单进程)
                         ├─ Gateway (rest): JwtAuth / SecurityIPLimit / Captcha / 统一响应
                         │     └─ 进程内直接调用 ──► 后端 logic（同一进程，无网络）
                         ├─ 后端（合并 user/idle/task/shop 四域）
                         │     ├─ user 域  认证/用户/积分
                         │     ├─ idle 域  挂机/结算
                         │     ├─ task 域  任务/进度/奖励
                         │     └─ shop 域  商城/兑换/抢购
                         └─ Metrics :8080/metrics（与 REST 共用端口）
```

- 网关持有 1 个**进程内** `HcRpc` 客户端(`LocalHcClient`),做参数绑定、鉴权、验证码、IP 限频、统一响应。
- 所有后端调用为**进程内函数调用**,无需 gRPC 网络、etcd 服务发现或跨进程通信。
- 仅对外暴露 REST(:8080)，Prometheus 指标统一由该端口的 `/metrics` 暴露。

## 2. 代码生成与启动(在有 go 工具链的机器上执行)

本仓库已生成 pb 代码并跑通编译;若修改 `app/rpc/hc.proto` 需重新生成(`app/rpc/hc/hc.pb.go`、`hc_grpc.pb.go`):

```bash
# 2.1 安装 protoc 工具链(一次性)
#     macOS: brew install protobuf protoc-gen-go protoc-gen-go-grpc
#     go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
#     go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest

# 2.2 生成 hc.proto 的 pb 代码(等价 make gen)
protoc --go_out=. --go_opt=paths=source_relative \
       --go-grpc_out=. --go-grpc_opt=paths=source_relative \
       app/rpc/hc.proto

# 2.3 依赖收敛(生成 go.sum)
go mod tidy

# 2.4 编译合并单进程二进制
make build-server

# 2.5 启动（无需 etcd，单进程）
make run-server
```

> 网关代码(`app/gateway/api`)为手写,`gateway.api` 仅作契约文档与 goctl 重建源;
> 若用 goctl 重新生成网关骨架(`goctl api go -api gateway.api -dir .`),会覆盖
> `internal/{handler,logic,types}` 等,需自行把中间件与响应包装合并回去。
> RPC 入口文件位于 `app/rpc/cmd/main.go`（非根目录 `hc.go`，避免与 pb 生成文件 package 冲突）。

## 3. 契约设计

| 契约文件 | 内容 |
|---|---|
| `app/rpc/hc.proto` | 26 个 rpc 方法:user 域 10(Register/Login/GoogleOAuth/RefreshToken/ChangePassword/GetProfile/UpdateProfile/GetPoints/AddPoints/DeductPoints)+ idle 域 6(Idle*)+ task 域 4(Task* + ReportProgress)+ shop 域 6(Shop* + Flash*) |
| `app/gateway/api/gateway.api` | 26 条 HTTP 路由 + 3 组中间件(公开 / SecurityIPLimit+Captcha / JwtAuth) |

HTTP 契约与原 gin 版**逐项对齐**(路径、方法、请求字段、统一响应
`{code,message,data,trace_id}`、分页 `{items,next_cursor,has_more}`)。
网关用 `protojson(UseProtoNames)` 把 rpc 返回结构转成 `snake_case` 字段名,客户端零改动。
user 域采用专用响应类型(`GetProfileResponse` 等),网关对 profile 接口解包 `resp.User`,
保证 HTTP `data` 仍为用户对象本身。

## 4. 与原 gin 版的差异对照

### 4.1 架构层

| 维度 | gin 版 | go-zero 版(当前) |
|---|---|---|
| 进程模型 | 单进程单体 | **单进程 `hc-server`**(网关 + 后端同进程) |
| 服务间调用 | 同进程函数调用 | 网关→后端为**进程内调用**(`LocalHcClient`);后端内域间为进程内调用 |
| 治理 | 自研中间件(限流/熔断/追踪/指标) | go-zero 内置(rest 熔断、otel 追踪、Prometheus、优雅关闭) |
| 配置 | viper + 自定义 Manager 热更新 | go-zero conf(`etc/*.yaml`) |
| 数据层 | gorm(多适配器) | 沿用 gorm(common/gormx 三方言) |

### 4.2 功能取舍(核心闭环内已实现 / 暂缓)

已实现(语义与原版一致,内部实现简化):
- 认证:注册/登录/Google OAuth/刷新/改密;JWT 算法与 claims 与原版完全一致。
- 用户:资料查询/更新、积分余额。
- 挂机:多设备上限、心跳、心跳超时(timeout)结算、停止结算、每日积分上限(`idle_daily_points`)、
  结算进程内入账,并新增上报挂机任务进度。
- 任务:启动 seed 7 个任务定义;daily/weekly/achievement 周期;进度原子 upsert(ON CONFLICT);
  领取进程内发奖;登录/挂机/兑换事件驱动 `ReportProgress`。
- 商城:商品分页/分类、兑换(原子条件扣库存 + 扣积分 + 建单,失败回滚补偿)、订单分页/详情(归属校验)。
- 抢购:时间窗校验、每人限购(订单计数)、原子抢配额(`sold_qty<limit_qty`)、扣积分、建单;无 Redis 预扣层。

暂缓(README 与 904 列为后续 TODO):
- 三级缓存(L1 Ristretto / L2 Redis / 布隆 / 热点 Key)与缓存防护
- 消息队列三件套(Kafka / RabbitMQ / RocketMQ)与事件总线、积分 outbox
- WebSocket 实时推送、K8s 就绪排水探针(go-zero 自带优雅关闭,ready 语义简化)
- 管理端(admin)路由、配置热更新 API、动态日志级别
- 服务端定时任务(抢购预热/超时结算——当前为心跳/停止时惰性结算)
- Redis 版限流(当前 IP 限频为进程内窗口,单实例有效;多实例需 `rest.WithLimiters` + redis)

## 5. 已知注意点

1. **错误码传递**:`common/errorx` 用 grpc status 编解码业务错误码(`status.Error(codes.Code(code), msg)`),
   网关 `errorx.Code(err)/Msg(err)` 还原,再映射 HTTP 状态(401/403/400/429/503/200+业务码)。
2. **protojson 大整数**:int64 默认输出为 JSON 数字(非字符串),与 gin 版一致。
3. **ON CONFLICT**:进度累加依赖 `gorm.io/gorm/clause.OnConflict`,sqlite/mysql/postgres 均支持。
4. **sqlite 单文件**:后端默认 `data/hc.db`;生产切 mysql/postgres 改 `config/server.yaml` 的
   `Rpc.DB.Driver/Dsn` 即可(表结构未变,migrations/ 可直接复用)。
5. **go 版本**:`go.mod` 声明 `go 1.25.0`;go-zero 要求 go ≥ 1.22,满足。
6. **go-zero 版本**:`go.mod` 锁定 `github.com/zeromicro/go-zero v1.7.x`(具体小版本以 tidy 结果为准);
   若 API 有小版本差异,集中点在 `common/errorx.go`(已改为 grpc status,不依赖 go-zero errorx 内部实现)。

## 6. 后续路线(超出本次范围)

- 网关限流升级为 go-zero `rest.WithLimiters`(Redis 令牌桶)替换进程内窗口;
- 三级缓存接入:在 rpc svc 内初始化 Ristretto/Redis,复用原 `internal/cache` 设计;
- MQ 消费与积分 outbox:恢复 `points_outbox` + Kafka 消费者;
- WebSocket 网关:go-zero 无内置 ws,可在网关用 `gorilla/websocket` 增加 `/api/v1/ws` 路由;
- 管理端与配置热更新:恢复 `/admin/flash/*` 路由与配置重载。

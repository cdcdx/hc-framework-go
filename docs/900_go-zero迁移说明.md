# 900 · go-zero 迁移说明(gin → go-zero 微服务)

> 分支:`feature/go-zero-migration`(基于 `main` 的 gin 版重写)。
> 范围:核心 API 闭环;架构形态:rest 网关 + rpc 拆分。
> 本文说明:如何生成缺失代码并跑起来、目录与契约设计、与原版差异与取舍。

## 1. 架构总览

```
                       ┌─────────────┐
   HTTP :8080 ───────► │ gateway-api │  rest 网关(go-zero)
                       └──────┬──────┘
        ┌──────────┬──────────┼──────────┬──────────┐
        ▼          ▼          ▼          ▼          ▼
   user-rpc    idle-rpc    task-rpc    shop-rpc   (etcd 服务发现)
   :8001       :8002       :8003       :8004
   认证/用户/积分 挂机会话/结算  任务/进度/奖励  商品/兑换/抢购
```

- 网关持有全部 4 个 rpc 客户端,做参数绑定、鉴权(JWT)、验证码、IP 限频、统一响应。
- rpc 之间调用:挂机/任务结算 → `user-rpc.AddPoints`;兑换扣款 → `user-rpc.DeductPoints`;
  挂机/兑换完成 → `task-rpc.ReportProgress`。
- 服务发现用 etcd;无 etcd 的环境,将各 rpc 配置里 `Etcd` 段换成
  `Target: direct://127.0.0.1:<port>` 直连即可。

## 2. 代码生成与启动(在有 go 工具链的机器上执行)

本机无 go 工具链,`*.pb.go`(protobuf 生成代码)与 `go.sum` 无法在此生成。
全部业务代码(server/logic/svc/handler/middleware/config)已按 go-zero 标准布局手写,
import 的是生成包,因此**只差 pb 生成 + 依赖收敛**:

```bash
# 2.1 安装 protoc 工具链(一次性)
#     macOS: brew install protobuf protoc-gen-go protoc-gen-go-grpc
#     go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
#     go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest

# 2.2 生成 4 个 rpc 的 pb 代码(等价 make gen)
protoc --go_out=. --go_opt=paths=source_relative \
       --go-grpc_out=. --go-grpc_opt=paths=source_relative \
       app/user/rpc/user.proto
protoc --go_out=. --go_opt=paths=source_relative \
       --go-grpc_out=. --go-grpc_opt=paths=source_relative \
       app/idle/rpc/idle.proto
protoc --go_out=. --go_opt=paths=source_relative \
       --go-grpc_out=. --go-grpc_opt=paths=source_relative \
       app/task/rpc/task.proto
protoc --go_out=. --go_opt=paths=source_relative \
       --go-grpc_out=. --go-grpc_opt=paths=source_relative \
       app/shop/rpc/shop.proto

# 2.3 依赖收敛(生成 go.sum)
go mod tidy

# 2.4 编译验证
make build

# 2.5 启动(需 etcd;前台一键)
make run
```

> 网关代码(`app/gateway/api`)为手写,`gateway.api` 仅作契约文档与 goctl 重建源;
> 若想用 goctl 重新生成网关骨架(`goctl api go -api gateway.api -dir .`),会覆盖
> `internal/{handler,logic,types}` 等,需自行把本说明的中间件与响应包装合并回去。

## 3. 契约设计

| 契约文件 | 内容 |
|---|---|
| `app/user/rpc/user.proto` | Register / Login / GoogleOAuth / RefreshToken / ChangePassword / GetProfile / UpdateProfile / GetPoints / **AddPoints** / **DeductPoints** |
| `app/idle/rpc/idle.proto` | Start / Heartbeat / Stop / StopDevice / Status / Records |
| `app/task/rpc/task.proto` | List / Progress / Claim / **ReportProgress** |
| `app/shop/rpc/shop.proto` | Items / Redeem / Orders / OrderDetail / FlashActivities / FlashRedeem |
| `app/gateway/api/gateway.api` | 22 条 HTTP 路由 + 3 组中间件(JwtAuth / SecurityIPLimit+Captcha / 公开) |

HTTP 契约与原 gin 版**逐项对齐**(路径、方法、请求字段、统一响应
`{code,message,data,trace_id}`、分页 `{items,next_cursor,has_more}`)。
网关用 `protojson(UseProtoNames)` 把 rpc 返回结构转成 `snake_case` 字段名,
保证客户端零改动。

## 4. 与原 gin 版的差异对照

### 4.1 架构层

| 维度 | gin 版 | go-zero 版 |
|---|---|---|
| 进程模型 | 单进程单体 | 网关 + 4 rpc 共 5 进程 |
| 服务间调用 | 同进程函数调用 | gRPC(zrpc),etcd 服务发现 |
| 治理 | 自研中间件(限流/熔断/追踪/指标) | go-zero 内置(rest 熔断、otel 追踪、Prometheus 指标、优雅关闭) |
| 配置 | viper + 自定义 Manager 热更新 | go-zero conf(`etc/*.yaml`,热更新能力保留于 rest) |
| 数据层 | gorm(多适配器) | 沿用 gorm(common/gormx 三方言) |

### 4.2 功能取舍(核心闭环内已实现 / 暂缓)

已实现(语义与原版一致,内部实现简化):
- 认证:注册/登录/Google OAuth/刷新/改密;JWT 算法与 claims 与原版完全一致(旧 token 可复用)。
- 用户:资料查询/更新、积分余额。
- 挂机:多设备上限、心跳、心跳超时(timeout)结算、停止结算、每日积分上限(`idle_daily_points`)、
  结算经 `user-rpc.AddPoints` 入账(不再走 outbox/MQ)。
- 任务:启动 seed 7 个任务定义;daily/weekly/achievement 周期;进度原子 upsert(ON CONFLICT);
  领取经 `user-rpc.AddPoints` 发奖;登录/挂机/兑换事件经 `ReportProgress` 驱动。
- 商城:商品分页/分类、兑换(原子条件扣库存 + 扣积分 + 建单,失败回滚补偿)、订单分页/详情(归属校验)。
- 抢购:时间窗校验、每人限购(订单计数)、原子抢配额(`sold_qty<limit_qty`)、扣积分、建单;无 Redis 预扣层。

暂缓(README 声明为后续 TODO):
- 三级缓存(L1 Ristretto / L2 Redis / 布隆 / 热点 Key)与缓存防护
- 消息队列三件套(Kafka / RabbitMQ / RocketMQ)与事件总线、积分 outbox
- WebSocket 实时推送、K8s 就绪排水探针(go-zero 自带优雅关闭,ready 语义简化)
- 管理端(admin)路由、配置热更新 API(`/debug/reload`)、动态日志级别
- 服务端定时任务(抢购预热/超时结算——当前改为心跳/停止时惰性结算)
- Redis 版限流(当前 IP 限频为进程内窗口,单实例有效;多实例需换 `rest.WithLimiters` + redis)

## 5. 已知注意点

1. **错误码传递**:rpc 服务端返回 `errorx.New(code, msg)`(内部用 go-zero `core/errorx`),
   跨 gRPC 编码为 status;网关 `errorx.Code(err)/Msg(err)` 还原,再映射 HTTP 状态
   (401/403/400/429/503/200+业务码),与 gin 版响应一致。
2. **protojson 大整数**:int64 默认输出为 JSON 数字(非字符串),与 gin 版一致。
3. **ON CONFLICT**:进度累加/幂等依赖 `gorm.io/gorm/clause.OnConflict`,sqlite/mysql/postgres 均支持。
4. **sqlite 单文件**:各 rpc 默认独立 `data/*.db`,生产切 mysql/postgres 后改为共用库
   (改 `etc/*.yaml` 的 `DB.Driver/Dsn` 即可,表结构未变,migrations/ 可直接复用)。
5. **go 版本**:`go.mod` 声明 `go 1.25.0`(与原仓库一致);go-zero v1.7.5 要求 go ≥ 1.22,满足。
6. **go-zero 版本与 API 兼容**:`go.mod` 锁定 `github.com/zeromicro/go-zero v1.7.5`——若该精确版本不存在
   (go-zero 版本节奏较快),`go mod tidy` 时把版本改为任一最新 `v1.7.x` 即可,代码 API 均按 v1.7 稳定接口书写。
   唯一可能随小版本波动的点是 `common/errorx.go` 中对 `core/errorx` 的调用
   (`New/Newf/CodeFromError/MsgFromError`);若编译报签名不匹配,只需按该版本实际签名微调这两个函数,
   其余代码不受影响。

## 6. 后续路线(超出本次范围)

- 网关限流升级为 go-zero `rest.WithLimiters`(Redis 令牌桶)替换进程内窗口;
- 三级缓存接入:在 rpc svc 内初始化 Ristretto/Redis,复用原 `internal/cache` 设计;
- MQ 消费与积分 outbox:恢复 `points_outbox` + Kafka 消费者(go-zero mq 或自研);
- WebSocket 网关:go-zero 无内置 ws,可在网关用 `gorilla/websocket` 增加 `/api/v1/ws` 路由;
- 管理端与配置热更新:恢复 `/admin/flash/*` 路由与 viper 热更新等价物(go-zero 配置重载)。

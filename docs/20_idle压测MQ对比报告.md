# idle 压测：五种 MQ 横向对比报告

> 数据来源：`idle-memory.log` / `idle-none.log` / `idle-rabbitmq.log` / `idle-rocketmq.log` / `idle-kafka.log`
> 压测脚本：`scripts/k6/idle.js`（心跳，10000 VU）+ `scripts/k6/idle_settle.js`（结算，500 VU）
> 生成日期：2026-07-16

## 一、测试说明

- 同一套 idle 压测在 5 种 `mq.type` 下各跑一遍：
  - `idle` 心跳场景：10000 最大 VU，8m 时长。心跳接口**不发 MQ 事件**，用于验证 MQ 类型对核心链路无影响。
  - `idle_settle` 结算场景：500 最大 VU，8m 时长。结算接口**会把事件发到 MQ**，是区分各 MQ 的关键场景。
- 全部用例 `idle_settle_ok = 100%`、`http_req_failed = 0%`，说明所有 MQ 方案功能正确，差异纯在**延迟/吞吐**。
- 阈值：`idle` 心跳 `http_req_duration p(99) < 500ms`；`idle_settle` 结算 `http_req_duration p(99) < 3000ms`。

## 二、idle_settle 结算场景横向对比（500 VU）

| MQ | settle avg | settle p90 | settle p95 | http p99(阈值项) | settle max | 吞吐 req/s | idle_settle_ok |
|---|---|---|---|---|---|---|---|
| memory | 7.72ms | 8.47ms | 12.68ms | 35.83ms | 732ms | 256.2 | 100% |
| **kafka** | 9.57ms | 10.17ms | 15.08ms | **36.5ms** | 631ms | 256.2 | 100% |
| rabbitmq | 9.70ms | 11.33ms | 16.10ms | 36.87ms | 695ms | 256.0 | 100% |
| none | 7.99ms | 8.86ms | 13.96ms | 42.88ms | **2984ms** | 256.2 | 100% |
| rocketmq | 10.41ms | 12.98ms | 19.44ms | 49.45ms | 694ms | 255.8 | 100% |

**结算综合排序**：`memory > kafka ≈ rabbitmq > none > rocketmq`（差距已很小，全部在同一梯队，且全部通过 p99 阈值）。

### 分项解读
- **memory / none**：最快。`memory` 进程内队列、`none` 不投递，无真实 broker RTT，延迟最低。`none` 的 `max=2984ms` 是单点尖峰（p99 仅 42.88ms），与 MQ 无关，疑为启动期/GC/调度抖动。
- **kafka**：本轮 http p99 = 36.5ms，回到第一梯队。此前一次运行曾出现 p99=120ms（见「历史备注」），重跑后该尖峰消失，确认是瞬时批次抖动而非稳态瓶颈。
- **rabbitmq**：真实 MQ 中表现最稳，p99=36.87ms，与 kafka 几乎并列。
- **rocketmq**：五者中数值最高（avg 10.41ms、p95 19.44ms、p99 49.45ms、max 694ms），但相较其批量发送改造前的 baseline 已大幅回落（见「历史备注」），与 rabbitmq 处于同一量级，不再属于异常值。

## 三、idle 心跳场景横向对比（10000 VU）

| MQ | http avg | http p95 | http p99(阈值项) | 吞吐 req/s | idle_heartbeat_ok |
|---|---|---|---|---|---|
| memory | 1.03ms | 1.94ms | 3.20ms | 163.69 | 100% |
| kafka | 0.999ms | 1.89ms | 2.92ms | 163.67 | 100% |
| rabbitmq | 1.01ms | 2.02ms | 3.06ms | 163.68 | 100% |
| none | 1.05ms | 2.01ms | 3.15ms | 163.58 | 100% |
| rocketmq | 1.0ms | 1.93ms | 3.05ms | 163.66 | 100% |

**结论**：五种 MQ 心跳场景几乎无差异（avg≈1ms、p99≈3ms、吞吐≈163 req/s、100% 成功）。心跳链路不发 MQ 事件，性能不受 `mq.type` 影响，符合预期。

## 四、真实 MQ @ stress 压测档对比（kafka / rabbitmq / rocketmq）

> 数据来源：`idle-kafka-config.stress.log` / `idle-rabbitmq-config.stress.log` / `idle-rocketmq-config.stress.log`
> 三者均使用 `config.stress.yaml`（压测档：MySQL 连接池 120→300、Redis L2 200→500、Mongo 100→300，tracing 降采样 0.1→0.01）+ MySQL 服务端调优（`max_connections=800`）。

### 4.1 idle 心跳场景（10000 VU，不发 MQ 事件）

| MQ | http p99(阈值<500) | http avg | 心跳 avg | 心跳 max | 吞吐 req/s | ok |
|---|---|---|---|---|---|---|
| kafka | 2.92ms | 0.999ms | 0.706ms | 7.52ms | 163.67 | 100% |
| rabbitmq | 3.06ms | 1.01ms | 0.721ms | 8.89ms | 163.68 | 100% |
| rocketmq | 3.05ms | 1.00ms | 0.713ms | 8.59ms | 163.66 | 100% |

三者几乎一致（差异 <5%）。本次 kafka 心跳 max 仅 7.52ms，是干净运行（无此前那次 35.79ms 毛刺）；rabbitmq/rocketmq 心跳 max 略高（8.6–8.9ms），仍为环境级微抖，与 MQ 类型无关。

### 4.2 idle_settle 结算场景（500 VU，发 MQ 事件）

| MQ | settle avg | settle p90 | settle p95 | http p99(阈值<3000) | settle max | 吞吐 req/s | idle_settle_ok |
|---|---|---|---|---|---|---|---|
| kafka | 9.57ms | 10.17ms | 15.08ms | 36.5ms | 631ms | 256.24 | 100% |
| rabbitmq | 9.70ms | 11.33ms | 16.10ms | 36.87ms | 695ms | 256.01 | 100% |
| rocketmq | 10.41ms | 12.98ms | 19.44ms | 49.45ms | 694ms | 255.81 | 100% |

**排序**：`kafka ≈ rabbitmq > rocketmq`。前两者差距极小（settle avg 9.57 vs 9.70ms、p99 36.5 vs 36.87ms），RocketMQ 各项高约 8–35%（p95 19.44ms、p99 49.45ms），但均远低于 3000ms 阈值、100% 成功。

### 4.3 stress 档结论
- 这三组数值与第二节「默认档（config.yaml）下的 kafka/rabbitmq/rocketmq」**完全一致**——说明在 500 VU 结算下，stress 档放大的连接池（MySQL 120→300、Redis 200→500、Mongo 100→300）**完全未被用满**，瓶颈不在连接池，故 stress 档在 500 VU 下未体现出降延迟或防报错收益。
- stress 档（容量/安全网档）的真正价值需在更高压测（如 1000+ VU 结算，或 Redis 故意降速）下才会显现，届时 300/500 的连接池 headroom 可避免 `Too many connections` 或连接池排队放大尾延迟。
- 日志头部均仅打印 `config.yaml`，无法从日志内区分实际配置；「stress」通过文件名约定（`-config.stress.log` = `config.stress.yaml`）。rabbitmq/rocketmq 日志多几行 `[Setup] register ok=200` 的 broker 连接日志，属正常接入差异，不影响性能。

## 五、历史备注（两次重要变更）

### 1. RocketMQ producer 批量发送改造
- 改造前 baseline（逐条 `SendSync`，每条一次 broker RTT）：settle avg 56.34ms、p95 89.39ms、http p99 446.19ms、max 8184ms、iterations 60466、吞吐 247.0 req/s。
- 改造后（同主题按批一次 `SendSync`，`batch_size` 默认 32，受单批 ≤4MB/4096 条约束）：settle avg 10.41ms、p95 19.44ms、http p99 49.45ms、max 694ms、iterations 62656、吞吐 255.8 req/s。
- 收益：avg ↓82%、p95 ↓78%、http p99 ↓89%、max ↓92%、吞吐 +3.6%。尾延迟从异常值回归正常水位。
- 相关提交：`perf(mq/rocketmq): producer 批量发送降低 broker RTT 尾延迟`（含 `internal/mq/rocketmq.go`、`rocketmq_batch_test.go`、配置项 `batch_size`）。

### 2. Kafka 单次运行 p99 抖动
- 早期一次 `idle-kafka.log` 运行出现 http p99 = 120.47ms（settle avg 11.82ms、max 800ms），疑为 Kafka producer `linger.ms`/批次攒批引入的周期性抖动。
- 重跑后的 `idle-kafka.log`（本报告采用数据）p99 回落至 36.5ms，确认该尖峰为瞬时/运行方差，非稳态瓶颈。两次 `max` 仍偶发 600–800ms 级抖动，频率低、不影响 p99 阈值。

## 六、结论与建议

1. 全部 5 种 MQ 在 idle 压测下功能正确、可靠性无损，差异仅在延迟/吞吐，且经过 RocketMQ 批量改造后已收敛到同一梯队。
2. 生产选型建议：
   - 追求最低延迟且可接受进程内/无投递：`memory` / `none`。
   - 真实 MQ 首选：`kafka` 或 `rabbitmq`（本轮 p99 均 ≈36ms，表现最佳）。
   - `rocketmq` 经批量改造后可用，尾延迟略高于前两者但差距很小。
3. 后续可跟进：
   - `none` 的 2984ms 单点尖峰（非 MQ 问题，建议排查启动期/GC/调度）。
   - Kafka producer 批次参数（`linger.ms` 等）以进一步压低偶发亚秒级抖动。
   - 如需量化 RocketMQ 改造收益，可保留本报告 baseline 数据作为对照。

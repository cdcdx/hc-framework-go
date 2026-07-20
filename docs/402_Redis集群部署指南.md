# Redis 集群部署指南（挂机心跳 / 离线检测）

本文档说明 `hc-framework-go` 的挂机心跳判活、离线检测、事件驱动结算在 **Redis Cluster（及 Sentinel）** 模式下的部署要点与容量评估。

> 相关能力：心跳去 DB 化（`idle:hb:*`）、活跃会话集合分片（`idle:active:set:{shard}`）、离线检测轮询 scanner、Keyspace Notification 事件驱动结算、集群安全的 `Manager.Delete`（Pipeline）。

---

## 一、三种 Redis 拓扑的取舍

| 拓扑 | 配置 | 事件驱动结算 | 离线检测 | 适用规模 |
| --- | --- | --- | --- | --- |
| 单机 / 主从 | `cache.l2.type=redis` | ✅ 支持（零轮询、近实时） | ✅ 轮询 scanner | 中小规模（百万级心跳以内） |
| Sentinel | `cache.l2.sentinel.enabled=true` | ✅ 支持（订阅连到 master） | ✅ 轮询 scanner | 中大规模、需高可用 |
| **Cluster** | `cache.l2.cluster.enabled=true` + `addresses` | ⚠️ **自动禁用** | ✅ 轮询 scanner（分片集合） | 大规模、需水平扩展 |

**关键点**：

- **Cluster 模式自动禁用事件驱动结算**。原因：Redis Cluster 每个 master 只对自己 slot 上的 key 发布 keyspace 事件，订阅者无法跨节点收齐 `expired` 事件，会导致漏结算。代码在 `StartEventDriven` 中检测到 `cache.l2.cluster.enabled=true` 即 `return`，回退为轮询 scanner 兜底。
- 若必须在 Cluster 下实现"到期即结算"，需改用 **Hash Tag** 把心跳 key 与订阅绑定到同一 slot，或引入独立的"到期即写"双写队列——属定制化改造，默认不开启。

---

## 二、启用 Redis Cluster

`config/config.yaml`：

```yaml
cache:
  l2:
    enabled: true
    type: redis
    cluster:
      enabled: true
      # 仅需填任意若干 seed 节点，go-redis 会自动发现集群拓扑
      addresses:
        - "10.0.1.11:6379"
        - "10.0.1.12:6379"
        - "10.0.1.13:6379"
    # Cluster 模式下以下字段仍生效：pool_size / dial_timeout / read_timeout / write_timeout
    pool_size: 64
idle:
  redis_key_prefix: "idle:active"
  active_set_shards: 256        # 活跃集合分片数
  event_driven_settle: true     # Cluster 下会被代码自动忽略，保留无副作用
  heartbeat_interval: "30s"
  timeout_threshold: "90s"      # 与基线假设一致（见 07_集群设备部署手册.md §1）
  offline_check_interval: "*/30 * * * *" # 轮询 scanner 兜底扫描（被钳制到 [timeout/3, 2×timeout]）
```

`L2` 客户端使用 `redis.UniversalClient`，**同一份代码**在单机 / Sentinel / Cluster 间通过配置自动路由，无需改代码。

---

## 三、集群安全要点（已实现，部署时无需改动）

1. **活跃集合分片** `idle:active:set:{shard}`（`shard = fnv32a(userID) % active_set_shards`）。
   - 心跳 key 已含 `userID`，集群下天然按 userID 分散到不同 slot，**本就分片**。
   - 真正会被打散的是方案引入的「活跃会话集合」——单一大集合会落单 slot 成为热点/大 key，分片后分散到多节点。
   - `ScanActiveSessions` 遍历全部分片并合并，离线检测负载随之分散。
2. **`Manager.Delete` 改为 Pipeline**：集群下多 key 若跨 slot，`client.Del` 会报 `CROSSSLOT`。Pipeline 让 go-redis 按 slot 自动分组提交，同时兼容单机与集群。
3. **事件驱动结算的跨 slot 风险已规避**：Cluster 下不订阅 keyspace 事件（见 §1）。

---

## 四、容量评估

### 4.1 内存占用

每个在线挂机设备占用一个心跳 key：

```
单 key ≈ 心跳 value(8B unix 时间戳) + Redis 开销(~50~80B 元数据)
≈ 70 B / 会话（取上限 100 B 估算）
```

| 在线会话数 | 心跳 key 内存 | 活跃集合成员（按 256 分片，含 `userID\x1fdeviceID` 约 40B） |
| --- | --- | --- |
| 10 万 | ~10 MB | ~4 MB |
| 100 万 | ~100 MB | ~40 MB |
| 1000 万 | ~1 GB | ~400 MB |

> 心跳 key 均带 `TTL=timeout_threshold`，离线即过期自动回收，内存只与**瞬时在线数**正相关，与历史总量无关。

### 4.2 集群节点数建议

按「每节点承载 200~300 万在线会话、内存占用 ~300 MB」粗估（留足 buffer 与故障转移余量）：

| 在线会话数 | 建议 master 节点数（3 副本） |
| --- | --- |
| ≤ 100 万 | 3 |
| ≤ 500 万 | 3~6 |
| ≤ 2000 万 | 6~12 |

### 4.3 带宽 / QPS

- **心跳写入**：每会话每 `heartbeat_interval` 一次 `SETEX`。
  - 100 万在线、30s 间隔 → ~3.3 万 SET/s（分散到各节点后单节点约 1 万 SET/s，压力极低）。
- **离线检测（轮询 scanner）**：每 `offline_check_interval` 一次，遍历分片集合做 `SSCAN` + **批量 Pipeline `EXISTS`**（`L2.BatchExists`，默认每批 100），仅离线会话才回源 DB 结算；百万活跃会话的判活 RTT 从百万次降到数百次。
  - 集合分片后，扫描压力按 shard 分散；多副本下按 Pod 分片（`scan_sharding`）进一步线性分摊，单节点 `SSCAN` 数据量小、不阻塞。

---

## 五、健康检查与验证

- **L2 健康**：`GET /health`（或 `Manager.Health`）返回 `l2_ok` 与 `l2_latency_ms`。
- **事件驱动（单机/Sentinel）验证**：
  1. 确认 Redis 服务端已开启 `notify-keyspace-events Ex`（自托管可执行 `CONFIG SET notify-keyspace-events Ex`；云 Redis 需在控制台预开启，应用 `ConfigSet` 可能被拒——仅告警不阻断）。
  2. 启动日志应出现：`idle event-driven settle subscribed channel=__keyevent@<db>__:expired`。
  3. 让某会话停止心跳，观察在 `timeout_threshold` 内（毫秒级）完成结算，而非等下一个 scanner 周期。
- **集群模式验证**：启动日志**不应**出现上述 subscribed 日志（确认事件驱动已自动禁用）；离线结算由 scanner 按 `offline_check_interval` 兜底完成。

---

## 六、运维注意事项

- **`active_set_shards` 为启动期固定值**：运行中改配置需重启，且若变更分片数需配套迁移旧集合成员（实践极少变动，未做在线迁移）。
- **Key 前缀隔离**：`cache.key_prefix` / `idle.redis_key_prefix` 用于多环境隔离，避免测试/生产 key 冲突。
- **云平台 Redis**：`CONFIG` 命令常被禁用，事件驱动所需的 `notify-keyspace-events` 必须在服务端/控制台预先开启。
- **多实例部署**：事件驱动结算经分布式锁（`idle:settle-lock:{userID}:{deviceID}`）+ `settleSession` 乐观锁去重，**无需选主**；轮询 scanner 在多实例下同样幂等，可安全并发运行。

---

## 七、相关文档

- [101_架构说明.md](./101_架构说明.md) §6.4 — 异步事件与协调级联（选主 / 死分片接管）。
- [403_可观测性与告警.md](./403_可观测性与告警.md) — `idle_scan_*` / `idle_settle_total` 指标清单与告警规则。
- [202_配置参考.md](./202_配置参考.md) §2.2 / §11.2 — `cache.l2.*` 与 `idle.*` 全量字段。
- [401_集群设备部署手册.md](./401_集群设备部署手册.md) §4.1 / §11 — Redis Cluster 物理拓扑与 K8s 清单。

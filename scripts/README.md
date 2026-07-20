# Scripts 脚本目录

本目录包含项目构建、部署、压测和数据迁移相关的可执行脚本。

## 目录结构

```
scripts/
├── README.md                  # 本文件
├── migrate.sh                 # 数据库迁移执行脚本
├── capacity_verify.sh         # 容量验证（汇总各 Pod /metrics）
├── k6/                        # k6 压力测试脚本
│   ├── auth.js                # 场景 1：注册/登录压测
│   ├── idle.js                # 场景 2：挂机心跳压测
│   ├── idle_settle.js         # 场景 3：挂机结算洪峰压测
│   ├── shop.js                # 场景 4：兑换抢购压测
│   ├── mixed.js               # 场景 5：混合负载压测
│   ├── shop_flash.js          # 场景 6：定时抢购压测（零超卖 + 削峰缓存拦截）
│   └── shop_flash.sh          # 场景 6 一键运行包装（k6 run 封装）
├── loadtest-heartbeat/        # 百万设备心跳压测（Go）
│   └── main.go
└── loadtest-settle/           # 百万设备结算洪峰压测（Go）
    └── main.go
```

## 脚本说明

### migrate.sh — 数据库迁移

对 4 个数据域（user / business / log / monitor）执行 `golang-migrate` 迁移。

```bash
# 全部域 up
bash scripts/migrate.sh up all

# 指定域 up / down
bash scripts/migrate.sh up business
bash scripts/migrate.sh down user

# 或通过 Makefile
make migrate                # 全部 up
make migrate DB=business    # 指定域
```

### capacity_verify.sh — 容量验证

逐 Pod 抓取 `/metrics`，汇总 `idle_scan_members_total` / `idle_scan_dead_total` / `idle_settle_total`，用于验证部署后的实际在线容量。

```bash
bash scripts/capacity_verify.sh
```

### k6/ — 压力测试

基于 [k6](https://k6.io) 的自动化压测脚本，5 个场景覆盖完整业务链路。

```bash
# 安装 k6（Linux）
sudo apt-get install k6

# 运行全部 5 个场景
make k6-test

# 单独运行某个场景
make k6-test-auth         # 注册/登录
make k6-test-idle         # 挂机心跳 + 结算
make k6-test-idle-settle  # 结算
make k6-test-shop         # 兑换抢购（验证零超卖）
make k6-test-shop-flash   # 定时抢购（场景 6：零超卖 + 削峰缓存拦截）
make k6-test-mixed        # 混合负载（70% 读 + 30% 写）

# 自定义参数（k6 --env）
k6 run --env BASE_URL=http://192.168.1.100:8080 scripts/k6/auth.js
k6 run --env ITEM_ID=2 scripts/k6/shop.js
```

### 场景 6：定时抢购压测 `shop_flash.js`

验证定时抢购（`/api/v1/shop/flash/redeem`）的**零超卖**与**削峰缓存拦截**：

- **两道防线**：Redis 原子预扣（削峰，超额返回 `10307`/`10308` 不触达 DB）+ DB 权威配额（兜底，`sold_qty < limit_qty` 才落单）。
- **零超卖硬标准**：活动 `sold_qty <= limit_qty`（teardown 经运营详情接口查得）。
- **门禁阈值**：`flash_redeem_success count<=500`、`db_final_sold_qty value>=0 && <=500`、`http_req_duration p99<2s`。

```bash
# 方式一（推荐）：setup 阶段经运营接口自动创建活动，需 admin.token
ADMIN_TOKEN=<your-admin-token> bash scripts/k6/shop_flash.sh

# 方式二：复用已存在活动（绕开 token；teardown 服务端对账会跳过）
FLASH_ACTIVITY_ID=7 bash scripts/k6/shop_flash.sh

# 一键包装支持的环境变量：BASE_URL / ITEM_ID / FLASH_ITEM_ID / LIMIT_QTY /
# PRICE_POINTS / PER_USER_LIMIT / END_TIME / K6_ARGS（透传 k6 额外参数）

# 或通过 Makefile（会先重置关联商品库存，确保每轮可验证零超卖）
make k6-test-shop-flash ADMIN_TOKEN=<your-admin-token>
make k6-test-shop-flash FLASH_ACTIVITY_ID=7
```

前置：商品 `FLASH_ITEM_ID`（默认复用 `ITEM_ID=1`）需 `is_active=1`、`stock>=LIMIT_QTY`；`config.admin.token` 需配置（方式一）。

验收指标见 [002_需求规格说明书.md](docs/002_需求规格说明书.md) §9.5 与 [302_定时抢购高并发方案.md](docs/302_定时抢购高并发方案.md)。

### loadtest-heartbeat/ — 百万设备心跳压测（Go）

模拟 N 台设备以 30s 间隔周期性发送心跳，统计 RPS / P50 / P95 / P99 延迟。
本地 HS256 签发 JWT，无需预先创建账号。

```bash
go run ./scripts/loadtest-heartbeat \
  -base http://localhost:8080 \
  -devices 1000000 -interval 30s -duration 5m \
  -secret <your-jwt-signing-key>
```

### loadtest-settle/ — 百万设备结算洪峰压测（Go）

模拟大量设备同时停止挂机触发结算，验证结算幂等性与积分最终一致性。

```bash
go run ./scripts/loadtest-settle \
  -base http://localhost:8080 \
  -devices 10000 -duration 2m \
  -secret <your-jwt-signing-key>
```

## 前置条件

- `migrate.sh`：需要 `python3` + `PyYAML`，以及 `golang-migrate`（通过 `go run` 自动拉取）。
- `k6/`：需要安装 [k6](https://k6.io/docs/get-started/installation/)。
- `loadtest-*/`：Go 项目内的脚本，可直接 `go run`。
- `capacity_verify.sh`：需要 `kubectl` 访问运行中的集群，以及 `curl` / `jq`。

## 相关文档

- 数据库迁移机制：[203_数据库迁移.md](docs/203_数据库迁移.md)
- 部署与压测指标：[401_集群设备部署手册.md](docs/401_集群设备部署手册.md) §12
- 验收标准：[002_需求规格说明书.md](docs/002_需求规格说明书.md) §9.5

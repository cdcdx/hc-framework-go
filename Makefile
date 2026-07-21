.PHONY: all build run run-stress test test-idle lint clean docker-build docker-run migrate help k6-test k6-test-auth k6-test-idle k6-test-idle-settle k6-test-shop k6-test-shop-flash k6-test-mixed k6-test-ws mysql-stress-tune stress-idle-settle

# 项目变量
APP_NAME := hcf-server
BUILD_DIR := ./bin
CMD_DIR := ./cmd/server
MAIN_FILE := $(CMD_DIR)/main.go

# 重置压测商品(id=1)库存分桶的 SQL（P2-5 分桶以桶为权威）。
# 仅改 shop_items.stock 列不会同步桶，导致 /shop/items 返回的桶之和仍为旧值，
# 压测预言机会误判“超卖”。本 SQL 把桶清空后按当前 shop_items.stock 列值重建 16 个均分桶
# （余数前置到前若干桶，与 Go 侧 makeBuckets 语义一致）。需 MySQL 8+（递归 CTE）；
# 若切换 SQLite/Postgres，请相应调整取模/整除语法（MOD/DIV -> % // FLOOR）。
RESET_SHOP_BUCKETS_SQL = DELETE FROM shop_item_stock_buckets WHERE item_id=1; INSERT INTO shop_item_stock_buckets (item_id, bucket, stock) WITH RECURSIVE seq(n) AS (SELECT 0 UNION ALL SELECT n+1 FROM seq WHERE n < 15) SELECT 1, seq.n, CASE WHEN seq.n < (SELECT MOD(stock,16) FROM shop_items WHERE id=1) THEN (SELECT stock DIV 16 FROM shop_items WHERE id=1) + 1 ELSE (SELECT stock DIV 16 FROM shop_items WHERE id=1) END FROM seq;

# 版本信息：构建时通过 -ldflags -X main.version / -X main.commit 注入 hc_build_info。
# 可用 `make build VERSION=v1.2.3 COMMIT=abc1234` 覆盖版本/commit（覆盖时不带 -dirty）。
VERSION ?= 0.1.0
COMMIT  ?= $(shell \
  c=$$(git rev-parse --short HEAD 2>/dev/null); \
  if [ -n "$$c" ] && [ -n "$$(git status --porcelain 2>/dev/null)" ]; then c="$$c-dirty"; fi; \
  echo "$${c:-unknown}" \
)

# 编译参数
CGO_ENABLED := 0
GOOS := linux
GOARCH := amd64
LDFLAGS := -s -w -X main.version=$(VERSION) -X main.commit=$(COMMIT)
BUILD_FLAGS := -trimpath

# Docker 变量
DOCKER_IMAGE := $(APP_NAME)
DOCKER_TAG := latest

# 默认目标
all: lint test build

## build: 编译二进制文件
build:
	@echo "Building $(APP_NAME)..."
	@mkdir -p $(BUILD_DIR)
	CGO_ENABLED=$(CGO_ENABLED) GOOS=$(GOOS) GOARCH=$(GOARCH) \
		go build $(BUILD_FLAGS) -ldflags="$(LDFLAGS)" -o $(BUILD_DIR)/$(APP_NAME) $(MAIN_FILE)
	@echo "Build complete: $(BUILD_DIR)/$(APP_NAME)"

## build-local: 本地编译（当前系统架构，启用 CGO 以支持 SQLite）
build-local:
	@echo "Building $(APP_NAME) for local platform..."
	@mkdir -p $(BUILD_DIR)
	CGO_ENABLED=1 go build $(BUILD_FLAGS) -ldflags="$(LDFLAGS)" -o $(BUILD_DIR)/$(APP_NAME) $(MAIN_FILE)
	@echo "Build complete: $(BUILD_DIR)/$(APP_NAME)"

## run: 运行（开发模式）
run:
	@echo "Starting $(APP_NAME)..."
	go run -ldflags="$(LDFLAGS)" $(MAIN_FILE)

## run-stress: 用压测容量档配置启动（config/config.stress.yaml：连接池 pool=300、mq.type=kafka 等）
run-stress:
	@echo "Starting $(APP_NAME) with STRESS profile (config/config.stress.yaml)..."
 	# GC 调优：压测 10000 长连接常驻会撑大 Go 堆，默认 GOGC=100 下 GC 频繁触发，并发 mark/mark-assist 与万级请求 goroutine 争 CPU，放大心跳尾延迟（idle.js 注释实测 p99≈364ms）。
 	# GOGC=200 将 GC 频率减半、零 OOM 风险，直接压低心跳 p99/p95 长尾；可用环境变量覆盖（如 GOGC=off GOMEMLIMIT=2GiB 进一步压尾，须保证容器内存充足）。
	GOGC=$${GOGC:-200} go run -ldflags="$(LDFLAGS)" $(MAIN_FILE) -config config/config.stress.yaml

## run-sqlite: 使用 SQLite 模式运行
run-sqlite:
	@echo "Starting $(APP_NAME) with SQLite..."
	APP_DATABASE_USER_DRIVER=sqlite \
	APP_DATABASE_BUSINESS_DRIVER=sqlite \
	APP_DATABASE_LOG_DRIVER=sqlite \
	APP_DATABASE_MONITOR_DRIVER=sqlite \
	APP_MQ_TYPE=none \
	APP_CACHE_L2_ENABLED=false \
	go run -ldflags="$(LDFLAGS)" $(MAIN_FILE)

## run-mysql: 使用 MySQL 模式运行
run-mysql:
	@echo "Starting $(APP_NAME) with MySQL..."
	APP_DATABASE_USER_DRIVER=mysql \
	APP_DATABASE_BUSINESS_DRIVER=mysql \
	APP_DATABASE_LOG_DRIVER=mysql \
	APP_DATABASE_MONITOR_DRIVER=mysql \
	APP_MQ_TYPE=none \
	APP_CACHE_L2_ENABLED=true \
	go run -ldflags="$(LDFLAGS)" $(MAIN_FILE)

## test: 运行测试
test:
	@echo "Running tests..."
	go test -race -coverprofile=coverage.out ./...

## test-idle: 仅运行挂机(idle)与心跳批量落库相关测试
test-idle:
	@echo "Running idle/heartbeat tests..."
	go test -race -coverprofile=coverage.out ./internal/service/... -run 'TestScan|TestIdle|TestHeartbeat|TestStartEventDriven|TestHandleHB'

## test-cover: 运行测试并打开覆盖率报告
test-cover: test
	go tool cover -html=coverage.out -o coverage.html
	@echo "Coverage report: coverage.html"

## test-integration: 运行集成测试（需要 testcontainers）
test-integration:
	@echo "Running integration tests..."
	@if [ -d test ]; then \
		go test -race -tags=integration ./test/...; \
	else \
		echo "No test/ directory found, searching project-wide for integration tests..."; \
		go test -race -tags=integration ./...; \
	fi

## lint: 代码静态检查
lint:
	@echo "Running golangci-lint..."
	@which golangci-lint >/dev/null 2>&1 || { echo "ERROR: golangci-lint not installed. Install: go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest"; exit 1; }
	golangci-lint run ./...

## fmt: 格式化代码
fmt:
	@echo "Formatting code..."
	go fmt ./...
	@which goimports >/dev/null 2>&1 && goimports -w . || go fmt ./...

## tidy: 整理依赖
tidy:
	@echo "Tidying dependencies..."
	go mod tidy

## clean: 清理构建产物
clean:
	@echo "Cleaning..."
	@rm -rf $(BUILD_DIR)
	@rm -f coverage.html coverage.out

## docker-build: 构建 Docker 镜像
docker-build:
	@echo "Building Docker image $(DOCKER_IMAGE):$(DOCKER_TAG)..."
	docker build -t $(DOCKER_IMAGE):$(DOCKER_TAG) .

## docker-run: 运行 Docker 容器
docker-run:
	@echo "Running Docker container $(DOCKER_IMAGE):$(DOCKER_TAG)..."
	docker run -p 8080:8080 \
		-e APP_DATABASE_USER_DRIVER=sqlite \
		-e APP_DATABASE_BUSINESS_DRIVER=sqlite \
		-e APP_DATABASE_LOG_DRIVER=sqlite \
		-e APP_DATABASE_MONITOR_DRIVER=sqlite \
		-e APP_CACHE_L2_ENABLED=false \
		$(DOCKER_IMAGE):$(DOCKER_TAG)

## migrate: 执行数据库迁移 (可选 DB=user|business|log|monitor)
migrate:
	@echo "Running database migrations..."
	bash scripts/migrate.sh $(DB)

## generate: 生成代码（mock、swagger 等）
generate:
	@echo "Generating mocks..."
	go generate ./...
	@echo "Generating Swagger swagger..."
	swag init -g $(MAIN_FILE) -o ./swagger

## k6-test: 运行 k6 压力测试（全部 4 个场景，任一失败则整体失败）因包含 idle_settle 结算洪峰，自动先执行 mysql-stress-tune 抬高 MySQL 服务端上限。
k6-test: mysql-stress-tune
	@set -e; \
	echo "========================================"; \
	echo "=== k6 压力测试：场景 1 — 注册/登录 ==="; \
	echo "========================================"; \
	k6 run scripts/k6/auth.js; \
	echo ""; \
	echo "========================================"; \
	echo "=== k6 压力测试：场景 2 — 挂机心跳 ==="; \
	echo "========================================"; \
	k6 run scripts/k6/idle.js; \
	echo ""; \
	echo "========================================"; \
	echo "=== k6 压力测试：场景 3 — 挂机结算洪峰 ==="; \
	echo "========================================"; \
	k6 run scripts/k6/idle_settle.js; \
	echo ""; \
	echo "========================================"; \
	echo "=== k6 压力测试：场景 4 — 兑换抢购 ==="; \
	echo "========================================"; \
	k6 run scripts/k6/shop.js; \
	echo ""; \
	echo "========================================"; \
	echo "=== k6 压力测试：场景 5 — 混合负载 ==="; \
	echo "========================================"; \
	k6 run scripts/k6/mixed.js; \
	echo ""; \
	echo "========================================"; \
	echo "=== k6 压力测试：场景 6 — WebSocket 长连接 ==="; \
	echo "========================================"; \
	k6 run scripts/k6/ws.js; \
	echo ""; \
	echo "✅ All k6 stress tests passed!"

## k6-test-auth: 仅运行认证压测
k6-test-auth:
	k6 run scripts/k6/auth.js

## k6-test-idle: 运行挂机压测（心跳 + 结算洪峰）同 k6-test-idle-settle，本目标已自动依赖 mysql-stress-tune 以适配结算洪峰。
k6-test-idle: mysql-stress-tune
	@k6 run scripts/k6/idle.js

## k6-test-idle-settle: 仅运行挂机结算洪峰压测（idle.settled）并在本机 MySQL 服务端容量不足时运行 mysql-stress-tune（max_connections=500）。
k6-test-idle-settle: mysql-stress-tune
	@echo "Resetting stress item 1 stock=500 in MySQL(hc_business)..."
	@docker exec -i mysql mysql -uroot -p123456 hc_business -e "UPDATE shop_items SET stock=500, price_points=0, version=0, is_active=1 WHERE id=1; $(RESET_SHOP_BUCKETS_SQL)" 2>/dev/null || echo "[warn] failed to reset stress item 1 stock/buckets via docker mysql"
	@docker exec -i mysql mysql -uroot -p123456 -e "SHOW VARIABLES LIKE 'max_connections';" 2>/dev/null | grep -q "max_connections[[:space:]]*800" || echo "[warn] MySQL max_connections 未检测到 800；如服务端仍报 Too many connections，请重新执行 make mysql-stress-tune。"
	@sleep 2
	k6 run scripts/k6/idle_settle.js

## k6-test-shop: 仅运行商城压测 开跑前重置压测商品（默认 id=1）库存，确保每轮都能验证“零超卖”（库存 500 被并发抢光后成功数不超过 500）。
## 业务库默认是 Docker 中的 MySQL （hc_business），如切换为 SQLite/Postgres 请相应调整重置命令。
k6-test-shop:
	@echo "Resetting stress item 1 stock=500 in MySQL(hc_business)..."
	@docker exec -i mysql mysql -udemo -p123456 hc_business -e "UPDATE shop_items SET stock=500, price_points=0, version=0, is_active=1 WHERE id=1;" 2>/dev/null || echo "[warn] failed to reset stock via docker mysql; ensure item 1 exists and is active, or set ITEM_ID to a prepared item"
	@docker exec -i mysql mysql -udemo -p123456 hc_business -e "$(RESET_SHOP_BUCKETS_SQL)" 2>/dev/null || echo "[warn] failed to reset stock buckets for item 1; ensure shop_item_stock_buckets table exists (P2-5)"
	@if [ -n "$(ADMIN_TOKEN)" ]; then \
	  curl -s -o /dev/null -w "[ok] reconciled Redis stock for item 1 (HTTP %{http_code})\n" -X POST "http://localhost:8080/api/v1/admin/shop/items/1/reconcile" -H "X-Admin-Token: $(ADMIN_TOKEN)" 2>/dev/null || echo "[warn] reconcile item 1 failed"; \
	else \
	  echo "[warn] ADMIN_TOKEN 未设置，跳过 Redis 库存对账（L2 启用时 item1 可能假售罄；建议 export ADMIN_TOKEN 后重跑）"; \
	fi
	k6 run scripts/k6/shop.js

## k6-test-shop-flash: 仅运行定时抢购压测（场景 6）
## 验证「零超卖 + 削峰缓存拦截」。setup 自动创建活动（需 ADMIN_TOKEN），或复用 FLASH_ACTIVITY_ID。
## 开跑前重置关联商品（默认 id=1）库存，确保每轮都能验证零超卖（限量被并发抢光后成功数不超过限量）。
## 业务库默认是 Docker 中的 MySQL （hc_business），如切换为 SQLite/Postgres 请相应调整重置命令。
k6-test-shop-flash:
	@echo "Resetting stress item 1 stock=1000 in MySQL(hc_business)..."
	@docker exec -i mysql mysql -udemo -p123456 hc_business -e "UPDATE shop_items SET stock=1000, price_points=0, version=0, is_active=1 WHERE id=1;" 2>/dev/null || echo "[warn] failed to reset stock via docker mysql; ensure item 1 exists and is active, or set FLASH_ITEM_ID to a prepared item"
	@echo "⚠️  需提供 ADMIN_TOKEN（创建活动）或 FLASH_ACTIVITY_ID（复用活动）环境变量"
	./scripts/k6/shop_flash.sh

## k6-test-mixed: 仅运行混合负载压测
## 普通兑换走分桶路径（与 shop 一致）；前序 shop 已把分桶扣光、shop-flash 不重建桶，
## 故需在开跑前把 item1 列+桶重置回 500，否则混合负载的“写/兑换”部分会因库存为 0 全失败。
k6-test-mixed:
	@echo "Resetting stress item 1 stock=500 in MySQL(hc_business)..."
	@docker exec -i mysql mysql -udemo -p123456 hc_business -e "UPDATE shop_items SET stock=500, price_points=0, version=0, is_active=1 WHERE id=1;" 2>/dev/null || echo "[warn] failed to reset stock via docker mysql; ensure item 1 exists and is active"
	@docker exec -i mysql mysql -udemo -p123456 hc_business -e "$(RESET_SHOP_BUCKETS_SQL)" 2>/dev/null || echo "[warn] failed to reset stock buckets for item 1; ensure shop_item_stock_buckets table exists (P2-5)"
	@if [ -n "$(ADMIN_TOKEN)" ]; then \
	  curl -s -o /dev/null -w "[ok] reconciled Redis stock for item 1 (HTTP %{http_code})\n" -X POST "http://localhost:8080/api/v1/admin/shop/items/1/reconcile" -H "X-Admin-Token: $(ADMIN_TOKEN)" 2>/dev/null || echo "[warn] reconcile item 1 failed"; \
	else \
	  echo "[warn] ADMIN_TOKEN 未设置，跳过 Redis 库存对账（L2 启用时 item1 可能假售罄；建议 export ADMIN_TOKEN 后重跑）"; \
	fi
	k6 run scripts/k6/mixed.js

## k6-test-ws: 仅运行 WebSocket 长连接压测
k6-test-ws:
	@sleep 1
	k6 run scripts/k6/ws.js

## mysql-stress-tune: 运行时抬高 MySQL 服务端上限（idle_settle 500 VU 必备；详见 deployments/mysql-stress.cnf）不重启、即时生效；未起 docker 'mysql' 容器时打印提示，请按 cnf 手动 SET GLOBAL 或挂载后重启。
mysql-stress-tune:
	@echo "Tuning MySQL for stress: max_connections=800, innodb_flush_log_at_trx_commit=2, sync_binlog=0, innodb_buffer_pool_size=1G ..."
	@docker exec -i mysql mysql -uroot -p123456 -e " \
		SET GLOBAL max_connections=800; \
		SET GLOBAL innodb_flush_log_at_trx_commit=2; \
		SET GLOBAL sync_binlog=0; \
		SET GLOBAL innodb_buffer_pool_size=1073741824;" 2>/dev/null \
		|| echo "[warn] docker 'mysql' 容器不可达；请按 deployments/mysql-stress.cnf 手动调优（SET GLOBAL ... 或挂载 cnf 后重启容器）。"

## db-reset-test: 压测前清空挂机积分相关表，消除历史行累积导致的日初全表 SUM 风暴。清空后 idle_daily_points 由启动/日初回填任务从 idle_records 重建，结算仍可正常累加。
db-reset-test:
	@echo "Resetting idle test tables (idle_records, idle_daily_points)..."
	@docker exec -i mysql mysql -uroot -p123456 hc_business -e "\
		TRUNCATE TABLE idle_daily_points; \
		TRUNCATE TABLE idle_records;" 2>/dev/null \
		|| echo "[warn] docker 'mysql' 容器不可达；请手动清空 hc_business.idle_records / idle_daily_points（TRUNCATE 或 DELETE）。"

## stress-idle-settle: 一键压测 idle_settle 结算洪峰（自动先抬高 MySQL 上限）
stress-idle-settle: mysql-stress-tune
	@echo "========================================"
	@echo "=== k6 压力测试：场景 3 — 挂机结算洪峰（500 VU）==="
	@echo "========================================"
	k6 run scripts/k6/idle_settle.js

## help: 显示帮助
help:
	@echo "Usage: make [target]"
	@echo ""
	@echo "Targets:"
	@sed -n 's/^##//p' $(MAKEFILE_LIST) | column -t -s ':' | sed -e 's/^/ /'

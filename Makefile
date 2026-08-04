# ============================================================
# HC Framework (go-zero 版，合并单进程部署)
# 架构: hc-server 单进程承载 Gateway(REST) + 同进程后端 logic。
#       指标统一经 :8080/metrics 暴露，由 Prometheus 抓取。
# 配置: config/server.yaml（含 Gateway + Rpc 两段）
# 监控: deployments/docker-compose.monitoring.yml
# ============================================================
SHELL := /bin/bash

BIN := bin

# ---- 所有 target 声明 ----
.PHONY: all help gen tidy \
        run build restart \
        test lint kill clean \
		migrate migrate-up migrate-down migrate-status \
        mon-up mon-down mon-restart mon-reload mon-status mon-logs mon-clean mon-health \
        k6-all k6-auth k6-idle k6-idle-settle k6-shop k6-shop-flash k6-mixed

# ============================================================
# 帮助
# ============================================================
help:
	@echo "HC Framework (go-zero 合并部署) 命令清单"
	@echo ""
	@echo "  开发流程:"
	@echo "    make all               一键: 编译 + 测试 (不含启动/监控)"
	@echo "    make tidy              go mod tidy 收敛依赖"
	@echo "    make run        前台启动 hc-server (REST :8080, 指标 :8080/metrics)"
	@echo "    make build      编译到 $(BIN)/hc-server"
	@echo "    make restart    停止 → 编译 → 前台启动 (开发热重启)"
	@echo "    make kill              释放 :8080 端口"
	@echo "    make clean             清理构建产物 + sqlite 数据"
	@echo ""
	@echo "  数据库迁移:"
	@echo "    make migrate           执行所有数据库 up 迁移"
	@echo "    make migrate-up        同 migrate"
	@echo "    make migrate-down      回滚所有迁移 (谨慎!)"
	@echo "    make migrate-status    查看迁移状态"
	@echo ""
	@echo "  测试与检查:"
	@echo "    make test              go test ./... (全部单测)"
	@echo "    make lint              go vet + gofmt"
	@echo ""
	@echo "  监控栈 (Prometheus + Alertmanager + Grafana):"
	@echo "    make mon-up            启动 (Prometheus :9090, Alertmanager :9093, Grafana :3000)"
	@echo "    make mon-down          停止 (保留时序数据卷)"
	@echo "    make mon-restart       重启 (down + up)"
	@echo "    make mon-reload        热加载告警规则 (仅 rules/*.yaml 变更)"
	@echo "    make mon-status        容器 + Targets + 数据源 一键诊断"
	@echo "    make mon-logs          查看 Prometheus 容器日志 (最近 50 行)"
	@echo "    make mon-clean         停止并清除 (含数据卷, 相当于完全重置)"
	@echo "    make mon-health        全链路诊断 (应用→Prometheus→Grafana→面板)"
	@echo ""
	@echo "  压力测试 (需安装 k6):"
	@echo "    make k6-auth           注册/登录"
	@echo "    make k6-idle           挂机心跳"
	@echo "    make k6-idle-settle    挂机结算洪峰"
	@echo "    make k6-shop           积分兑换抢购"
	@echo "    make k6-shop-flash     定时抢购 (需 ADMIN_TOKEN 环境变量)"
	@echo "    make k6-mixed          混合负载"
	@echo "    make k6-all            以上全部 (不含 shop-flash + ws)"
	@echo ""
	@echo "  (独立双进程模式已废弃, 默认使用合并单进程 hc-server)"

# ============================================================
# 代码生成
# ============================================================
gen:
	protoc --plugin=protoc-gen-go="$$(go env GOPATH)/bin/protoc-gen-go" \
	       --plugin=protoc-gen-go-grpc="$$(go env GOPATH)/bin/protoc-gen-go-grpc" \
	       --go_out=. --go_opt=paths=source_relative \
	       --go-grpc_out=. --go-grpc_opt=paths=source_relative \
	       app/rpc/hc.proto
	@echo "pb 代码生成完成。"

tidy:
	go mod tidy

# ============================================================
# 数据库迁移
# ============================================================
# 使用 scripts/migrate.sh，默认读取 config/server.yaml。
# 环境变量 CONFIG_FILE 可覆盖配置文件路径。
# 例: make migrate                     → 所有数据库 up
#     make migrate-down DB=user        → 仅回滚 user 库
#     make migrate-status DB=user      → 查看 user 库迁移版本
MIGRATE := bash scripts/migrate.sh

migrate migrate-up:
	@$(MIGRATE) up all

migrate-down:
	@$(MIGRATE) down all

migrate-status:
	@$(MIGRATE) up all 2>&1 | grep -E "版本|version|迁移" || true
	@echo "提示: 完整状态查看请执行 scripts/migrate.sh up all"

# ============================================================
# 运行 / 构建 / 重启（合并单进程 hc-server）
# ============================================================
run:
	@echo "启动 hc-server (REST :8080, 指标 :8080/metrics)..."
	@go run ./app/server/cmd -f config/server.yaml

build:
	mkdir -p $(BIN)
	go build -o $(BIN)/hc-server ./app/server/cmd
	@echo "构建完成: $(BIN)/hc-server"

restart: kill build
	@echo "重启 hc-server ..."
	@go run ./app/server/cmd -f config/server.yaml

# ============================================================
# 测试与检查
# ============================================================
test:
	go test ./...

lint:
	go vet ./...
	gofmt -l .

# ============================================================
# 停止 / 清理
# ============================================================
kill:
	@echo "释放端口..."
	@-fuser -k 8080/tcp 2>/dev/null && echo "  已释放 :8080" || echo "  :8080 空闲"

clean:
	rm -rf $(BIN)
	rm -f data/hc.db
	@echo "已清理 $(BIN)/ 和 sqlite 数据"

# ============================================================
# 监控栈 (Prometheus + Alertmanager + Grafana)
# ============================================================
# 前置: docker + docker-compose 独立二进制。
# 配置文件: deployments/prometheus/{prometheus.yaml,alertmanager.yaml,rules/}。
# Dashboard: deployments/grafana/{provisioning/,hc-framework-dashboard.json} (自动导入)。
# 重要: compose 文件内挂载路径为相对于 deployments/ 目录，故所有 mon-* 均先 cd deployments。
#
# 网页入口:
#   Prometheus   http://localhost:9090  (Status → Targets 确认 hc-server UP)
#   Alertmanager http://localhost:9093  (告警路由 / 静默)
#   Grafana      http://localhost:3000  (admin / admin, Dashboard 已自动导入)
MON_COMPOSE := docker-compose -f docker-compose.monitoring.yml

mon-up:
	@echo "启动监控栈..."
	@cd deployments && $(MON_COMPOSE) up -d
	@sleep 3
	@docker ps --filter "name=hc-prometheus" --filter "name=hc-alertmanager" --filter "name=hc-grafana" \
		--format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
	@echo ""
	@echo "Prometheus  : http://localhost:9090"
	@echo "Alertmanager: http://localhost:9093"
	@echo "Grafana     : http://localhost:3000 (admin/admin)"

mon-down:
	@echo "停止监控栈 (保留数据卷)..."
	@cd deployments && $(MON_COMPOSE) down
	@echo "完成。时序数据已保留，下次 mon-up 恢复。"

mon-restart:
	@echo "重启监控栈..."
	@cd deployments && $(MON_COMPOSE) down && $(MON_COMPOSE) up -d
	@sleep 3
	@docker ps --filter "name=hc-prometheus" --filter "name=hc-alertmanager" --filter "name=hc-grafana" \
		--format "table {{.Names}}\t{{.Status}}"

mon-reload:
	@echo "热加载 Prometheus 规则 (仅 rules/*.yaml 变更)..."
	@curl -s -XPOST http://localhost:9090/-/reload -w "\nHTTP %{http_code}\n" \
		|| (echo "失败: Prometheus 未运行或未启用 --web.enable-lifecycle"; exit 1)
	@echo "已重载。可执行 make mon-status 验证。"

mon-status:
	@echo "=== 监控容器 ==="
	@docker ps --filter "name=hc-prometheus" --filter "name=hc-alertmanager" --filter "name=hc-grafana" \
		--format "table {{.Names}}\t{{.Status}}\t{{.Ports}}" 2>/dev/null || echo "  (无监控容器在运行)"
	@echo ""
	@echo "=== Prometheus Targets ==="
	@curl -s http://localhost:9090/api/v1/targets 2>/dev/null | \
		python3 -c "import sys,json; d=json.load(sys.stdin); [print(' ',t['scrapePool'],t['health'],t['labels'].get('instance','')) for t in d['data']['activeTargets']]" 2>/dev/null || \
		echo "  (Prometheus 不可达)"
	@echo ""
	@echo "=== Grafana 数据源 ==="
	@curl -s http://localhost:3000/api/datasources -u admin:admin 2>/dev/null | \
		python3 -c "import sys,json; d=json.load(sys.stdin); print(' ', [x['name'] for x in d]) if isinstance(d,list) else print('  (Grafana 不可达)')" 2>/dev/null || \
		echo "  (Grafana 不可达)"

mon-logs:
	@docker logs hc-prometheus --tail 50 2>&1

mon-clean:
	@echo "清除监控栈 (含数据卷)..."
	@cd deployments && $(MON_COMPOSE) down -v
	@echo "已清除: 容器 + 网络 + 所有时序数据。"

# 一键全链路诊断：应用 → Prometheus → Grafana 端到端验证。
mon-health:
	@echo "========== HC Framework 全链路监控诊断 =========="
	@echo ""
	@echo "[1/4] 应用 /metrics 端点..."
	@code=$$(curl -s -o /dev/null -w "%{http_code}" http://localhost:8080/health 2>/dev/null); \
	if [ "$$code" = "200" ]; then \
		echo "  ✓ hc-server /health 200"; \
	else \
		echo "  ✗ hc-server 不可达 (HTTP $$code), 请先 make run"; exit 1; \
	fi
	@http_count=$$(curl -s http://localhost:8080/metrics | grep "http_server_requests_code_total" | grep -v "^#" | wc -l); \
	if [ "$$http_count" -gt 0 ]; then \
		echo "  ✓ http_server_requests_code_total $$http_count 个样本"; \
	else \
		echo "  ✗ 无 HTTP 请求指标, 检查 main.go 是否调用了 prometheus.Enable()"; \
	fi
	@echo ""
	@echo "[2/4] Prometheus 抓取..."
	@prom_up=$$(curl -s http://localhost:9090/api/v1/query?query=up 2>/dev/null | python3 -c "import sys,json; r=json.load(sys.stdin)['data']['result']; print(len(r))" 2>/dev/null); \
	if [ "$$prom_up" -gt 0 ]; then \
		echo "  ✓ Prometheus 可达, up 指标 $$prom_up 个 target"; \
		target=$$(curl -s http://localhost:9090/api/v1/targets 2>/dev/null | python3 -c "import sys,json; [print('    ',t['scrapePool'],t['health'],t['lastError'] or '') for t in json.load(sys.stdin)['data']['activeTargets']]" 2>/dev/null); \
		echo "$$target"; \
	else \
		echo "  ✗ Prometheus 不可达 (localhost:9090), 请 make mon-up"; \
	fi
	@echo ""
	@echo "[3/4] Grafana 数据源..."
	@ds=$$(curl -s http://localhost:3000/api/datasources -u admin:admin 2>/dev/null | python3 -c "import sys,json; d=json.load(sys.stdin); print(len(d) if isinstance(d,list) else 0)" 2>/dev/null); \
	if [ "$$ds" -gt 0 ]; then \
		echo "  ✓ Grafana 数据源 $$ds 个"; \
	else \
		echo "  ✗ Grafana 无数据源 (admin/admin 不可达或 provisioning 未挂载)"; \
	fi
	@echo ""
	@echo "[4/4] Dashboard 面板 PromQL 验证..."
	@qps=$$(curl -s "http://localhost:3000/api/datasources/proxy/1/api/v1/query?query=sum(rate(http_server_requests_code_total%5B1m%5D))by(path)" -u admin:admin 2>/dev/null | python3 -c "import sys,json;r=json.load(sys.stdin)['data']['result'];print(len(r))" 2>/dev/null); \
	p99=$$(curl -s "http://localhost:3000/api/datasources/proxy/1/api/v1/query?query=histogram_quantile(0.99,sum(rate(http_server_requests_duration_ms_bucket%5B5m%5D))by(le))" -u admin:admin 2>/dev/null | python3 -c "import sys,json;r=json.load(sys.stdin)['data']['result'];print(r[0]['value'][1] if r else '无')" 2>/dev/null); \
	echo "  ✓ QPS paths: $$qps 条, P99 延迟: $$p99 ms"
	@echo ""
	@echo "========== 诊断完成 =========="
	@echo "面板: http://localhost:3000/dashboards → HC Framework (go-zero)"

# 一键从零到监控就绪：编译 → 测试 → 启动应用 → 启动监控 → 全链路验证。
all: build test
	@echo ""
	@echo "构建 + 测试完成。"
	@echo "启动应用:  make run"
	@echo "启动监控:  make mon-up"
	@echo "全链路诊断: make mon-health"

# ============================================================
# 压力测试 (k6)
# ============================================================
# 前置: 安装 k6 (https://k6.io/docs/get-started/installation/)
#       确保 hc-server 已启动 (make run)
# 环境变量: BASE_URL(默认 http://localhost:8080), ADMIN_TOKEN(shop-flash 必填)
K6_BASE := k6 run
K6_DIR  := scripts/k6

k6-auth:
	$(K6_BASE) $(K6_DIR)/auth.js

k6-idle:
	$(K6_BASE) $(K6_DIR)/idle.js

k6-idle-settle:
	$(K6_BASE) $(K6_DIR)/idle_settle.js

k6-shop:
	$(K6_BASE) $(K6_DIR)/shop.js

k6-shop-flash:
	@if [ -z "$$ADMIN_TOKEN" ]; then \
		echo "错误: 请设置 ADMIN_TOKEN 环境变量"; \
		echo "用法: ADMIN_TOKEN=xxx make k6-shop-flash"; \
		exit 1; \
	fi
	$(K6_BASE) $(K6_DIR)/shop_flash.js

k6-mixed:
	$(K6_BASE) $(K6_DIR)/mixed.js

# ws 场景在 go-zero 版暂不支持
k6-all: k6-auth k6-idle k6-idle-settle k6-shop k6-mixed
	@echo "全部 k6 场景完成 (shop-flash 需单独: ADMIN_TOKEN=xxx make k6-shop-flash)"
	@echo "ws 场景暂不支持 (go-zero 版未实现 WebSocket)"

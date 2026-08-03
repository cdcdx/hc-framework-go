# ============================================================
# HC Framework (go-zero 版,单 rpc 架构)
# 架构:rest 网关(gateway-api) + 单一领域 rpc(hc-rpc,含 user/idle/task/shop)
# ============================================================
SHELL := /bin/bash

BIN := bin

.PHONY: help gen tidy run build test lint kill

help:
	@echo "HC Framework (go-zero) 常用命令:"
	@echo "  make gen        protoc 生成 rpc pb 代码(需要 protoc+protoc-gen-go+protoc-gen-go-grpc)"
	@echo "  make tidy       go mod tidy 收敛依赖(首次克隆后执行)"
	@echo "  make run        启动 hc-rpc + 网关(前台)"
	@echo "  make build      编译 2 个服务到 $(BIN)/"
	@echo "  make test       运行全部单元测试"
	@echo "  make lint       go vet + gofmt 检查"

# ---------- 代码生成 ----------
# rpc pb 代码(手写的 server/logic/svc/client 依赖这些生成文件)
# 通过 go env GOPATH 动态定位 protoc 插件，确保跨环境可用
gen:
	protoc --plugin=protoc-gen-go="$$(go env GOPATH)/bin/protoc-gen-go" \
	       --plugin=protoc-gen-go-grpc="$$(go env GOPATH)/bin/protoc-gen-go-grpc" \
	       --go_out=. --go_opt=paths=source_relative \
	       --go-grpc_out=. --go-grpc_opt=paths=source_relative \
	       app/rpc/hc.proto
	@echo "pb 代码生成完成。网关 routes/handler/types 为手写,无需 goctl 重建;"
	@echo "若想用 goctl 重建网关,可执行: cd app/gateway/api && goctl api go -api gateway.api -dir ."

tidy:
	go mod tidy

# ---------- 运行 ----------
run: run-hc run-gateway

run-hc:
	cd app/rpc && go run ./cmd -f etc/hc.yaml

run-gateway:
	cd app/gateway/api && go run gateway.go -f etc/gateway.yaml

# ---------- 构建 ----------
build:
	mkdir -p $(BIN)
	cd app/rpc && go build -o ../../$(BIN)/hc-rpc ./cmd
	cd app/gateway/api && go build -o ../../$(BIN)/gateway-api gateway.go
	@echo "构建完成: $(BIN)/"

# ---------- 测试与检查 ----------
test:
	go test ./...

lint:
	go vet ./...
	gofmt -l .

# ---------- 停止服务 ----------
kill:
	@echo "停止 hc-rpc (8001) 和 gateway-api (8080)..."
	@-fuser -k 8001/tcp 2>/dev/null && echo "  已释放 8001 (hc-rpc)" || echo "  8001 空闲"
	@-fuser -k 8080/tcp 2>/dev/null && echo "  已释放 8080 (gateway-api)" || echo "  8080 空闲"
	@echo "完成"

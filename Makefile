# ============================================================
# HC Framework (go-zero 版,单 rpc 架构)
# 架构:rest 网关(gateway-api) + 单一领域 rpc(hc-rpc,含 user/idle/task/shop)
# ============================================================
SHELL := /bin/bash

BIN := bin

.PHONY: help gen tidy run build test lint

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
gen:
	protoc --go_out=. --go_opt=paths=source_relative \
	       --go-grpc_out=. --go-grpc_opt=paths=source_relative \
	       app/rpc/hc.proto
	@echo "pb 代码生成完成。网关 routes/handler/types 为手写,无需 goctl 重建;"
	@echo "若想用 goctl 重建网关,可执行: cd app/gateway/api && goctl api go -api gateway.api -dir ."

tidy:
	go mod tidy

# ---------- 运行 ----------
run: run-hc run-gateway

run-hc:
	cd app/rpc && go run hc.go -f etc/hc.yaml

run-gateway:
	cd app/gateway/api && go run gateway.go -f etc/gateway.yaml

# ---------- 构建 ----------
build:
	mkdir -p $(BIN)
	cd app/rpc && go build -o ../../$(BIN)/hc-rpc hc.go
	cd app/gateway/api && go build -o ../../$(BIN)/gateway-api gateway.go
	@echo "构建完成: $(BIN)/"

# ---------- 测试与检查 ----------
test:
	go test ./...

lint:
	go vet ./...
	gofmt -l .

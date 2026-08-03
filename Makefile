# ============================================================
# HC Framework (go-zero 微服务版)
# 架构:rest 网关(gateway-api) + 4 个领域 rpc(user/idle/task/shop)
# ============================================================
SHELL := /bin/bash

BIN := bin

.PHONY: help gen tidy run build test lint

help:
	@echo "HC Framework (go-zero) 常用命令:"
	@echo "  make gen        用 protoc 生成 4 个 rpc 的 pb 代码(需要 protoc+protoc-gen-go+protoc-gen-go-grpc)"
	@echo "  make tidy       go mod tidy 收敛依赖(首次克隆后执行)"
	@echo "  make run        Etcd 存在时一键启动 4 rpc + 网关(前台)"
	@echo "  make build      编译全部服务到 $(BIN)/"
	@echo "  make test       运行全部单元测试"
	@echo "  make lint       go vet + gofmt 检查"

# ---------- 代码生成 ----------
# rpc pb 代码(手写的 server/logic/svc 依赖这些生成文件)
gen:
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
	@echo "pb 代码生成完成。若想用 goctl 重建网关(routes/handler/types),可执行:"
	@echo "  cd app/gateway/api && goctl api go -api gateway.api -dir ."

tidy:
	go mod tidy

# ---------- 运行 ----------
run: run-user run-idle run-task run-shop run-gateway

run-gateway:
	cd app/gateway/api && go run gateway.go -f etc/gateway.yaml

run-user:
	cd app/user/rpc && go run user.go -f etc/user.yaml

run-idle:
	cd app/idle/rpc && go run idle.go -f etc/idle.yaml

run-task:
	cd app/task/rpc && go run task.go -f etc/task.yaml

run-shop:
	cd app/shop/rpc && go run shop.go -f etc/shop.yaml

# ---------- 构建 ----------
build:
	mkdir -p $(BIN)
	cd app/gateway/api && go build -o ../../$(BIN)/gateway-api gateway.go
	cd app/user/rpc && go build -o ../../$(BIN)/user-rpc user.go
	cd app/idle/rpc && go build -o ../../$(BIN)/idle-rpc idle.go
	cd app/task/rpc && go build -o ../../$(BIN)/task-rpc task.go
	cd app/shop/rpc && go build -o ../../$(BIN)/shop-rpc shop.go
	@echo "构建完成: $(BIN)/"

# ---------- 测试与检查 ----------
test:
	go test ./...

lint:
	go vet ./...
	gofmt -l .

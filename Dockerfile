# ============================================================
# HC Framework (go-zero 版) — 合并单进程 hc-server
# 单进程承载 Gateway(REST :8080) + 同进程后端 logic，
# Prometheus 指标统一经 :8080/metrics 暴露。
# ============================================================
FROM golang:1.25-alpine AS builder

# 构建依赖：CGO 启用（mattn/go-sqlite3），需 gcc + musl-dev。
RUN apk add --no-cache gcc musl-dev

WORKDIR /src

# 先拉取依赖，利用层缓存（go.sum/go.mod 变化时才重拉）。
COPY go.mod go.sum ./
RUN go mod download

# 复制源码并编译合并单进程二进制。
COPY . .
RUN CGO_ENABLED=1 GOOS=linux go build -o /out/hc-server ./app/server/cmd

# ============================================================
FROM alpine:3.20

RUN apk add --no-cache ca-certificates tzdata \
    && addgroup -S hc && adduser -S hc -G hc

WORKDIR /app

# 合并部署配置（Gateway + Rpc 两段）。
COPY config/ ./config/
# 启动脚本：加载 config/server.yaml，优雅转发信号。
COPY deployments/docker-entrypoint.sh ./docker-entrypoint.sh
RUN chmod +x ./docker-entrypoint.sh

COPY --from=builder /out/hc-server ./hc-server

# REST + /metrics 共用端口。
EXPOSE 8080

USER hc
ENTRYPOINT ["./docker-entrypoint.sh"]

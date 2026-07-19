# ============================================
# Stage 1: Build
# ============================================
FROM golang:1.25-alpine AS builder

# 安装构建依赖
RUN apk add --no-cache git ca-certificates tzdata

# 设置工作目录
WORKDIR /app

# 复制依赖文件，利用 Docker 层缓存
COPY go.mod go.sum ./
RUN go mod download

# 复制源码
COPY . .

# 编译
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
    go build -trimpath -ldflags="-s -w" \
    -o /app/hcf-server \
    ./cmd/server/main.go

# ============================================
# Stage 2: Runtime
# ============================================
FROM alpine:3.20

# 安装运行依赖
RUN apk add --no-cache ca-certificates tzdata curl && \
    # 创建非 root 用户
    adduser -D -g '' appuser

# 复制编译产物
COPY --from=builder /app/hcf-server /app/hcf-server
COPY --from=builder /app/config /app/config
COPY --from=builder /app/migrations /app/migrations

# 使用非 root 用户
USER appuser

# 健康检查
HEALTHCHECK --interval=10s --timeout=3s --retries=3 \
    CMD curl -f http://localhost:8080/health || exit 1

# 暴露端口
EXPOSE 8080

# 启动
ENTRYPOINT ["/app/hcf-server"]

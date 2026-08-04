# 构建阶段：以 gateway 为例（各 rpc 同理，替换 -f 参数与入口文件即可）
FROM golang:1.25-alpine AS builder
WORKDIR /build
COPY go.mod go.sum* ./
RUN go mod download
COPY . .
# 网关
RUN CGO_ENABLED=0 GOOS=linux go build -o /bin/hc-gateway ./app/gateway/api/gateway.go

# 运行阶段
FROM alpine:3.20
RUN apk add --no-cache ca-certificates tzdata
WORKDIR /app
COPY --from=builder /bin/hc-gateway /app/hc-gateway
COPY app/gateway/api/etc /app/etc
EXPOSE 8080
ENTRYPOINT ["/app/hc-gateway", "-f", "/app/etc/gateway.yaml"]

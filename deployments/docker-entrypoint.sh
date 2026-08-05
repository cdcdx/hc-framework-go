#!/bin/sh
# HC Framework 容器启动入口
# 合并单进程 hc-server：REST (:8080) + 指标 (:8080/metrics) 共用端口。
set -e

CONFIG_FILE="${CONFIG_FILE:-/app/config/server.yaml}"

if [ ! -f "$CONFIG_FILE" ]; then
  echo "[ERROR] 配置文件不存在: $CONFIG_FILE" >&2
  exit 1
fi

echo "[INFO] 启动 hc-server, 配置文件: $CONFIG_FILE"
echo "[INFO] REST  : http://0.0.0.0:8080"
echo "[INFO] 指标  : http://0.0.0.0:8080/metrics"

# 用 exec 替换进程，确保 SIGTERM/SIGINT 直接送达 hc-server（go-zero 优雅关闭）。
exec ./hc-server -f "$CONFIG_FILE"

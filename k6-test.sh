#!/bin/bash
# k6 全场景压测一键运行脚本
# 前置: 确保服务已启动 (make run)，k6 已安装 (https://k6.io/docs/get-started/installation/)
# 用法: ADMIN_TOKEN=xxx bash k6-test.sh
set -euo pipefail

ADMIN_TOKEN="${ADMIN_TOKEN:-}"
BASE_URL="${BASE_URL:-http://localhost:8080}"

echo "======== k6 全场景压测 ========"
echo "BASE_URL: $BASE_URL"
echo "ADMIN_TOKEN: ${ADMIN_TOKEN:-(未设置，shop_flash 将跳过)}"
echo ""

# 1. 认证压测
echo "===== [1/7] auth: 注册/登录 ====="
AUTH_VUS=150 k6 run scripts/k6/auth.js

# 2. 挂机心跳 (等待 5 分钟让心跳会话稳定)
echo "===== [2/7] idle: 挂机心跳 + DB 读 ====="
IDLE_VUS=10000 DBSTRESS_VUS=800 k6 run scripts/k6/idle.js

# 3. 挂机结算 (依赖心跳会话存在)
echo "===== [3/7] idle-settle: 挂机结算洪峰 ====="
MAX_VUS=500 k6 run scripts/k6/idle_settle.js

# 4. 商城兑换
echo "===== [4/7] shop: 积分兑换抢购 ====="
SHOP_VUS=1000 k6 run scripts/k6/shop.js

# 5. 定时抢购 (需要 ADMIN_TOKEN)
if [[ -n "$ADMIN_TOKEN" ]]; then
    echo "===== [5/7] shop-flash: 定时抢购 ====="
    FLASH_VUS=1000 ADMIN_TOKEN="$ADMIN_TOKEN" k6 run scripts/k6/shop_flash.js
else
    echo "===== [5/7] shop-flash: 跳过 (未设置 ADMIN_TOKEN) ====="
fi

# 6. 混合负载
echo "===== [6/7] mixed: 混合负载 ====="
MIXED_VUS=2000 k6 run scripts/k6/mixed.js

# 7. WebSocket (可选，需要 go-zero 版支持 ws)
echo "===== [7/7] ws: WebSocket 压测 (go-zero 版暂不支持，跳过) ====="
# WS_HEARTBEAT_VUS=10000 WS_RECONNECT_VUS=200 k6 run scripts/k6/ws.js

echo ""
echo "======== 压测完成 ========"

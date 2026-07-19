#!/usr/bin/env bash
# =============================================================================
# shop_flash.sh — 定时抢购高并发压测一键运行
# -----------------------------------------------------------------------------
# 对应 k6 脚本 scripts/k6/shop_flash.js，验证「零超卖」+「削峰缓存拦截」。
# setup 阶段会经运营接口自动创建抢购活动（需 ADMIN_TOKEN），或复用已有活动
# （FLASH_ACTIVITY_ID）。每个 VU 独占一个自动注册的测试账号，各抢 1 次。
#
# 前置条件：
#   - 服务已启动且可访问（默认 http://localhost:8080）。
#   - 商品 FLASH_ITEM_ID（默认复用 ITEM_ID=1）需 is_active=1、stock>=LIMIT_QTY。
#   - 二选一：
#       a) 配置 admin.token（config.admin.token），并通过 ADMIN_TOKEN 传入
#          → setup 自动建活动，teardown 可做服务端 sold_qty 对账；
#       b) 预先手动创建活动，通过 FLASH_ACTIVITY_ID 指定 → 复用
#          （teardown 服务端对账会跳过，因缺 ADMIN_TOKEN）。
#
# 用法：
#   ./shop_flash.sh
#   ADMIN_TOKEN=xxxx ./shop_flash.sh
#   FLASH_ACTIVITY_ID=7 LIMIT_QTY=200 ./shop_flash.sh
#   K6_ARGS="--vus 2000 --duration 2m" ./shop_flash.sh
#
# 环境变量：
#   BASE_URL          目标地址（默认 http://localhost:8080）
#   ADMIN_TOKEN       运营令牌（创建活动用）
#   FLASH_ACTIVITY_ID 复用已有活动 ID（优先级高于 ADMIN_TOKEN 创建）
#   ITEM_ID           压测商品 id（默认 1）
#   FLASH_ITEM_ID     活动关联商品 id（默认同 ITEM_ID）
#   LIMIT_QTY         活动限量（默认 500，零超卖门禁）
#   PRICE_POINTS      抢购价（默认 0，不扣积分）
#   PER_USER_LIMIT    每人限购（默认 1）
#   END_TIME          活动结束时间 RFC3339（默认空=不限）
#   K6_ARGS           k6 额外参数，如 --vus 2000 --duration 2m --out json=report.json
# =============================================================================
set -uo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCRIPT="$HERE/shop_flash.js"

BASE_URL="${BASE_URL:-http://localhost:8080}"
ADMIN_TOKEN="${ADMIN_TOKEN:-}"
FLASH_ACTIVITY_ID="${FLASH_ACTIVITY_ID:-}"
ITEM_ID="${ITEM_ID:-1}"
FLASH_ITEM_ID="${FLASH_ITEM_ID:-$ITEM_ID}"
LIMIT_QTY="${LIMIT_QTY:-500}"
PRICE_POINTS="${PRICE_POINTS:-0}"
PER_USER_LIMIT="${PER_USER_LIMIT:-1}"
END_TIME="${END_TIME:-}"
FLASH_START_OFFSET="${FLASH_START_OFFSET:--1}"
K6_ARGS="${K6_ARGS:-}"

echo "==> 定时抢购压测配置"
echo "    BASE_URL          = $BASE_URL"
echo "    ITEM_ID           = $ITEM_ID"
echo "    FLASH_ITEM_ID     = $FLASH_ITEM_ID"
echo "    LIMIT_QTY         = $LIMIT_QTY"
echo "    PRICE_POINTS      = $PRICE_POINTS"
echo "    PER_USER_LIMIT    = $PER_USER_LIMIT"
echo "    END_TIME          = ${END_TIME:-<不限>}"
if [ -n "$FLASH_ACTIVITY_ID" ]; then
  echo "    活动来源          = 复用 FLASH_ACTIVITY_ID=$FLASH_ACTIVITY_ID"
elif [ -n "$ADMIN_TOKEN" ]; then
  echo "    活动来源          = setup 自动创建（ADMIN_TOKEN 已设置）"
else
  echo "    [ERROR] 必须设置 ADMIN_TOKEN（自动创建活动）或 FLASH_ACTIVITY_ID（复用活动）" >&2
  exit 1
fi

# 校验 k6 是否可用
if ! command -v k6 >/dev/null 2>&1; then
  echo "[ERROR] 未找到 k6，请先安装：https://k6.io/docs/get-started/installation/" >&2
  exit 1
fi

# 构造 k6 --env 参数（shell 变量空值也传递，脚本侧以 __ENV.X || '' 兜底）
ENV_ARGS=()
ENV_ARGS+=(--env "BASE_URL=$BASE_URL")
ENV_ARGS+=(--env "ITEM_ID=$ITEM_ID")
ENV_ARGS+=(--env "FLASH_ITEM_ID=$FLASH_ITEM_ID")
ENV_ARGS+=(--env "LIMIT_QTY=$LIMIT_QTY")
ENV_ARGS+=(--env "PRICE_POINTS=$PRICE_POINTS")
ENV_ARGS+=(--env "PER_USER_LIMIT=$PER_USER_LIMIT")
ENV_ARGS+=(--env "FLASH_START_OFFSET=$FLASH_START_OFFSET")
ENV_ARGS+=(--env "ADMIN_TOKEN=$ADMIN_TOKEN")
ENV_ARGS+=(--env "FLASH_ACTIVITY_ID=$FLASH_ACTIVITY_ID")
[ -n "$END_TIME" ] && ENV_ARGS+=(--env "END_TIME=$END_TIME")

echo ""
echo "==> 启动 k6 压测（limit_qty=$LIMIT_QTY, 期望成功数 <= $LIMIT_QTY）"
echo ""
# exec 替换进程，退出码即 k6 退出码，便于 CI 门禁
exec k6 run "${ENV_ARGS[@]}" $K6_ARGS "$SCRIPT"

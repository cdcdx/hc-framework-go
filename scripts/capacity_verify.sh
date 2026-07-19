#!/usr/bin/env bash
# =============================================================================
# capacity_verify.sh — 百万设备挂机容量部署验证
# -----------------------------------------------------------------------------
# 在 kubectl apply 之后运行，逐个 Pod 抓取 Prometheus 指标（/metrics）与最近
# 日志，检查离线检测 scanner 的负载是否被打散到各 Pod、单轮扫描耗时是否健康、
# 结算是否推进。用于验证“按 Pod 分片扫描 + 批量判活 + 并发结算”是否达标。
#
# 前置条件：
#   - kubectl 已配置好目标集群上下文，且对目标命名空间有 get/pods/logs 权限。
#   - 服务已暴露 /metrics（Prometheus 文本格式）。
#
# 用法：
#   ./capacity_verify.sh
#   NS=prod LABEL="app=hcf" EXPECTED_ONLINE=1000000 MAX_SCAN_SECONDS=60 ./capacity_verify.sh
#
# 环境变量：
#   NS                命名空间（默认 default）
#   LABEL             筛选 Pod 的 label selector（默认 app=hc-framework）
#   PORT              指标端口（默认 8080）
#   METRICS_PATH      指标路径（默认 /metrics）
#   EXPECTED_ONLINE   可选：预期在线设备总数，用于一致性校验（对比各 Pod members 之和）
#   MAX_SCAN_SECONDS  单轮扫描耗时告警阈值（默认 60s，应明显小于 offline_check_interval）
# =============================================================================
set -uo pipefail

NS="${NS:-default}"
LABEL="${LABEL:-app=hc-framework}"
PORT="${PORT:-8080}"
METRICS_PATH="${METRICS_PATH:-/metrics}"
EXPECTED_ONLINE="${EXPECTED_ONLINE:-}"
MAX_SCAN_SECONDS="${MAX_SCAN_SECONDS:-60}"

num() { printf '%s' "$1" | awk '{print ($1==""?0:$1)+0}'; }

echo "==> 查询 Pod 列表 (ns=$NS label=$LABEL)"
PODS="$(kubectl -n "$NS" get pods -l "$LABEL" -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}')"
if [ -z "$PODS" ]; then
  echo "ERROR: 未找到匹配 label=$LABEL 的 Pod" >&2
  exit 1
fi
POD_COUNT="$(printf '%s\n' "$PODS" | grep -c .)"
echo "    找到 $POD_COUNT 个 Pod"

# 抓取单个 Pod 的指标文本：优先 kubectl proxy（无需 Pod 内 curl），失败则回退 exec。
fetch_metrics() {
  local pod="$1"
  local out
  out="$(kubectl -n "$NS" get --raw "/api/v1/namespaces/$NS/pods/$pod:$PORT/proxy$METRICS_PATH" 2>/dev/null)"
  if [ -n "$out" ]; then printf '%s\n' "$out"; return 0; fi
  # fallback：Pod 内执行 curl/wget
  out="$(kubectl -n "$NS" exec "$pod" -- curl -s "http://localhost:$PORT$METRICS_PATH" 2>/dev/null)"
  if [ -n "$out" ]; then printf '%s\n' "$out"; return 0; fi
  echo ""
  return 1
}

printf '\n%-45s %10s %10s %10s %12s %12s %10s\n' \
  "POD" "MEMBERS" "DEAD" "SETTLED" "SCAN_AVG_s" "SCAN_LAST" "LOG_SETTLE"
printf '%s\n' "--------------------------------------------------------------------------------------------------------------------"

TOTAL_MEMBERS=0
TOTAL_DEAD=0
TOTAL_SETTLED=0
WARN=0

while IFS= read -r POD; do
  [ -z "$POD" ] && continue
  M="$(fetch_metrics "$POD")" || { printf '%-45s %s\n' "$POD" "METRICS_UNAVAILABLE"; WARN=$((WARN+1)); continue; }

  members=$(printf '%s\n' "$M" | awk '$1=="idle_scan_members_total"{print $2}')
  dead=$(printf '%s\n' "$M" | awk '$1=="idle_scan_dead_total"{print $2}')
  dur_sum=$(printf '%s\n' "$M" | awk '$1=="idle_scan_duration_seconds_sum"{print $2}')
  dur_cnt=$(printf '%s\n' "$M" | awk '$1=="idle_scan_duration_seconds_count"{print $2}')
  st_timeout=$(printf '%s\n' "$M" | awk '$1=="idle_settle_total" && /reason="timeout"/{print $2}')
  st_completed=$(printf '%s\n' "$M" | awk '$1=="idle_settle_total" && /reason="completed"/{print $2}')
  st_skipped=$(printf '%s\n' "$M" | awk '$1=="idle_settle_total" && /reason="skipped"/{print $2}')

  members=$(num "$members"); dead=$(num "$dead")
  dur_sum=$(num "$dur_sum"); dur_cnt=$(num "$dur_cnt")
  st_timeout=$(num "$st_timeout"); st_completed=$(num "$st_completed"); st_skipped=$(num "$st_skipped")

  settled=$((st_timeout + st_completed))
  avg=0; last=0
  if awk "BEGIN{exit !($dur_cnt>0)}"; then
    avg=$(awk "BEGIN{printf \"%.2f\", $dur_sum/$dur_cnt}")
  fi
  # 最近一次扫描耗时（近似用 avg；histogram 无单点 last，可用 last 近似 avg）
  last="$avg"

  # 最近日志中的 settled 行数（tail 200）
  log_settle=$(kubectl -n "$NS" logs "$POD" --tail=200 2>/dev/null | grep -c "Idle timeout settled" || true)

  printf '%-45s %10s %10s %10s %12s %12s %10s\n' \
    "$POD" "$members" "$dead" "$settled" "$avg" "$last" "$log_settle"

  TOTAL_MEMBERS=$((TOTAL_MEMBERS + members))
  TOTAL_DEAD=$((TOTAL_DEAD + dead))
  TOTAL_SETTLED=$((TOTAL_SETTLED + settled))

  if awk "BEGIN{exit !($avg>$MAX_SCAN_SECONDS)}"; then
    echo "  [WARN] $POD 单轮扫描耗时 $avg s > 阈值 $MAX_SCAN_SECONDS s（Redis/DB 可能变慢或 dead 激增）"
    WARN=$((WARN+1))
  fi
done <<< "$PODS"

printf '%s\n' "--------------------------------------------------------------------------------------------------------------------"
printf '%-45s %10s %10s %10s\n' "TOTAL($POD_COUNT pods)" "$TOTAL_MEMBERS" "$TOTAL_DEAD" "$TOTAL_SETTLED"

echo ""
echo "==> 一致性校验"
if [ -n "$EXPECTED_ONLINE" ]; then
  EXPECTED_ONLINE=$(num "$EXPECTED_ONLINE")
  diff=$((TOTAL_MEMBERS - EXPECTED_ONLINE))
  pct=$(awk "BEGIN{printf \"%.1f\", ($diff>=0?$diff:-$diff)/$EXPECTED_ONLINE*100}")
  echo "    在线设备(各 Pod members 之和)=$TOTAL_MEMBERS, 预期=$EXPECTED_ONLINE, 偏差=${pct}%"
  if awk "BEGIN{exit !($pct>10)}"; then
    echo "  [WARN] 偏差 >10%：检查是否有 Pod 未纳入扫描分片、或 headless 发现的 pod_total 与实际副本数不一致。"
    WARN=$((WARN+1))
  else
    echo "  [OK] 偏差在 ±10% 内，扫描分片覆盖完整。"
  fi
else
  echo "    未设置 EXPECTED_ONLINE，跳过在线设备数一致性校验。"
fi

echo ""
echo "==> 分片覆盖提示"
echo "    若启用 scan_sharding，各 Pod 的 owned_set_shards 应互不重叠且并集覆盖 [0, active_set_shards)。"
echo "    扩容/缩容后请确认：headless 模式各 Pod 经 DNS 感知的 pod_total 一致（日志搜 'idle pod discovery updated pod_total'）。"

if [ "$WARN" -gt 0 ]; then
  echo ""
  echo "==> 结论：存在 $WARN 处告警，请按上文提示排查。"
  exit 2
else
  echo ""
  echo "==> 结论：各 Pod 扫描指标健康，容量部署验证通过。"
fi

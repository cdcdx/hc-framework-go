#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# ── 颜色 ──
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'

log_info()  { echo -e "${CYAN}[INFO]${NC}  $*"; }
log_ok()   { echo -e "${GREEN}[OK]${NC}    $*"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC}  $*"; }
log_err()  { echo -e "${RED}[ERROR]${NC} $*"; }

# ── 配置 ──
BINARY_NAME="hcf-server"
KUBECTL_VERSION="v1.34.0"
KIND_VERSION="v0.22.0"
CLUSTER_NAME="dev"
CONFIG_FILE="config/config.yaml"

# ── 帮助 ──
show_usage() {
    cat <<EOF
用法: ./build.sh <command> [options]

Commands:
  init        初始化开发环境 (安装 kubectl + kind, 创建本地 K8s 集群)
  build       编译并启动服务 (默认)
  docker      构建 Docker 镜像并部署到 K8s
  test        运行测试 (含 race detector)
  lint        代码检查 (vet + fmt)
  clean       清理构建产物
  deps        安装/更新 Go 依赖
  swagger     生成 Swagger 文档
  kill        关闭运行中的服务 (先检测端口占用, 占用则关闭对应 PID)
  k6          运行 k6 压力测试 (可选场景: auth idle idle-settle shop shop-flash mixed all)

示例:
  ./build.sh                # 编译并启动
  ./build.sh init           # 初始化环境
  ./build.sh docker         # Docker 构建+部署
  ./build.sh test           # 运行测试

EOF
}

# ── 工具检查 ──
require_cmd() {
    local cmd="$1"
    if ! command -v "$cmd" &>/dev/null; then
        log_err "缺少命令: $cmd, 请先安装"
        exit 1
    fi
}

# ── init: 初始化开发环境 ──
cmd_init() {
    log_info "初始化开发环境..."

    apt install -y apache2-utils

    # kubectl
    if command -v kubectl &>/dev/null; then
        log_ok "kubectl 已安装: $(kubectl version --client --short 2>/dev/null || kubectl version --client 2>/dev/null | head -1)"
    else
        log_info "安装 kubectl ${KUBECTL_VERSION}..."
        curl -fsSLO "https://dl.k8s.io/release/${KUBECTL_VERSION}/bin/linux/amd64/kubectl"
        chmod +x kubectl
        sudo mv kubectl /usr/local/bin/kubectl
        log_ok "kubectl 安装完成: $(kubectl version --client 2>/dev/null | head -1)"
    fi

    # kind
    if command -v kind &>/dev/null; then
        log_ok "kind 已安装: $(kind version 2>/dev/null | head -1)"
    else
        log_info "安装 kind ${KIND_VERSION}..."
        curl -fsSLo /usr/local/bin/kind "https://kind.sigs.k8s.io/dl/${KIND_VERSION}/kind-linux-amd64"
        chmod +x /usr/local/bin/kind
        log_ok "kind 安装完成: $(kind version 2>/dev/null | head -1)"
    fi

    # Docker 检查
    if ! docker info &>/dev/null; then
        log_err "Docker 未运行, 请先启动 Docker"
        exit 1
    fi

    # 创建 K8s 集群
    if kind get clusters 2>/dev/null | grep -qx "$CLUSTER_NAME"; then
        log_ok "K8s 集群 '${CLUSTER_NAME}' 已存在"
    else
        log_info "创建 K8s 集群 '${CLUSTER_NAME}'..."
        kind create cluster --name "$CLUSTER_NAME"
        log_ok "集群创建完成"
    fi

    log_info "初始化完成, 执行 make deploy 部署应用"
}

# ── docker: 构建镜像并部署 ──
cmd_docker() {
    require_cmd docker
    require_cmd kubectl

    log_info "构建 Docker 镜像..."
    make docker-build
    log_ok "镜像构建完成"

    log_info "部署到 Kubernetes..."
    # 将镜像加载到 kind 集群 (本地集群)
    if kind get clusters 2>/dev/null | grep -qx "$CLUSTER_NAME"; then
        kind load docker-image server:1.0.0 --name "$CLUSTER_NAME" 2>/dev/null || true
    fi
    make deploy
    log_ok "部署完成"

    # 等待就绪
    log_info "等待 Pod 就绪..."
    kubectl rollout status deployment/server --timeout=120s 2>/dev/null || log_warn "等待超时, 请手动检查"
    kubectl get pods -l app=server -o wide 2>/dev/null || true
}

# ── 端口解析: 环境变量 APP_SERVER_PORT 优先, 否则读取 config.yaml 的 server.port ──
resolve_port() {
    if [[ -n "${APP_SERVER_PORT:-}" ]]; then
        echo "$APP_SERVER_PORT"
        return
    fi
    if [[ -f "$CONFIG_FILE" ]]; then
        # 提取 server: 块下第一个 port: 值
        local port
        port="$(awk '
            /^server:/       { in_server=1; next }
            /^[^[:space:]]/  { in_server=0 }
            in_server && /^[[:space:]]+port:/ {
                gsub(/[^0-9]/, "", $2); print $2; exit
            }' "$CONFIG_FILE")"
        [[ -n "$port" ]] && { echo "$port"; return; }
    fi
    echo "8080"   # 兜底默认
}

# ── 端口检测: 找出占用目标端口 LISTEN 的进程 PID ──
find_port_pids() {
    local port="$1"
    if command -v lsof &>/dev/null; then
        lsof -tiTCP:"$port" -sTCP:LISTEN 2>/dev/null || true
    elif command -v ss &>/dev/null; then
        ss -lptnH "sport = :$port" 2>/dev/null | grep -oE 'pid=[0-9]+' | cut -d= -f2 | sort -u || true
    elif command -v fuser &>/dev/null; then
        fuser "$port"/tcp 2>/dev/null | tr -s ' ' '\n' | grep -E '^[0-9]+$' || true
    else
        echo ""
    fi
}

# ── 端口检测: 若目标端口被占用则报错并给出处置建议 ──
check_port() {
    local port="$1"
    local pids
    pids="$(find_port_pids "$port")"

    if [[ -z "$pids" ]]; then
        log_ok "端口 ${port} 空闲"
        return 0
    fi

    log_err "端口 ${port} 已被占用, 占用进程 PID: ${pids//$'\n'/, }"
    # 展示占用进程详情, 便于排查
    for pid in $pids; do
        local pcmd
        pcmd="$(ps -p "$pid" -o comm= 2>/dev/null || echo '?')"
        log_warn "  PID ${pid} -> ${pcmd}"
    done
    log_info "可执行 './build.sh kill' 关闭本服务, 或设置 APP_SERVER_PORT=<其他端口> 后重试"
    return 1
}

# ── 关闭占用指定端口的进程 (SIGTERM 优雅关闭, 超时后 SIGKILL) ──
kill_port_owner() {
    local port="$1"
    local pids
    pids="$(find_port_pids "$port")"

    if [[ -z "$pids" ]]; then
        log_ok "端口 ${port} 无占用, 无需关闭"
        return 0
    fi

    log_warn "端口 ${port} 被以下进程占用, 准备关闭:"
    for pid in $pids; do
        local pcmd
        pcmd="$(ps -p "$pid" -o comm=,args= 2>/dev/null || echo '?')"
        log_warn "  PID ${pid} -> ${pcmd}"
    done

    log_info "发送 SIGTERM (端口 ${port})..."
    # shellcheck disable=SC2086
    kill -TERM $pids 2>/dev/null || true

    local waited=0
    while [[ $waited -lt 10 ]] && [[ -n "$(find_port_pids "$port")" ]]; do
        sleep 1
        ((waited++))
    done

    if [[ -n "$(find_port_pids "$port")" ]]; then
        log_warn "优雅关闭超时 (>10s), 强制 SIGKILL (端口 ${port})..."
        # shellcheck disable=SC2086
        kill -9 $pids 2>/dev/null || true
        sleep 1
    fi

    if [[ -n "$(find_port_pids "$port")" ]]; then
        log_err "无法关闭端口 ${port} 的占用进程, 请手动检查"
        return 1
    fi
    log_ok "端口 ${port} 已释放"
    return 0
}

# ── build: 编译并启动 ──
cmd_build() {
    # Swagger 文档 (可选)
    if command -v swag &>/dev/null; then
        log_info "生成 Swagger 文档..."
        swag init -g cmd/server/main.go -o ./swagger --parseDependency --parseInternal 2>&1 | grep -vE "warning: (failed to get package name|failed to evaluate const)" || true
        log_ok "Swagger 文档已更新"
    else
        log_warn "swag 未安装, 跳过文档生成 (install: go install github.com/swaggo/swag/cmd/swag@latest)"
    fi

    log_info "编译 ${BINARY_NAME}..."
    make build
    log_ok "编译完成"

    # 启动前端口检测, 避免 "address already in use"
    local port
    port="$(resolve_port)"
    log_info "检测服务端口 ${port}..."
    check_port "$port" || exit 1

    log_info "启动服务 (端口 ${port})..."
    exec ./bin/"$BINARY_NAME"
}

# ── test: 运行测试 ──
cmd_test() {
    log_info "运行测试 (race detector)..."
    make test
}

# ── lint: 代码检查 ──
cmd_lint() {
    log_info "代码检查..."
    make lint
    log_ok "检查完成"
}

# ── clean: 清理 ──
cmd_clean() {
    log_info "清理构建产物..."
    make clean
    log_ok "清理完成"
}

# ── deps: 依赖管理 ──
cmd_deps() {
    log_info "更新 Go 依赖..."
    go mod download
    go mod tidy
    log_ok "依赖更新完成"
}

# ── kill: 关闭运行中的服务 ──
# 主逻辑: 先检测服务端口是否被占用, 被占用则关闭对应 PID; 端口工具不可用时回退按进程名清理
cmd_kill() {
    local port
    port="$(resolve_port)"
    log_info "检测服务端口 ${port}..."

    local port_pids
    port_pids="$(find_port_pids "$port" 2>/dev/null || true)"

    if [[ -n "$port_pids" ]]; then
        # 端口被占用 -> 关闭对应 PID
        kill_port_owner "$port"
        return $?
    fi

    # 端口未被占用: 端口检测工具缺失或进程尚未监听, 回退按进程名清理 (避免误杀本脚本)
    log_info "端口 ${port} 未被占用, 按进程名清理 ${BINARY_NAME}..."
    local pids
    pids="$(pgrep -f "bin/${BINARY_NAME}" 2>/dev/null || true)"
    if [[ -z "$pids" ]]; then
        log_ok "${BINARY_NAME} 未运行, 端口 ${port} 空闲, 无需处理"
        return 0
    fi
    log_info "命中进程 PID: ${pids//$'\n'/, }"

    log_info "发送 SIGTERM, 等待优雅关闭..."
    # shellcheck disable=SC2086
    kill -TERM $pids 2>/dev/null || true

    local waited=0
    while [[ $waited -lt 10 ]] && pgrep -f "bin/${BINARY_NAME}" &>/dev/null; do
        sleep 1
        ((waited++))
    done

    if pgrep -f "bin/${BINARY_NAME}" &>/dev/null; then
        log_warn "优雅关闭超时 (>10s), 强制 SIGKILL..."
        # shellcheck disable=SC2086
        pkill -9 -f "bin/${BINARY_NAME}" 2>/dev/null || true
        sleep 1
    fi

    if pgrep -f "bin/${BINARY_NAME}" &>/dev/null; then
        log_err "无法关闭 ${BINARY_NAME}, 请手动检查"
        return 1
    fi
    log_ok "${BINARY_NAME} 已关闭"
}

# ── swagger: 仅生成文档 ──
cmd_swagger() {
    require_cmd swag
    log_info "生成 Swagger 文档..."
    swag init -g cmd/server/main.go -o ./swagger --parseDependency --parseInternal 2>&1 | grep -vE "warning: (failed to get package name|failed to evaluate const)" || true
    log_ok "Swagger 文档已生成: swagger/"
}

# ── k6: 运行压力测试 ──
# 用法: ./build.sh k6 [scenario]
#   scenario: auth | idle | idle-settle | shop | shop-flash | mixed | all(默认)
#   脚本读取环境变量: BASE_URL(默认 http://localhost:8080), ITEM_ID(默认 1)
K6_DIR="scripts/k6"
declare -A K6_SCENARIOS=(
    [auth]="auth.js"
    [idle]="idle.js"
    [idle-settle]="idle_settle.js"
    [shop]="shop.js"
    [shop-flash]="shop_flash.js"
    [mixed]="mixed.js"
    [ws]="ws.js"
)
cmd_k6() {
    require_cmd k6
    local scenario="${1:-all}"

    if [[ "$scenario" == "all" ]]; then
        local failed=0
        for s in auth idle idle-settle shop shop-flash mixed ws; do
            log_info "===== k6 场景: ${s} (${K6_SCENARIOS[$s]}) ====="
            k6 run "${K6_DIR}/${K6_SCENARIOS[$s]}" || failed=1
        done
        if [[ $failed -ne 0 ]]; then
            log_err "存在失败的 k6 场景"
            return 1
        fi
        log_ok "全部 k6 场景通过"
        return 0
    fi

    if [[ -z "${K6_SCENARIOS[$scenario]:-}" ]]; then
        log_err "未知 k6 场景: $scenario (可选: auth idle idle-settle shop shop-flash mixed all)"
        return 1
    fi

    log_info "运行 k6 场景: ${scenario} (BASE_URL=${BASE_URL:-http://localhost:8080})"
    k6 run "${K6_DIR}/${K6_SCENARIOS[$scenario]}"
}

# ── 入口 ──
CMD="${1:-build}"
shift || true

case "$CMD" in
    init)    cmd_init    "$@" ;;
    docker)  cmd_docker  "$@" ;;
    build)   cmd_build   "$@" ;;
    test)    cmd_test    "$@" ;;
    lint)    cmd_lint    "$@" ;;
    clean)   cmd_clean   "$@" ;;
    deps)    cmd_deps    "$@" ;;
    swagger) cmd_swagger "$@" ;;
    kill)    cmd_kill    "$@" ;;
    k6)      cmd_k6      "$@" ;;
    -h|--help|help) show_usage ;;
    *)
        log_err "未知命令: $CMD"
        show_usage
        exit 1
        ;;
esac

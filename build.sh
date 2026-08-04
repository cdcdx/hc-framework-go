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

# ── 配置（go-zero 版：合并单进程 hc-server，兼容独立双进程 hc-rpc + gateway-api）──
BINARIES=("hc-server" "hc-rpc" "gateway-api")
HC_RPC_PORT=8001
GATEWAY_PORT=8080
METRICS_PORT=8080
KUBECTL_VERSION="v1.34.0"
KIND_VERSION="v0.22.0"
CLUSTER_NAME="dev"

# ── 帮助 ──
show_usage() {
    cat <<EOF
用法: ./build.sh <command> [options]

Commands:
  init        初始化开发环境 (安装 kubectl + kind, 创建本地 K8s 集群)
  build       编译并启动服务 (默认: hc-rpc + gateway-api)
  docker      构建 Docker 镜像并部署到 K8s
  test        运行测试 (go test ./...)
  lint        代码检查 (go vet + gofmt)
  clean       清理构建产物 (bin/)
  deps        安装/更新 Go 依赖
  kill        关闭运行中的服务 (释放 ${HC_RPC_PORT}/${GATEWAY_PORT} 端口)
  k6          运行 k6 压力测试 (可选场景: auth idle idle-settle shop shop-flash mixed all)

示例:
  ./build.sh                # 编译并启动 (make build && 启动服务)
  ./build.sh init           # 初始化环境
  ./build.sh kill           # 关闭服务
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

    # kubectl
    if command -v kubectl &>/dev/null; then
        log_ok "kubectl 已安装: $(kubectl version --client 2>/dev/null | head -1 || true)"
    else
        log_info "安装 kubectl ${KUBECTL_VERSION}..."
        curl -fsSLO "https://dl.k8s.io/release/${KUBECTL_VERSION}/bin/linux/amd64/kubectl"
        chmod +x kubectl
        sudo mv kubectl /usr/local/bin/kubectl
        log_ok "kubectl 安装完成"
    fi

    # kind
    if command -v kind &>/dev/null; then
        log_ok "kind 已安装: $(kind version 2>/dev/null | head -1 || true)"
    else
        log_info "安装 kind ${KIND_VERSION}..."
        curl -fsSLo /usr/local/bin/kind "https://kind.sigs.k8s.io/dl/${KIND_VERSION}/kind-linux-amd64"
        chmod +x /usr/local/bin/kind
        log_ok "kind 安装完成"
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

    log_info "初始化完成"
}

# ── docker: 构建镜像并部署 ──
cmd_docker() {
    require_cmd docker
    require_cmd kubectl

    log_info "构建 Docker 镜像..."
    docker build -t hc-framework:latest .
    log_ok "镜像构建完成"

    # 将镜像加载到 kind 集群
    if kind get clusters 2>/dev/null | grep -qx "$CLUSTER_NAME"; then
        kind load docker-image hc-framework:latest --name "$CLUSTER_NAME" 2>/dev/null || true
    fi

    log_info "部署到 Kubernetes..."
    kubectl apply -f deployments/ 2>/dev/null || log_warn "deployments/ 部署跳过（目录可能为空或不完整）"
    log_ok "部署完成"
}

# ── 端口检测 ──
find_port_pids() {
    local port="$1"
    if command -v fuser &>/dev/null; then
        fuser "$port"/tcp 2>/dev/null || true
    elif command -v ss &>/dev/null; then
        ss -lptnH "sport = :$port" 2>/dev/null | grep -oE 'pid=[0-9]+' | cut -d= -f2 | sort -u || true
    else
        echo ""
    fi
}

# ── build: 编译并启动（合并单进程模式）──
cmd_build() {
    log_info "编译合并单进程服务..."
    make build-server
    log_ok "编译完成: bin/hc-server"

    # 启动 hc-server（单进程：REST + 同进程后端 + metrics 共用 :8080）
    log_info "启动 hc-server (REST + metrics 共用 :${GATEWAY_PORT})..."
    ./bin/hc-server -f config/server.yaml &
    sleep 1

    log_ok "服务已启动: hc-server :${GATEWAY_PORT}"
    log_info "健康检查: curl http://localhost:${GATEWAY_PORT}/health"
    log_info "指标端点:   curl http://localhost:${GATEWAY_PORT}/metrics"

    # 等待前台进程
    wait
}

# ── server: 仅构建/运行合并单进程（与 build 等价，语义化别名）──
cmd_server() {
    cmd_build "$@"
}

# ── test ──
cmd_test() {
    log_info "运行测试..."
    make test
}

# ── lint ──
cmd_lint() {
    log_info "代码检查..."
    make lint
    log_ok "检查完成"
}

# ── clean ──
cmd_clean() {
    log_info "清理构建产物..."
    rm -rf bin/
    rm -f data/hc.db
    log_ok "清理完成"
}

# ── deps ──
cmd_deps() {
    log_info "更新 Go 依赖..."
    go mod download
    go mod tidy
    log_ok "依赖更新完成"
}

# ── kill: 关闭运行中的服务 ──
cmd_kill() {
    log_info "停止服务 (${HC_RPC_PORT}/${GATEWAY_PORT})..."
    make kill
}

# ── k6 ──
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
    server)  cmd_server  "$@" ;;
    test)    cmd_test    "$@" ;;
    lint)    cmd_lint    "$@" ;;
    clean)   cmd_clean   "$@" ;;
    deps)    cmd_deps    "$@" ;;
    kill)    cmd_kill    "$@" ;;
    k6)      cmd_k6      "$@" ;;
    -h|--help|help) show_usage ;;
    *)
        log_err "未知命令: $CMD"
        show_usage
        exit 1
        ;;
esac

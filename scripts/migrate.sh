#!/bin/bash
# ============================================
# 数据库迁移执行脚本
# 支持全部 4 个数据库: user / business / log / monitor
# 分层结构: migrations/<数据库>/<驱动>/
# DSN 统一从 config.yaml 读取，支持环境变量覆盖
# 用法: migrate.sh [up|down] [user|business|log|monitor|all]
# ============================================
set -euo pipefail

MIGRATIONS_DIR="./migrations"
CONFIG_FILE="${CONFIG_FILE:-config/config.yaml}"
MIGRATE_CMD="go run -tags sqlite3,mysql,postgres,clickhouse github.com/golang-migrate/migrate/v4/cmd/migrate@latest"

# 颜色
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
BLUE='\033[0;34m'; CYAN='\033[0;36m'; BOLD='\033[1m'; NC='\033[0m'

log_info()  { echo -e "${GREEN}[INFO]${NC}  $1" >&2; }
log_warn()  { echo -e "${YELLOW}[WARN]${NC}  $1" >&2; }
log_error() { echo -e "${RED}[ERROR]${NC} $1" >&2; }
log_step()  { echo -e "\n${BOLD}${CYAN}━━━ $1 ━━━${NC}" >&2; }
log_detail(){ echo -e "        ${BLUE}→${NC} $1" >&2; }

print_separator() {
    echo -e "${BLUE}============================================================${NC}" >&2
}

# ============================================
# 配置读取
# ============================================

config_yaml() {
    local keypath="$1"
    local fallback="${2:-}"
    if [ ! -f "$CONFIG_FILE" ]; then
        echo "$fallback"
        return
    fi
    python3 -c "
import yaml
with open('$CONFIG_FILE') as f:
    cfg = yaml.safe_load(f) or {}
for part in '$keypath'.split('.'):
    cfg = cfg.get(part, {}) if isinstance(cfg, dict) else {}
result = cfg if isinstance(cfg, (str, int, float, bool)) else ''
print(result if result != '' else '$fallback')
" 2>/dev/null || echo "$fallback"
}

# ============================================
# 数据库定义
# ============================================

ALL_DBS=(user business log monitor)

db_label() {
    case "$1" in
        user)     echo "用户" ;;
        business) echo "业务" ;;
        log)      echo "日志" ;;
        monitor)  echo "监控" ;;
        *)        echo "$1" ;;
    esac
}

# 获取指定数据库的驱动
get_driver() {
    local db=$1
    local env_var
    env_var=$(echo "APP_DATABASE_$(echo "$db" | tr '[:lower:]' '[:upper:]')_DRIVER")
    if [ -n "${!env_var:-}" ]; then
        echo "${!env_var}"
        return
    fi
    config_yaml "database.${db}.driver" ""
}

# 是否 SQL 类驱动
is_sql_driver() {
    case "$1" in
        mysql|postgres|sqlite|clickhouse) return 0 ;;
        *) return 1 ;;
    esac
}

# 获取 DSN（不含协议前缀）
get_dsn() {
    local db=$1
    local driver=$2
    local env_prefix
    env_prefix=$(echo "$db" | tr '[:lower:]' '[:upper:]')

    case "$driver" in
        mysql)
            local env_var="APP_DATABASE_${env_prefix}_MYSQL_MASTER"
            local dsn="${!env_var:-}"
            if [ -z "$dsn" ]; then
                dsn=$(config_yaml "database.${db}.mysql.master" \
                    "mysql://root:password@127.0.0.1:3306/hc_${db}?charset=utf8mb4&parseTime=True")
            fi
            # 剥离 mysql:// 前缀，转为 go-sql-driver 标准格式: user:pass@tcp(host:port)/db?params
            if [[ "$dsn" == mysql://* ]]; then
                dsn="${dsn#mysql://}"
                dsn=$(echo "$dsn" | sed -E 's|^([^:]+):([^@]+)@([^/]+)/(.+)$|\1:\2@tcp(\3)/\4|')
            fi
            ;;
        postgres)
            local env_var="APP_DATABASE_${env_prefix}_POSTGRES_MASTER"
            local raw="${!env_var:-}"
            if [ -z "$raw" ]; then
                raw=$(config_yaml "database.${db}.postgres.master" \
                    "postgres://postgres:postgres@127.0.0.1:5432/hc_${db}?sslmode=disable")
            fi
            if [[ "$raw" == postgres://* ]]; then
                dsn="${raw#postgres://}"
            elif [[ "$raw" == postgresql://* ]]; then
                dsn="${raw#postgresql://}"
            else
                dsn="$raw"
            fi
            ;;
        sqlite|sqlite3)
            local env_var="APP_DATABASE_${env_prefix}_DSN"
            local dsn="${!env_var:-}"
            if [ -z "$dsn" ]; then
                dsn=$(config_yaml "database.${db}.dsn" \
                    "file:./data/hc_${db}.db?cache=shared&_journal_mode=WAL")
            fi
            dsn="${dsn#file:}"
            ;;
        clickhouse)
            local env_var="APP_DATABASE_${env_prefix}_CLICKHOUSE_DSN"
            local raw_dsn="${!env_var:-}"
            if [ -z "$raw_dsn" ]; then
                raw_dsn=$(config_yaml "database.${db}.clickhouse.dsn" \
                    "clickhouse://default:@127.0.0.1:19000/hc_${db}")
            fi
            # golang-migrate clickhouse 驱动使用特有的 DSN 格式:
            #   clickhouse://host:port?username=user&password=pass&database=db&...
            # 与 clickhouse-go/v2 原生的 user:pass@host:port/db 格式不同，需转换
            dsn=$(python3 -c "
import re, sys
raw = sys.argv[1]
raw = re.sub(r'^clickhouse://', '', raw)
m = re.match(r'([^:]*):([^@]*)@([^/]+)/([^?]*)\??(.*)', raw)
if m:
    user, pwd, host, db, params = m.groups()
    parts = []
    if user: parts.append(f'username={user}')
    if pwd:  parts.append(f'password={pwd}')
    if db:   parts.append(f'database={db}')
    if params: parts.append(params)
    print(f'{host}?{\"&\".join(parts)}')
else:
    print(raw)
" "$raw_dsn")
            # 追加多语句参数
            case "$dsn" in
                *\?*) dsn="${dsn}&x-multi-statement=true" ;;
                *)    dsn="${dsn}?x-multi-statement=true" ;;
            esac
            ;;
        *)
            log_error "不支持的驱动: $driver"
            exit 1
            ;;
    esac
    echo "$dsn"
}

# ============================================
# 核心迁移逻辑
# ============================================

get_migrate_driver() {
    case "$1" in
        sqlite) echo "sqlite3" ;;
        *)      echo "$1" ;;
    esac
}

list_migration_files() {
    local path=$1
    if [ -d "$path" ]; then
        local count
        count=$(find "$path" -maxdepth 1 -type f \( -name "*.up.sql" -o -name "*.down.sql" \) | wc -l)
        log_detail "发现 ${count} 个迁移文件:"
        find "$path" -maxdepth 1 -type f \( -name "*.up.sql" -o -name "*.down.sql" \) | sort | while read -r f; do
            log_detail "  - $(basename "$f")"
        done
    fi
}

check_current_version() {
    local migration_path=$1
    local full_db_url=$2
    local version
    version=$($MIGRATE_CMD -path "$migration_path" -database "$full_db_url" version 2>&1 | grep -Eo '^[0-9]+' | head -1) || true
    if [ -n "$version" ]; then
        log_detail "当前版本: ${version}"
    else
        log_detail "当前版本: (无迁移记录)"
    fi
}

run_migration() {
    local db=$1
    local driver=$2
    local direction=$3
    local subdir="${db}/${driver}"
    local migration_path="${MIGRATIONS_DIR}/${subdir}"
    local migrate_driver
    migrate_driver=$(get_migrate_driver "$driver")

    if [ ! -d "$migration_path" ]; then
        log_warn "跳过: 迁移目录不存在 — ${migration_path}"
        return 1
    fi

    local dsn
    dsn=$(get_dsn "$db" "$driver")

    # 脱敏显示
    local masked
    if [ "$driver" = "sqlite" ] || [ "$driver" = "sqlite3" ]; then
        masked="$dsn"
    else
        masked=$(echo "$dsn" | sed -E 's/([^:]*:)[^@]*(@.*)/\1****\2/')
    fi

    print_separator
    log_info "$(db_label "$db")数据库: ${driver}  |  方向: ${direction}"
    log_detail "迁移目录: ${migration_path}"
    if [ "$driver" = "sqlite" ] || [ "$driver" = "sqlite3" ]; then
        log_detail "DB 文件: ${masked}"
    else
        log_detail "DSN: ${masked}"
    fi

    list_migration_files "$migration_path"

    local full_db_url="${migrate_driver}://${dsn}"

    check_current_version "$migration_path" "$full_db_url"

    log_detail "执行迁移..."
    local output exit_code
    output=$($MIGRATE_CMD \
        -path "$migration_path" \
        -database "$full_db_url" \
        "$direction" 2>&1)
    exit_code=$?
    echo "$output" | while read -r line; do log_detail "${line}"; done
    if [ $exit_code -ne 0 ]; then
        log_error "迁移命令执行失败（exit code: ${exit_code}）"
        return 1
    fi

    check_current_version "$migration_path" "$full_db_url"
    log_info "✓ $(db_label "$db")数据库 ${driver} ${direction} 迁移完成"
    return 0
}

# ============================================
# 主流程
# ============================================
main() {
    local direction="up"
    local filter="all"

    # 解析参数
    for arg in "$@"; do
        case "$arg" in
            up|down)                       direction="$arg" ;;
            user|business|log|monitor|all) filter="$arg" ;;
            *)                             log_error "未知参数: $arg"; exit 1 ;;
        esac
    done

    print_separator
    echo -e "${BOLD}${CYAN}  数据库迁移工具${NC}" >&2
    print_separator
    log_info "迁移方向: ${direction}"
    log_info "迁移范围: ${filter}"
    log_info "配置文件: ${CONFIG_FILE}"

    # 收集目标
    local targets=()
    declare -a target_dbs target_drvs
    local total=0 success=0 skipped=0

    for db in "${ALL_DBS[@]}"; do
        if [ "$filter" != "all" ] && [ "$db" != "$filter" ]; then
            continue
        fi

        local driver
        driver=$(get_driver "$db")

        if [ -z "$driver" ]; then
            log_detail "$(db_label "$db")数据库: driver 未设置 → 使用 SQLite 回退"
            target_dbs+=("$db")
            target_drvs+=("sqlite")
        elif is_sql_driver "$driver"; then
            target_dbs+=("$db")
            target_drvs+=("$driver")
        else
            log_detail "跳过 $(db_label "$db")数据库: driver=${driver} (非 SQL，无需表结构迁移)"
            (( ++skipped ))
        fi
    done

    if [ ${#target_dbs[@]} -eq 0 ]; then
        log_warn "没有需要迁移的目标"
        exit 0
    fi

    echo "" >&2
    log_info "将依次处理 ${#target_dbs[@]} 个目标:"
    for i in "${!target_dbs[@]}"; do
        log_detail "$(db_label "${target_dbs[$i]}")数据库 → ${target_drvs[$i]}"
    done

    # 逐个执行
    for i in "${!target_dbs[@]}"; do
        (( ++total ))
        local db="${target_dbs[$i]}"
        local drv="${target_drvs[$i]}"

        log_step "Step ${total}/${#target_dbs[@]}: $(db_label "$db")数据库 (${drv})"

        if run_migration "$db" "$drv" "$direction"; then
            (( ++success ))
        else
            log_error "$(db_label "$db")数据库 (${drv}) 迁移失败"
        fi
    done

    # 汇总
    echo "" >&2
    print_separator
    if [ $skipped -gt 0 ]; then
        log_detail "跳过: ${skipped} 个 (非 SQL 驱动)"
    fi
    echo -e "${BOLD}${GREEN}  迁移结果: ${success}/${total} 成功${NC}" >&2
    print_separator
}

main "$@"

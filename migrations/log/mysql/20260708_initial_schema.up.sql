-- ============================================
-- 日志数据库 MySQL 初始 Schema
-- 对齐 internal/model/log.go 的 GORM 模型字段类型。
-- 登录审计（event_type=login）的结构化字段 login_type / login_result /
-- fail_reason / device_info 直接内嵌于 audit_logs，不再单独建 login_records
-- （登录写入从 2 次降到 1 次，减少 DB IO）。
-- 所有建表均使用 IF NOT EXISTS 幂等，可重复执行。
-- ============================================

-- --------------------------------------------------
-- 审计日志表（含登录结构化字段）
-- --------------------------------------------------
CREATE TABLE IF NOT EXISTS audit_logs (
    id           BIGINT NOT NULL AUTO_INCREMENT,
    user_id      VARCHAR(191) NOT NULL DEFAULT '',
    event_type   VARCHAR(50)  NOT NULL DEFAULT '',
    login_type   VARCHAR(20)  NOT NULL DEFAULT '',
    login_result VARCHAR(10)  NOT NULL DEFAULT '',
    fail_reason  VARCHAR(255),
    device_info  VARCHAR(512),
    detail       TEXT,
    ip_address   VARCHAR(45),
    user_agent   VARCHAR(512),
    created_at   DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    INDEX idx_audit_logs_user_id      (user_id),
    INDEX idx_audit_logs_event_type   (event_type),
    INDEX idx_audit_logs_login_type   (login_type),
    INDEX idx_audit_logs_login_result (login_result),
    INDEX idx_audit_logs_created_at   (created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

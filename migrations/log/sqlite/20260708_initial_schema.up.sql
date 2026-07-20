-- ============================================
-- 日志数据库 SQLite 初始 Schema
-- 登录审计（event_type=login）的结构化字段 login_type / login_result /
-- fail_reason / device_info 直接内嵌于 audit_logs，不再单独建 login_records
-- （登录写入从 2 次降到 1 次，减少 DB IO）。
-- ============================================

CREATE TABLE IF NOT EXISTS audit_logs (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id      TEXT NOT NULL DEFAULT '',
    event_type   TEXT NOT NULL DEFAULT '',
    login_type   TEXT NOT NULL DEFAULT '',
    login_result TEXT NOT NULL DEFAULT '',
    fail_reason  TEXT,
    device_info  TEXT,
    detail       TEXT,
    ip_address   TEXT,
    user_agent   TEXT,
    created_at   DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_audit_logs_user_id      ON audit_logs(user_id);
CREATE INDEX IF NOT EXISTS idx_audit_logs_event_type   ON audit_logs(event_type);
CREATE INDEX IF NOT EXISTS idx_audit_logs_login_type   ON audit_logs(login_type);
CREATE INDEX IF NOT EXISTS idx_audit_logs_login_result ON audit_logs(login_result);
CREATE INDEX IF NOT EXISTS idx_audit_logs_created_at   ON audit_logs(created_at);

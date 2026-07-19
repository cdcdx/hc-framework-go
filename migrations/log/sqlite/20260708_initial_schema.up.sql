-- ============================================
-- 日志数据库 SQLite 初始 Schema
-- ============================================

CREATE TABLE IF NOT EXISTS audit_logs (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id     TEXT NOT NULL,
    event_type  TEXT NOT NULL,
    detail      TEXT,
    ip_address  TEXT,
    user_agent  TEXT,
    created_at  DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_audit_logs_user_id    ON audit_logs(user_id);
CREATE INDEX IF NOT EXISTS idx_audit_logs_event_type ON audit_logs(event_type);
CREATE INDEX IF NOT EXISTS idx_audit_logs_created_at ON audit_logs(created_at);

-- 结构化登录记录表（需求 §6.9 / §10.8）：保存 login_type / login_result / fail_reason 等结构化字段
CREATE TABLE IF NOT EXISTS login_records (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id      TEXT NOT NULL,
    login_type   TEXT NOT NULL,
    ip_address   TEXT,
    device_info  TEXT,
    login_result TEXT NOT NULL,
    fail_reason  TEXT,
    created_at   DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_login_records_user_id      ON login_records(user_id);
CREATE INDEX IF NOT EXISTS idx_login_records_login_type   ON login_records(login_type);
CREATE INDEX IF NOT EXISTS idx_login_records_login_result ON login_records(login_result);
CREATE INDEX IF NOT EXISTS idx_login_records_created_at   ON login_records(created_at);

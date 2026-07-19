-- ============================================
-- 监控数据库 SQLite 初始 Schema
-- ============================================

CREATE TABLE IF NOT EXISTS monitor_metrics (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id     TEXT,
    metric_type TEXT NOT NULL,
    metric_name TEXT NOT NULL,
    value       REAL NOT NULL,
    tags        TEXT,
    created_at  DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_monitor_metrics_user_id     ON monitor_metrics(user_id);
CREATE INDEX IF NOT EXISTS idx_monitor_metrics_metric_type ON monitor_metrics(metric_type);
CREATE INDEX IF NOT EXISTS idx_monitor_metrics_metric_name ON monitor_metrics(metric_name);
CREATE INDEX IF NOT EXISTS idx_monitor_metrics_created_at  ON monitor_metrics(created_at);

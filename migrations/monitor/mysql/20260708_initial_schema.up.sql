-- ============================================
-- 监控数据库 MySQL 初始 Schema
-- 对齐 internal/model/monitor.go 的 GORM 模型字段类型。
-- 所有建表均使用 IF NOT EXISTS 幂等，可重复执行。
-- ============================================

-- --------------------------------------------------
-- 监控指标表
-- --------------------------------------------------
CREATE TABLE IF NOT EXISTS monitor_metrics (
    id          BIGINT NOT NULL AUTO_INCREMENT,
    user_id     VARCHAR(191),
    metric_type VARCHAR(50)  NOT NULL DEFAULT '',
    metric_name VARCHAR(100) NOT NULL DEFAULT '',
    value       DOUBLE       NOT NULL DEFAULT 0,
    tags        TEXT,
    created_at  DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    INDEX idx_monitor_metrics_user_id     (user_id),
    INDEX idx_monitor_metrics_metric_type (metric_type),
    INDEX idx_monitor_metrics_metric_name (metric_name),
    INDEX idx_monitor_metrics_created_at  (created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

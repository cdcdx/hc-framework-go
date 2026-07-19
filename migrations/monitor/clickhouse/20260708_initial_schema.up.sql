-- ============================================
-- 监控数据库 ClickHouse 初始 Schema
-- ClickHouse MergeTree 引擎，ORDER BY 即主键索引
-- ============================================

CREATE TABLE IF NOT EXISTS monitor_metrics (
    id          Int64,
    user_id     String,
    metric_type String,
    metric_name String,
    value       Float64,
    tags        String,
    created_at  DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (created_at, metric_type, metric_name);

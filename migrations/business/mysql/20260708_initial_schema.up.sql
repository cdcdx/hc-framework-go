-- ============================================
-- 业务数据完整迁移 (MySQL)
-- 合并：初始表结构 + 消费侧进度表 + 索引优化 + 定时抢购活动表
-- 所有建表/建索引均使用 IF NOT EXISTS 幂等，可重复执行。
-- ============================================

-- --------------------------------------------------
-- 1. 挂机记录表
-- --------------------------------------------------
CREATE TABLE IF NOT EXISTS idle_records (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    user_id VARCHAR(64) NOT NULL,
    device_id VARCHAR(128) NOT NULL DEFAULT '',
    start_time DATETIME NOT NULL,
    end_time DATETIME,
    last_heartbeat_at DATETIME,
    duration_seconds INT NOT NULL DEFAULT 0,
    points_earned BIGINT NOT NULL DEFAULT 0,
    status ENUM('active', 'completed', 'timeout') NOT NULL DEFAULT 'active',
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_user_id (user_id),
    INDEX idx_user_start (user_id, start_time),
    INDEX idx_status (status),
    INDEX idx_status_hb (status, last_heartbeat_at),
    INDEX idx_user_created (user_id, created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- --------------------------------------------------
-- 1.1 每日挂机积分汇总表（根治 getDailyPointsDB 的全表 SUM 慢查询）
-- --------------------------------------------------
CREATE TABLE IF NOT EXISTS idle_daily_points (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    user_id VARCHAR(191) NOT NULL,
    day VARCHAR(20) NOT NULL,
    total BIGINT NOT NULL DEFAULT 0,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY idx_idle_daily_user_day (user_id, day)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- --------------------------------------------------
-- 2. 任务表
-- --------------------------------------------------
CREATE TABLE IF NOT EXISTS tasks (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    task_type ENUM('daily', 'weekly', 'achievement') NOT NULL,
    task_key VARCHAR(128) NOT NULL,
    task_name VARCHAR(256) NOT NULL DEFAULT '',
    target_value INT NOT NULL DEFAULT 1,
    reward_points BIGINT NOT NULL DEFAULT 0,
    is_active TINYINT(1) NOT NULL DEFAULT 1,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE INDEX idx_task_key (task_key),
    INDEX idx_task_type (task_type)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- --------------------------------------------------
-- 3. 用户任务进度表
-- --------------------------------------------------
CREATE TABLE IF NOT EXISTS user_task_progress (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    user_id VARCHAR(64) NOT NULL,
    task_id BIGINT NOT NULL,
    period VARCHAR(32) NOT NULL DEFAULT '',
    current_progress INT NOT NULL DEFAULT 0,
    is_completed TINYINT(1) NOT NULL DEFAULT 0,
    is_claimed TINYINT(1) NOT NULL DEFAULT 0,
    completed_at DATETIME,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE INDEX idx_user_task_period (user_id, task_id, period),
    INDEX idx_user_id (user_id),
    INDEX idx_task_id (task_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- --------------------------------------------------
-- 4. 事件去重表
-- --------------------------------------------------
CREATE TABLE IF NOT EXISTS event_dedup (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    event_id VARCHAR(128) NOT NULL,
    source VARCHAR(64) NOT NULL DEFAULT '',
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE INDEX uk_event_id (event_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- --------------------------------------------------
-- 5. 商品表
-- --------------------------------------------------
CREATE TABLE IF NOT EXISTS shop_items (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(256) NOT NULL DEFAULT '',
    description TEXT,
    price_points BIGINT NOT NULL DEFAULT 0,
    stock INT NOT NULL DEFAULT 0,
    version INT NOT NULL DEFAULT 0,
    image_url VARCHAR(512) NOT NULL DEFAULT '',
    category VARCHAR(64) NOT NULL DEFAULT '',
    is_active TINYINT(1) NOT NULL DEFAULT 1,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_category (category),
    INDEX idx_is_active (is_active)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- --------------------------------------------------
-- 5.1 库存分桶表（P2-5：根治抢兑写热点）
-- 将单商品库存拆分到 N 个桶行，普通兑换按 hash(userID)%N 选桶做原子条件扣减
-- （WHERE stock>=quantity），把原本集中在 shop_items 单行的行锁竞争分散到 N 行。
-- 桶是库存的权威存储；shop_items.stock 仅作展示/兜底（仓储层读路径返回桶之和）。
-- --------------------------------------------------
CREATE TABLE IF NOT EXISTS shop_item_stock_buckets (
    item_id BIGINT NOT NULL,
    bucket  INT   NOT NULL,
    stock   INT   NOT NULL DEFAULT 0,
    PRIMARY KEY (item_id, bucket)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- --------------------------------------------------
-- 6. 兑换订单表
-- --------------------------------------------------
CREATE TABLE IF NOT EXISTS redeem_orders (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    user_id VARCHAR(64) NOT NULL,
    item_id BIGINT NOT NULL,
    item_name VARCHAR(256) NOT NULL DEFAULT '',
    points_spent BIGINT NOT NULL DEFAULT 0,
    order_status ENUM('pending', 'completed', 'cancelled') NOT NULL DEFAULT 'completed',
    activity_id BIGINT NOT NULL DEFAULT 0,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_user_created (user_id, created_at),
    INDEX idx_item_id (item_id),
    INDEX idx_orders_activity_user (activity_id, user_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- --------------------------------------------------
-- 7. 积分流水表
-- --------------------------------------------------
CREATE TABLE IF NOT EXISTS points_transactions (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    user_id VARCHAR(64) NOT NULL,
    change_amount BIGINT NOT NULL DEFAULT 0,
    balance_after BIGINT NOT NULL DEFAULT 0,
    change_type ENUM('idle_reward', 'task_reward', 'redeem_spend', 'admin_adjust') NOT NULL,
    reference_id VARCHAR(128) NOT NULL DEFAULT '',
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_user_id (user_id),
    INDEX idx_created_at (created_at),
    INDEX idx_change_type (change_type)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- --------------------------------------------------
-- 8. 积分可靠投递表（Transactional Outbox）
-- --------------------------------------------------
CREATE TABLE IF NOT EXISTS points_outbox (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    user_id VARCHAR(64) NOT NULL,
    delta BIGINT NOT NULL DEFAULT 0,
    ref_type VARCHAR(32) NOT NULL DEFAULT '',
    ref_id VARCHAR(128) NOT NULL DEFAULT '',
    event_id VARCHAR(128) NOT NULL,
    status VARCHAR(16) NOT NULL DEFAULT 'pending',
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE INDEX uk_event_id (event_id),
    INDEX idx_user_id (user_id),
    INDEX idx_status_created (status, created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- --------------------------------------------------
-- 9. 消费侧进度表（Kafka offset 持久化）
-- --------------------------------------------------
CREATE TABLE IF NOT EXISTS kafka_consumer_progress (
    `topic`     VARCHAR(191) NOT NULL,
    `partition` INT          NOT NULL,
    `offset`    BIGINT       NOT NULL DEFAULT 0,
    `updated_at` TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (`topic`, `partition`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- --------------------------------------------------
-- 10. 消费侧幂等去重表
-- --------------------------------------------------
CREATE TABLE IF NOT EXISTS kafka_consumer_dedup (
    id        BIGINT       NOT NULL AUTO_INCREMENT PRIMARY KEY,
    event_id  VARCHAR(128) NOT NULL,
    `topic`     VARCHAR(191) NOT NULL DEFAULT '',
    `partition` INT          NOT NULL DEFAULT 0,
    `offset`    BIGINT       NOT NULL DEFAULT 0,
    created_at TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uk_consumer_dedup_event_id (event_id),
    INDEX idx_consumer_dedup_event_id (event_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- --------------------------------------------------
-- 11. 抢购活动表（定时抢购功能）
-- --------------------------------------------------
CREATE TABLE IF NOT EXISTS shop_flash_activities (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    item_id BIGINT NOT NULL,
    name VARCHAR(191) NOT NULL DEFAULT '',
    start_time DATETIME NOT NULL,
    end_time DATETIME NULL,
    limit_qty INT NOT NULL DEFAULT 0,
    sold_qty INT NOT NULL DEFAULT 0,
    per_user_limit INT NOT NULL DEFAULT 1,
    price_points BIGINT NOT NULL DEFAULT 0,
    status VARCHAR(20) NOT NULL DEFAULT 'active',
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_flash_item (item_id),
    INDEX idx_flash_start (start_time),
    INDEX idx_flash_status (status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- --------------------------------------------------
-- 12. 附加索引（独立 CREATE INDEX，兼容 MySQL 全版本）
-- 说明：CREATE TABLE 使用 IF NOT EXISTS，当表已存在时整段跳过，内联索引不会补建；
-- 故将以下索引以独立语句放在建表之后，保证「表已存在、但索引缺失」的重复迁移场景仍能补齐。
-- （MySQL 的 ALTER TABLE ... ADD INDEX IF NOT EXISTS 语法不被支持，故用普通 CREATE INDEX。）
-- --------------------------------------------------
CREATE INDEX idx_user_device_status ON idle_records (user_id, device_id, status);
CREATE INDEX idx_user_status ON idle_records (user_id, status);
CREATE INDEX idx_pts_user_created ON points_transactions (user_id, created_at);

-- ============================================
-- 初始数据：默认任务配置
-- ============================================
INSERT INTO tasks (task_type, task_key, task_name, target_value, reward_points) VALUES
('daily', 'daily_login', '每日登录', 1, 100),
('daily', 'daily_idle_30', '挂机满30分钟', 30, 200),
('daily', 'daily_redeem_1', '完成1次兑换', 1, 300),
('weekly', 'weekly_idle_300', '累计挂机300分钟', 300, 1000),
('weekly', 'weekly_redeem_3', '累计兑换3次', 3, 1500),
('achievement', 'achieve_points_10000', '累计获得10000积分', 10000, 5000),
('achievement', 'achieve_redeem_100', '兑换100次', 100, 10000)
ON DUPLICATE KEY UPDATE task_name = VALUES(task_name);
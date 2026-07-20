-- ============================================
-- 业务数据完整迁移 (PostgreSQL)
-- 合并：初始表结构 + 消费侧进度表 + 索引优化 + 定时抢购活动表
-- 所有建表/建索引均使用 IF NOT EXISTS 幂等，可重复执行。
-- ============================================

-- --------------------------------------------------
-- 1. 挂机记录表
-- --------------------------------------------------
CREATE TABLE IF NOT EXISTS idle_records (
    id BIGSERIAL PRIMARY KEY,
    user_id VARCHAR(64) NOT NULL,
    device_id VARCHAR(128) NOT NULL DEFAULT '',
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP,
    last_heartbeat_at TIMESTAMP,
    duration_seconds INTEGER NOT NULL DEFAULT 0,
    points_earned BIGINT NOT NULL DEFAULT 0,
    status VARCHAR(16) NOT NULL DEFAULT 'active'
        CHECK(status IN ('active', 'completed', 'timeout')),
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_idle_user_id ON idle_records(user_id);
CREATE INDEX IF NOT EXISTS idx_idle_user_start ON idle_records(user_id, start_time);
CREATE INDEX IF NOT EXISTS idx_idle_status ON idle_records(status);
CREATE INDEX IF NOT EXISTS idx_idle_status_hb ON idle_records(status, last_heartbeat_at);
CREATE INDEX IF NOT EXISTS idx_idle_user_created ON idle_records(user_id, created_at);
CREATE INDEX IF NOT EXISTS idx_idle_user_device_status ON idle_records(user_id, device_id, status);
CREATE INDEX IF NOT EXISTS idx_idle_user_status ON idle_records(user_id, status);
-- 覆盖索引：加速「当日已得挂机积分」范围聚合（user_id + created_at 过滤 + points_earned 覆盖），走索引-only 扫描避免回表随机 IO。
CREATE INDEX IF NOT EXISTS idx_idle_user_created_pts ON idle_records(user_id, created_at, points_earned);

-- --------------------------------------------------
-- 1.1 每日挂机积分汇总表（根治 getDailyPointsDB 的全表 SUM 慢查询）
-- --------------------------------------------------
CREATE TABLE IF NOT EXISTS idle_daily_points (
    id BIGSERIAL PRIMARY KEY,
    user_id VARCHAR(191) NOT NULL,
    day VARCHAR(20) NOT NULL,
    total BIGINT NOT NULL DEFAULT 0,
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_idle_daily_user_day ON idle_daily_points(user_id, day);

-- --------------------------------------------------
-- 2. 任务表
-- --------------------------------------------------
CREATE TABLE IF NOT EXISTS tasks (
    id BIGSERIAL PRIMARY KEY,
    task_type VARCHAR(16) NOT NULL
        CHECK(task_type IN ('daily', 'weekly', 'achievement')),
    task_key VARCHAR(128) NOT NULL UNIQUE,
    task_name VARCHAR(256) NOT NULL DEFAULT '',
    target_value INTEGER NOT NULL DEFAULT 1,
    reward_points BIGINT NOT NULL DEFAULT 0,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_tasks_type ON tasks(task_type);

-- --------------------------------------------------
-- 3. 用户任务进度表
-- --------------------------------------------------
CREATE TABLE IF NOT EXISTS user_task_progress (
    id BIGSERIAL PRIMARY KEY,
    user_id VARCHAR(64) NOT NULL,
    task_id BIGINT NOT NULL,
    period VARCHAR(32) NOT NULL DEFAULT '',
    current_progress INTEGER NOT NULL DEFAULT 0,
    is_completed BOOLEAN NOT NULL DEFAULT FALSE,
    is_claimed BOOLEAN NOT NULL DEFAULT FALSE,
    completed_at TIMESTAMP,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    UNIQUE(user_id, task_id, period)
);
CREATE INDEX IF NOT EXISTS idx_progress_user ON user_task_progress(user_id);
CREATE INDEX IF NOT EXISTS idx_progress_task ON user_task_progress(task_id);

-- --------------------------------------------------
-- 4. 事件去重表
-- --------------------------------------------------
CREATE TABLE IF NOT EXISTS event_dedup (
    id BIGSERIAL PRIMARY KEY,
    event_id VARCHAR(128) NOT NULL UNIQUE,
    source VARCHAR(64) NOT NULL DEFAULT '',
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_event_dedup_event_id ON event_dedup(event_id);

-- --------------------------------------------------
-- 5. 商品表
-- --------------------------------------------------
CREATE TABLE IF NOT EXISTS shop_items (
    id BIGSERIAL PRIMARY KEY,
    name VARCHAR(256) NOT NULL DEFAULT '',
    description TEXT,
    price_points BIGINT NOT NULL DEFAULT 0,
    stock INTEGER NOT NULL DEFAULT 0,
    version INTEGER NOT NULL DEFAULT 0,
    image_url VARCHAR(512) NOT NULL DEFAULT '',
    category VARCHAR(64) NOT NULL DEFAULT '',
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_shop_category ON shop_items(category);
CREATE INDEX IF NOT EXISTS idx_shop_active ON shop_items(is_active);

-- --------------------------------------------------
-- 5.1 库存分桶表（P2-5：根治抢兑写热点）
-- 将单商品库存拆分到 N 个桶行，普通兑换按 hash(userID)%N 选桶做原子条件扣减
-- （WHERE stock>=quantity），把原本集中在 shop_items 单行的行锁竞争分散到 N 行。
-- 桶是库存的权威存储；shop_items.stock 仅作展示/兜底（仓储层读路径返回桶之和）。
-- --------------------------------------------------
CREATE TABLE IF NOT EXISTS shop_item_stock_buckets (
    item_id BIGINT NOT NULL,
    bucket  INTEGER NOT NULL,
    stock   INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (item_id, bucket)
);

-- --------------------------------------------------
-- 6. 兑换订单表
-- --------------------------------------------------
CREATE TABLE IF NOT EXISTS redeem_orders (
    id BIGSERIAL PRIMARY KEY,
    user_id VARCHAR(64) NOT NULL,
    item_id BIGINT NOT NULL,
    item_name VARCHAR(256) NOT NULL DEFAULT '',
    points_spent BIGINT NOT NULL DEFAULT 0,
    order_status VARCHAR(16) NOT NULL DEFAULT 'completed'
        CHECK(order_status IN ('pending', 'completed', 'cancelled')),
    activity_id BIGINT NOT NULL DEFAULT 0,
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_orders_user ON redeem_orders(user_id, created_at);
CREATE INDEX IF NOT EXISTS idx_orders_item ON redeem_orders(item_id);
CREATE INDEX IF NOT EXISTS idx_orders_activity_user ON redeem_orders(activity_id, user_id);

-- --------------------------------------------------
-- 7. 积分流水表
-- --------------------------------------------------
CREATE TABLE IF NOT EXISTS points_transactions (
    id BIGSERIAL PRIMARY KEY,
    user_id VARCHAR(64) NOT NULL,
    change_amount BIGINT NOT NULL DEFAULT 0,
    balance_after BIGINT NOT NULL DEFAULT 0,
    change_type VARCHAR(16) NOT NULL
        CHECK(change_type IN ('idle_reward', 'task_reward', 'redeem_spend', 'admin_adjust')),
    reference_id VARCHAR(128) NOT NULL DEFAULT '',
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_txn_user ON points_transactions(user_id);
CREATE INDEX IF NOT EXISTS idx_txn_time ON points_transactions(created_at);
CREATE INDEX IF NOT EXISTS idx_txn_type ON points_transactions(change_type);
CREATE INDEX IF NOT EXISTS idx_pts_user_created ON points_transactions(user_id, created_at);

-- --------------------------------------------------
-- 8. 积分可靠投递表（Transactional Outbox）
-- --------------------------------------------------
CREATE TABLE IF NOT EXISTS points_outbox (
    id BIGSERIAL PRIMARY KEY,
    user_id VARCHAR(64) NOT NULL,
    delta BIGINT NOT NULL DEFAULT 0,
    ref_type VARCHAR(32) NOT NULL DEFAULT '',
    ref_id VARCHAR(128) NOT NULL DEFAULT '',
    event_id VARCHAR(128) NOT NULL UNIQUE,
    status VARCHAR(16) NOT NULL DEFAULT 'pending',
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_outbox_user_id ON points_outbox(user_id);
CREATE INDEX IF NOT EXISTS idx_outbox_status_created ON points_outbox(status, created_at);

-- --------------------------------------------------
-- 9. 消费侧进度表（Kafka offset 持久化）
-- --------------------------------------------------
CREATE TABLE IF NOT EXISTS kafka_consumer_progress (
    topic     VARCHAR(191) NOT NULL,
    partition INTEGER      NOT NULL,
    "offset"  BIGINT       NOT NULL DEFAULT 0,
    updated_at TIMESTAMP   NOT NULL DEFAULT NOW(),
    PRIMARY KEY (topic, partition)
);

-- --------------------------------------------------
-- 10. 消费侧幂等去重表
-- --------------------------------------------------
CREATE TABLE IF NOT EXISTS kafka_consumer_dedup (
    id        BIGSERIAL PRIMARY KEY,
    event_id  VARCHAR(128) NOT NULL UNIQUE,
    topic     VARCHAR(191) NOT NULL DEFAULT '',
    partition INTEGER      NOT NULL DEFAULT 0,
    "offset"  BIGINT       NOT NULL DEFAULT 0,
    created_at TIMESTAMP   NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_consumer_dedup_event_id ON kafka_consumer_dedup(event_id);

-- --------------------------------------------------
-- 11. 抢购活动表（定时抢购功能）
-- --------------------------------------------------
CREATE TABLE IF NOT EXISTS shop_flash_activities (
    id BIGSERIAL PRIMARY KEY,
    item_id BIGINT NOT NULL,
    name VARCHAR(191) NOT NULL DEFAULT '',
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP NULL,
    limit_qty INTEGER NOT NULL DEFAULT 0,
    sold_qty INTEGER NOT NULL DEFAULT 0,
    per_user_limit INTEGER NOT NULL DEFAULT 1,
    price_points BIGINT NOT NULL DEFAULT 0,
    status VARCHAR(20) NOT NULL DEFAULT 'active',
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_flash_item ON shop_flash_activities(item_id);
CREATE INDEX IF NOT EXISTS idx_flash_start ON shop_flash_activities(start_time);
CREATE INDEX IF NOT EXISTS idx_flash_status ON shop_flash_activities(status);

-- ============================================
-- 初始数据：默认任务配置
-- ============================================
INSERT INTO tasks (task_type, task_key, task_name, target_value, reward_points) VALUES
('daily',      'daily_login',          '每日登录',            1,   100),
('daily',      'daily_idle_30',        '挂机满30分钟',        30,  200),
('daily',      'daily_redeem_1',       '完成1次兑换',         1,   300),
('weekly',     'weekly_idle_300',      '累计挂机300分钟',      300, 1000),
('weekly',     'weekly_redeem_3',      '累计兑换3次',         3,   1500),
('achievement','achieve_points_10000', '累计获得10000积分',   10000, 5000),
('achievement','achieve_redeem_100',   '兑换100次',           100, 10000)
ON CONFLICT (task_key) DO UPDATE SET task_name = EXCLUDED.task_name;
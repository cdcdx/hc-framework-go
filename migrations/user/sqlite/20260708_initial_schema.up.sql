-- ============================================
-- 用户数据库 SQLite 初始 Schema（全部 9 张表）
-- 与 GORM AutoMigrate 模型完全对齐。
-- ============================================

-- 1. 用户表
CREATE TABLE IF NOT EXISTS users (
    user_id            TEXT PRIMARY KEY,
    username           TEXT NOT NULL,
    email              TEXT NOT NULL UNIQUE,
    password_hash      TEXT,
    google_id          TEXT UNIQUE,
    avatar_url         TEXT DEFAULT '',
    points_balance     INTEGER DEFAULT 0 CHECK(points_balance >= 0),
    status             TEXT DEFAULT 'active' CHECK(status IN ('active','banned','deleted')),
    password_changed_at DATETIME,
    created_at         DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at         DATETIME DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_users_email  ON users(email);
CREATE INDEX IF NOT EXISTS idx_users_status ON users(status);

-- 2. 挂机记录表
CREATE TABLE IF NOT EXISTS idle_records (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id           TEXT NOT NULL,
    device_id         TEXT NOT NULL DEFAULT '',
    start_time        DATETIME NOT NULL,
    end_time          DATETIME,
    last_heartbeat_at DATETIME,
    duration_seconds  INTEGER DEFAULT 0,
    points_earned     INTEGER DEFAULT 0,
    status            TEXT DEFAULT 'active',
    created_at        DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at        DATETIME DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_idle_user_status        ON idle_records(user_id, status);
CREATE INDEX IF NOT EXISTS idx_idle_user_device_status ON idle_records(user_id, device_id, status);
CREATE INDEX IF NOT EXISTS idx_idle_status_heartbeat   ON idle_records(status, last_heartbeat_at);

-- 3. 每日挂机积分汇总表
CREATE TABLE IF NOT EXISTS idle_daily_points (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id    TEXT NOT NULL,
    day        TEXT NOT NULL,
    total      INTEGER NOT NULL DEFAULT 0,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_idle_daily_user_day ON idle_daily_points(user_id, day);

-- 4. 任务表
CREATE TABLE IF NOT EXISTS tasks (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    task_type     TEXT NOT NULL,
    task_key      TEXT NOT NULL UNIQUE,
    task_name     TEXT NOT NULL DEFAULT '',
    target_value  INTEGER DEFAULT 1,
    reward_points INTEGER DEFAULT 0,
    is_active     INTEGER DEFAULT 1,
    created_at    DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at    DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- 5. 用户任务进度表
CREATE TABLE IF NOT EXISTS user_task_progress (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id          TEXT NOT NULL,
    task_id          INTEGER NOT NULL,
    period           TEXT DEFAULT '',
    current_progress INTEGER DEFAULT 0,
    is_completed     INTEGER DEFAULT 0,
    is_claimed       INTEGER DEFAULT 0,
    completed_at     DATETIME,
    created_at       DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at       DATETIME DEFAULT CURRENT_TIMESTAMP
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_user_task_period ON user_task_progress(user_id, task_id, period);

-- 6. 商品表
CREATE TABLE IF NOT EXISTS shop_items (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    name         TEXT NOT NULL DEFAULT '',
    description  TEXT,
    price_points INTEGER DEFAULT 0,
    stock        INTEGER DEFAULT 0,
    version      INTEGER DEFAULT 0,
    image_url    TEXT,
    category     TEXT DEFAULT '',
    is_active    INTEGER DEFAULT 1,
    created_at   DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at   DATETIME DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_shop_items_category ON shop_items(category);
CREATE INDEX IF NOT EXISTS idx_shop_items_is_active ON shop_items(is_active);

-- 7. 兑换订单表
CREATE TABLE IF NOT EXISTS redeem_orders (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id      TEXT NOT NULL,
    item_id      INTEGER NOT NULL,
    item_name    TEXT NOT NULL DEFAULT '',
    points_spent INTEGER DEFAULT 0,
    order_status TEXT DEFAULT 'completed',
    activity_id  INTEGER NOT NULL DEFAULT 0,
    created_at   DATETIME DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_redeem_user_created  ON redeem_orders(user_id, created_at);
CREATE INDEX IF NOT EXISTS idx_redeem_item           ON redeem_orders(item_id);
CREATE INDEX IF NOT EXISTS idx_redeem_activity_user  ON redeem_orders(activity_id, created_at);

-- 8. 抢购活动表
CREATE TABLE IF NOT EXISTS shop_flash_activities (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    name            TEXT NOT NULL,
    item_id         INTEGER NOT NULL,
    limit_qty       INTEGER NOT NULL DEFAULT 0,
    sold_qty        INTEGER NOT NULL DEFAULT 0,
    bucket_count    INTEGER NOT NULL DEFAULT 1,
    per_user_limit  INTEGER NOT NULL DEFAULT 1,
    price_points    INTEGER NOT NULL DEFAULT 0,
    start_time      DATETIME NOT NULL,
    end_time        DATETIME,
    status          TEXT DEFAULT 'pending',
    created_at      DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at      DATETIME DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_shop_flash_item ON shop_flash_activities(item_id);
CREATE INDEX IF NOT EXISTS idx_shop_flash_start ON shop_flash_activities(start_time);
CREATE INDEX IF NOT EXISTS idx_shop_flash_status ON shop_flash_activities(status);

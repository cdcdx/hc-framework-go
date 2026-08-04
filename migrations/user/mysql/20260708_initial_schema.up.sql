-- ============================================
-- 用户数据库 MySQL 初始 Schema（全部 9 张表）
-- 与 GORM AutoMigrate 模型完全对齐。
-- 所有建表均使用 IF NOT EXISTS 幂等，可重复执行。
-- ============================================

-- --------------------------------------------------
-- 1. 用户表
-- --------------------------------------------------
CREATE TABLE IF NOT EXISTS users (
    user_id             VARCHAR(191) NOT NULL,
    username            VARCHAR(100) NOT NULL DEFAULT '',
    email               VARCHAR(200) NOT NULL DEFAULT '',
    password_hash       VARCHAR(255),
    google_id           VARCHAR(191),
    avatar_url          TEXT,
    points_balance      BIGINT        NOT NULL DEFAULT 0,
    status              VARCHAR(20)   NOT NULL DEFAULT 'active',
    password_changed_at DATETIME,
    created_at          DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at          DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (user_id),
    UNIQUE KEY uk_users_email    (email),
    UNIQUE KEY uk_users_google_id (google_id),
    INDEX idx_users_status (status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- --------------------------------------------------
-- 2. 挂机记录表
-- --------------------------------------------------
CREATE TABLE IF NOT EXISTS idle_records (
    id                BIGINT       NOT NULL AUTO_INCREMENT,
    user_id           VARCHAR(191) NOT NULL,
    device_id         VARCHAR(191) NOT NULL DEFAULT '',
    start_time        DATETIME     NOT NULL,
    end_time          DATETIME,
    last_heartbeat_at DATETIME,
    duration_seconds  INT          DEFAULT 0,
    points_earned     BIGINT       DEFAULT 0,
    status            VARCHAR(20)  DEFAULT 'active',
    created_at        DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at        DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    INDEX idx_idle_user_status        (user_id, status),
    INDEX idx_idle_user_device_status (user_id, device_id, status),
    INDEX idx_idle_status_heartbeat   (status, last_heartbeat_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- --------------------------------------------------
-- 3. 每日挂机积分汇总表
-- --------------------------------------------------
CREATE TABLE IF NOT EXISTS idle_daily_points (
    id         BIGINT       NOT NULL AUTO_INCREMENT,
    user_id    VARCHAR(191) NOT NULL,
    day        VARCHAR(20)  NOT NULL,
    total      BIGINT       NOT NULL DEFAULT 0,
    updated_at DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE KEY uk_idle_daily_user_day (user_id, day)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- --------------------------------------------------
-- 4. 任务表
-- --------------------------------------------------
CREATE TABLE IF NOT EXISTS tasks (
    id            BIGINT       NOT NULL AUTO_INCREMENT,
    task_type     VARCHAR(50)  NOT NULL,
    task_key      VARCHAR(191) NOT NULL,
    task_name     VARCHAR(191) NOT NULL DEFAULT '',
    target_value  INT          DEFAULT 1,
    reward_points BIGINT       DEFAULT 0,
    is_active     TINYINT(1)   DEFAULT 1,
    created_at    DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at    DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE KEY uk_tasks_task_key (task_key)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- --------------------------------------------------
-- 5. 用户任务进度表
-- --------------------------------------------------
CREATE TABLE IF NOT EXISTS user_task_progress (
    id               BIGINT       NOT NULL AUTO_INCREMENT,
    user_id          VARCHAR(191) NOT NULL,
    task_id          BIGINT       NOT NULL,
    period           VARCHAR(50)  DEFAULT '',
    current_progress INT          DEFAULT 0,
    is_completed     TINYINT(1)   DEFAULT 0,
    is_claimed       TINYINT(1)   DEFAULT 0,
    completed_at     DATETIME,
    created_at       DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at       DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE KEY uk_user_task_period (user_id, task_id, period)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- --------------------------------------------------
-- 6. 商品表
-- --------------------------------------------------
CREATE TABLE IF NOT EXISTS shop_items (
    id           BIGINT       NOT NULL AUTO_INCREMENT,
    name         VARCHAR(191) NOT NULL DEFAULT '',
    description  TEXT,
    price_points BIGINT       DEFAULT 0,
    stock        INT          DEFAULT 0,
    version      INT          DEFAULT 0,
    image_url    TEXT,
    category     VARCHAR(100) DEFAULT '',
    is_active    TINYINT(1)   DEFAULT 1,
    created_at   DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at   DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    INDEX idx_shop_items_category (category),
    INDEX idx_shop_items_is_active (is_active)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- --------------------------------------------------
-- 7. 兑换订单表
-- --------------------------------------------------
CREATE TABLE IF NOT EXISTS redeem_orders (
    id           BIGINT       NOT NULL AUTO_INCREMENT,
    user_id      VARCHAR(191) NOT NULL,
    item_id      BIGINT       NOT NULL,
    item_name    VARCHAR(191) NOT NULL DEFAULT '',
    points_spent BIGINT       DEFAULT 0,
    order_status VARCHAR(20)  DEFAULT 'completed',
    activity_id  BIGINT       NOT NULL DEFAULT 0,
    created_at   DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    INDEX idx_redeem_user_created (user_id, created_at),
    INDEX idx_redeem_item         (item_id),
    INDEX idx_redeem_activity_user (activity_id, created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- --------------------------------------------------
-- 8. 抢购活动表
-- --------------------------------------------------
CREATE TABLE IF NOT EXISTS shop_flash_activities (
    id              BIGINT       NOT NULL AUTO_INCREMENT,
    name            VARCHAR(255) NOT NULL,
    item_id         BIGINT       NOT NULL,
    limit_qty       INT          NOT NULL DEFAULT 0,
    sold_qty        INT          NOT NULL DEFAULT 0,
    bucket_count    INT          NOT NULL DEFAULT 1,
    per_user_limit  INT          NOT NULL DEFAULT 1,
    price_points    BIGINT       NOT NULL DEFAULT 0,
    start_time      DATETIME     NOT NULL,
    end_time        DATETIME,
    status          VARCHAR(20)  DEFAULT 'pending',
    created_at      DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at      DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    INDEX idx_shop_flash_item   (item_id),
    INDEX idx_shop_flash_start  (start_time),
    INDEX idx_shop_flash_status (status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- ============================================
-- 用户数据库 MySQL 初始 Schema
-- 对齐 internal/model/user.go 的 GORM 模型字段类型。
-- user_id 为业务主键（varchar(191)，无自增），与 GORM primaryKey 一致。
-- 所有建表均使用 IF NOT EXISTS 幂等，可重复执行。
-- ============================================

-- --------------------------------------------------
-- 用户表
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

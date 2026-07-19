-- ============================================
-- 用户数据库 SQLite 初始 Schema
-- ============================================

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

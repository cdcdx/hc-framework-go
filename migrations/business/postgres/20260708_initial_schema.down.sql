-- 回滚：删除所有业务表（整合版本，覆盖 12 张表，含库存分桶表）
DROP TABLE IF EXISTS shop_item_stock_buckets;
DROP TABLE IF EXISTS shop_flash_activities;
DROP TABLE IF EXISTS kafka_consumer_dedup;
DROP TABLE IF EXISTS kafka_consumer_progress;
DROP TABLE IF EXISTS points_outbox;
DROP TABLE IF EXISTS points_transactions;
DROP TABLE IF EXISTS event_dedup;
DROP TABLE IF EXISTS redeem_orders;
DROP TABLE IF EXISTS shop_items;
DROP TABLE IF EXISTS user_task_progress;
DROP TABLE IF EXISTS tasks;
DROP TABLE IF EXISTS idle_records;
DROP TABLE IF EXISTS idle_daily_points;
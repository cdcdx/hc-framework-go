package gormx

import (
	"fmt"

	"gorm.io/driver/mysql"
	"gorm.io/driver/postgres"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

// OpenDriver 根据驱动名创建 gorm.Dialector。
// 支持: sqlite / mysql / postgres。
// mongodb / clickhouse / elasticsearch 等其他驱动返回错误（待实现）。
func OpenDriver(driver, dsn string) (gorm.Dialector, error) {
	switch driver {
	case "sqlite":
		return sqlite.Open(dsn), nil
	case "mysql":
		return mysql.Open(dsn), nil
	case "postgres":
		return postgres.Open(dsn), nil
	case "mongodb", "clickhouse", "elasticsearch":
		return nil, fmt.Errorf("driver %s not yet implemented (use mongodb/clickhouse/elasticsearch adapter)", driver)
	default:
		return nil, fmt.Errorf("unsupported driver: %s (available: sqlite/mysql/postgres)", driver)
	}
}

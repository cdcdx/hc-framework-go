// Package gormx GORM 连接工厂（多方言）。
package gormx

import (
	"fmt"

	"gorm.io/driver/mysql"
	"gorm.io/driver/postgres"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

// Open 按 driver 打开数据库连接：
//   - mysql / postgres / sqlite（默认，零依赖启动）
func Open(driver, dsn string) (*gorm.DB, error) {
	var dialector gorm.Dialector
	switch driver {
	case "mysql":
		dialector = mysql.Open(dsn)
	case "postgres", "postgresql":
		dialector = postgres.Open(dsn)
	default:
		dialector = sqlite.Open(dsn)
	}

	db, err := gorm.Open(dialector, &gorm.Config{})
	if err != nil {
		return nil, fmt.Errorf("open %s db: %w", driver, err)
	}
	return db, nil
}

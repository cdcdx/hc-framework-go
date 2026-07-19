package db

import (
	"log"
	"os"
	"time"

	gormlogger "gorm.io/gorm/logger"
)

// newGormLogger 返回 GORM 日志器（Warn 级别 + 慢查询 200ms）。
//
// 开启 IgnoreRecordNotFoundError：使 Start/查询预检时的 "record not found"
// 不再作为 Error 打印到日志，消除压测与正常路径下大量无害的
// record-not-found 噪音（这些未被找到的记录已被业务代码正确当作“不存在”处理）。
// 等价于 gormlogger.Default.LogMode(Warn)，仅额外屏蔽 RecordNotFound 噪音。
func newGormLogger() gormlogger.Interface {
	return gormlogger.New(
		log.New(os.Stdout, "\r\n", log.LstdFlags),
		gormlogger.Config{
			SlowThreshold:             200 * time.Millisecond,
			LogLevel:                  gormlogger.Warn,
			IgnoreRecordNotFoundError: true,
		},
	)
}

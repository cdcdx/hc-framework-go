// Package logger 日志封装与初始化。
package logger

import (
	"fmt"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var (
	atomicLevel = zap.NewAtomicLevelAt(zap.InfoLevel)
	global      *zap.Logger
)

// Init 初始化全局 logger，支持 "json" 和 "console" 两种格式
func Init(level, format string) {
	setLevel(level)

	var config zap.Config
	switch format {
	case "console":
		config = zap.NewDevelopmentConfig()
		config.EncoderConfig.EncodeLevel = zapcore.CapitalColorLevelEncoder
	default: // "json" 或空
		config = zap.NewProductionConfig()
	}

	config.Level = atomicLevel
	config.EncoderConfig.TimeKey = "timestamp"
	config.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder

	var err error
	global, err = config.Build()
	if err != nil {
		global = zap.NewNop()
	}
}

// L 返回全局 logger
func L() *zap.Logger {
	if global == nil {
		Init("info", "json")
	}
	return global
}

// Sync 刷新日志缓冲区
func Sync() {
	if global != nil {
		_ = global.Sync()
	}
}

// SetLevel 动态调整日志级别，level 无效时返回 error
func SetLevel(level string) error {
	return setLevel(level)
}

// GetLevel 返回当前日志级别
func GetLevel() string {
	return atomicLevel.Level().String()
}

func setLevel(level string) error {
	switch level {
	case "debug":
		atomicLevel.SetLevel(zap.DebugLevel)
	case "info":
		atomicLevel.SetLevel(zap.InfoLevel)
	case "warn":
		atomicLevel.SetLevel(zap.WarnLevel)
	case "error":
		atomicLevel.SetLevel(zap.ErrorLevel)
	case "fatal":
		atomicLevel.SetLevel(zap.FatalLevel)
	default:
		return fmt.Errorf("unknown log level: %s", level)
	}
	return nil
}

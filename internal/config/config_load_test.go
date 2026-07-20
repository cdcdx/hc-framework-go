package config

import (
	"testing"
	"time"
)

// TestLoadShippedConfig 校验随仓库发布的 config/config.yaml 能被正确解析并通过 Validate。
// 这能防止「代码/文档新增字段后配置结构漂移」：若某字段在 struct 中改名却未同步 yaml，
// viper 不会报错（忽略未知键），但此处断言关键字段已被正确解码。
func TestLoadShippedConfig(t *testing.T) {
	// 从 internal/config 回退到仓库根的 config/config.yaml。
	cfg, err := Load("../../config/config.yaml")
	if err != nil {
		t.Fatalf("Load shipped config: %v", err)
	}

	if cfg.Server.Port <= 0 {
		t.Fatalf("server.port = %d, want > 0", cfg.Server.Port)
	}
	if cfg.Server.Mode == "" {
		t.Fatal("server.mode must not be empty")
	}

	// 与 docs/202_配置参考.md 对齐的关键新增字段（此前缺失、已补文档）：
	if cfg.Server.ReadHeaderTimeout <= 0 {
		t.Fatalf("server.read_header_timeout = %v, want > 0", cfg.Server.ReadHeaderTimeout)
	}
	if cfg.Server.IdleTimeout <= 0 {
		t.Fatalf("server.idle_timeout = %v, want > 0", cfg.Server.IdleTimeout)
	}
	if cfg.Server.IdleTimeout <= 30*time.Second {
		t.Fatalf("server.idle_timeout = %v, should be > heartbeat(30s) to avoid premature idle close", cfg.Server.IdleTimeout)
	}

	if len(cfg.MQ.Kafka.Brokers) == 0 {
		t.Fatal("mq.kafka.brokers must be configured")
	}
	if cfg.MQ.Kafka.Producer.QueueSize <= 0 {
		t.Fatalf("mq.kafka.producer.queue_size = %d, want > 0", cfg.MQ.Kafka.Producer.QueueSize)
	}
	if cfg.MQ.Kafka.Consumer.RetryMax <= 0 {
		t.Fatalf("mq.kafka.consumer.retry_max = %d, want > 0", cfg.MQ.Kafka.Consumer.RetryMax)
	}
	if cfg.MQ.Kafka.Consumer.CloseTimeout <= 0 {
		t.Fatalf("mq.kafka.consumer.close_timeout = %v, want > 0", cfg.MQ.Kafka.Consumer.CloseTimeout)
	}
}

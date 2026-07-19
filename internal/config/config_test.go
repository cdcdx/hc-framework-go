package config

import "testing"

// TestKafkaConsumerConfig_CheckDeadConfig 锁定「无消费组直读模式下死配置告警」语义（13 §6.5 #5 / §3.55）：
// 仅当字段被设为有意义值时才告警，零值视为未配置不误报；batch_trigger_interval 不在此列（仍生效）。
func TestKafkaConsumerConfig_CheckDeadConfig(t *testing.T) {
	// 零值（未配置）→ 无告警
	zero := KafkaConsumerConfig{}
	if got := zero.CheckDeadConfig(); len(got) != 0 {
		t.Fatalf("zero-value config should produce no warnings, got %v", got)
	}

	// 全部死配置设为有意义值 → 4 条告警
	full := KafkaConsumerConfig{
		GroupID:           "hc-consumer-group",
		EnableAutoCommit:  true,
		MaxPollRecords:    100,
		BatchTriggerCount: 100,
	}
	warns := full.CheckDeadConfig()
	if len(warns) != 4 {
		t.Fatalf("expected 4 dead-config warnings, got %d: %v", len(warns), warns)
	}
	for _, w := range warns {
		if w == "" {
			t.Fatalf("warning must not be empty")
		}
	}

	// 部分设置 → 仅对应告警
	partial := KafkaConsumerConfig{GroupID: "g1", EnableAutoCommit: false, MaxPollRecords: 0, BatchTriggerCount: 0}
	if got := partial.CheckDeadConfig(); len(got) != 1 {
		t.Fatalf("expected exactly 1 warning for group_id only, got %v", got)
	}

	// batch_trigger_interval 仍生效，不应出现在死配置告警中
	interval := KafkaConsumerConfig{BatchTriggerInterval: 2_000_000_000} // 2s
	if got := interval.CheckDeadConfig(); len(got) != 0 {
		t.Fatalf("batch_trigger_interval must not be flagged as dead config, got %v", got)
	}
}

// TestConfig_Validate_KafkaInstanceAssign 验证 Kafka 无消费组多实例分区分配的越界配置在
// 启动期 / 热更新期被 Validate 拦截（见 13 §6.5 #1 / §3.53 / 本轮）：越界会让 assignPartitions
// 退化全量消费，多实例部署下造成分区被重复处理，故必须在配置层拦截而非运行期退化。
func TestConfig_Validate_KafkaInstanceAssign(t *testing.T) {
	// base 构造一个「仅 Kafka 实例分配相关字段待覆盖、其余必填均已合法」的配置，隔离本项校验。
	base := func() *Config {
		return &Config{
			Server: ServerConfig{Mode: "test", Port: 8080}, // 通过必填校验
			MQ:     MQConfig{Type: "kafka"},
		}
	}

	// 单实例（instance_count<=1）：不裁剪，InstanceID 任意值均合法（向后兼容）。
	single := base()
	single.MQ.Kafka.Consumer = KafkaConsumerConfig{InstanceCount: 0, InstanceID: 99}
	if err := single.Validate(); err != nil {
		t.Fatalf("single-instance config should pass validate, got %v", err)
	}

	// 多实例：合法分配（id 落在 [0, count)）。
	ok := base()
	ok.MQ.Kafka.Consumer = KafkaConsumerConfig{InstanceCount: 3, InstanceID: 2}
	if err := ok.Validate(); err != nil {
		t.Fatalf("valid instance assign (id=2/count=3) should pass, got %v", err)
	}

	// 多实例：InstanceID == InstanceCount（越界）→ 必须拦截。
	eq := base()
	eq.MQ.Kafka.Consumer = KafkaConsumerConfig{InstanceCount: 2, InstanceID: 2}
	if err := eq.Validate(); err == nil {
		t.Fatal("instance_id == instance_count must be rejected by Validate")
	}

	// 多实例：InstanceID 为负（越界）→ 必须拦截。
	neg := base()
	neg.MQ.Kafka.Consumer = KafkaConsumerConfig{InstanceCount: 2, InstanceID: -1}
	if err := neg.Validate(); err == nil {
		t.Fatal("negative instance_id must be rejected by Validate")
	}

	// 非 kafka 类型：跳过实例分配校验，避免误伤其它 MQ 后端（即便 Kafka 段越界也不校验）。
	other := base()
	other.MQ.Type = "rabbitmq"
	other.MQ.Kafka.Consumer = KafkaConsumerConfig{InstanceCount: 2, InstanceID: 99} // 越界但非 kafka
	if err := other.Validate(); err != nil {
		t.Fatalf("non-kafka type should skip instance assign validation, got %v", err)
	}
}

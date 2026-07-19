package mq

import (
	"testing"

	"github.com/cdcdx/hc-framework-go/internal/config"
)

// NewRabbitMQProducer 对空 url 返回 error（factory 在无 url 时返回 nil 保持 no-op）。
func TestNewRabbitMQProducer_EmptyURL(t *testing.T) {
	p, err := NewRabbitMQProducer(mqCfg(nil), "g1", nil)
	if err == nil {
		t.Fatal("want error for empty rabbitmq url")
	}
	if p != nil {
		t.Fatalf("want nil producer on error, got %T", p)
	}
}

// NewRabbitMQConsumer 对空 url 返回 error。
func TestNewRabbitMQConsumer_EmptyURL(t *testing.T) {
	c, err := NewRabbitMQConsumer(mqCfg(nil), "g1", nil)
	if err == nil {
		t.Fatal("want error for empty rabbitmq url")
	}
	if c != nil {
		t.Fatalf("want nil consumer on error, got %T", c)
	}
}

// SkipIfNoRabbitMQ 标注：真实连接（amqp.Dial）需要可用的 RabbitMQ 实例，
// 由集成测试覆盖；单元测试仅验证 url 缺省时的 error/no-op 分支与 topic 映射。
var _ = config.MQConfig{}

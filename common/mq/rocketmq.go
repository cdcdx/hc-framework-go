package mq

import (
	"context"
	"time"

	"github.com/apache/rocketmq-client-go/v2"
	"github.com/apache/rocketmq-client-go/v2/consumer"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/apache/rocketmq-client-go/v2/producer"
	"github.com/cdcdx/hc-framework-go/common/metrics"
	"github.com/zeromicro/go-zero/core/logx"
)

// resolveRocketMQNameServer 返回 NameServer 地址列表。
func resolveRocketMQNameServer(cfg Config) primitive.NamesrvAddr {
	if len(cfg.RocketMQ.NameServer) > 0 {
		return primitive.NamesrvAddr(cfg.RocketMQ.NameServer)
	}
	return primitive.NamesrvAddr{"127.0.0.1:9876"}
}

func resolveRocketMQGroup(cfg Config) string {
	if cfg.RocketMQ.Group != "" {
		return cfg.RocketMQ.Group
	}
	return "hc-framework"
}

// rocketmqProducer RocketMQ 消息生产者。
type rocketmqProducer struct {
	p     rocketmq.Producer
	topic string
}

func newRocketMQProducer(cfg Config) *rocketmqProducer {
	opts := []producer.Option{
		producer.WithNameServer(resolveRocketMQNameServer(cfg)),
		producer.WithGroupName(resolveRocketMQGroup(cfg)),
	}
	if cfg.RocketMQ.Retry > 0 {
		opts = append(opts, producer.WithRetry(cfg.RocketMQ.Retry))
	}
	if cfg.RocketMQ.Namespace != "" {
		opts = append(opts, producer.WithNamespace(cfg.RocketMQ.Namespace))
	}
	if cfg.RocketMQ.AccessKey != "" {
		opts = append(opts, producer.WithCredentials(primitive.Credentials{
			AccessKey: cfg.RocketMQ.AccessKey,
			SecretKey: cfg.RocketMQ.SecretKey,
		}))
	}
	p, err := rocketmq.NewProducer(opts...)
	if err != nil {
		logx.Errorf("[mq-rocketmq] create producer error: %v", err)
		return &rocketmqProducer{p: nil, topic: cfg.RocketMQ.Topic}
	}
	if err := p.Start(); err != nil {
		logx.Errorf("[mq-rocketmq] start producer error: %v", err)
	}
	return &rocketmqProducer{p: p, topic: cfg.RocketMQ.Topic}
}

func (r *rocketmqProducer) send(ctx context.Context, topic, key string, value []byte) error {
	if r.p == nil {
		return ErrProducerNotStarted
	}
	t := topic
	if t == "" {
		t = r.topic
	}
	if t == "" {
		return ErrTopicEmpty
	}
	msg := &primitive.Message{
		Topic: t,
		Body:  value,
	}
	if key != "" {
		msg.WithKeys([]string{key})
	}
	_, err := r.p.SendSync(ctx, msg)
	return err
}

func (r *rocketmqProducer) Send(ctx context.Context, topic, key string, value []byte) error {
	err := r.send(ctx, topic, key, value)
	if err != nil {
		metrics.MQProduceTotal.WithLabelValues(topic, "error").Inc()
	} else {
		metrics.MQProduceTotal.WithLabelValues(topic, "ok").Inc()
	}
	return err
}

func (r *rocketmqProducer) SendAsync(ctx context.Context, topic, key string, value []byte) error {
	go func() {
		// async 用 Background 避免 ctx 取消导致消息丢失，日志保留上游 ctx。
		if err := r.send(context.Background(), topic, key, value); err != nil {
			logx.WithContext(ctx).Errorf("[mq-rocketmq] async send error: %v", err)
		}
	}()
	return nil
}

func (r *rocketmqProducer) Close() error {
	if r.p == nil {
		return nil
	}
	return r.p.Shutdown()
}

// rocketmqConsumer RocketMQ 消息消费者（推模式）。
type rocketmqConsumer struct {
	c     rocketmq.PushConsumer
	topic string
}

func newRocketMQConsumer(cfg Config) *rocketmqConsumer {
	topic := cfg.RocketMQ.Topic
	group := resolveRocketMQGroup(cfg)
	opts := []consumer.Option{
		consumer.WithNameServer(resolveRocketMQNameServer(cfg)),
		consumer.WithGroupName(group),
	}
	if cfg.RocketMQ.Namespace != "" {
		opts = append(opts, consumer.WithNamespace(cfg.RocketMQ.Namespace))
	}
	if cfg.RocketMQ.AccessKey != "" {
		opts = append(opts, consumer.WithCredentials(primitive.Credentials{
			AccessKey: cfg.RocketMQ.AccessKey,
			SecretKey: cfg.RocketMQ.SecretKey,
		}))
	}
	c, err := rocketmq.NewPushConsumer(opts...)
	if err != nil {
		logx.Errorf("[mq-rocketmq] create consumer error: %v", err)
		return &rocketmqConsumer{c: nil, topic: topic}
	}
	return &rocketmqConsumer{c: c, topic: topic}
}

func (r *rocketmqConsumer) Subscribe(ctx context.Context, topic string, handler Handler) error {
	t := topic
	if t == "" {
		t = r.topic
	}
	if t == "" {
		return ErrTopicEmpty
	}
	if r.c == nil {
		return ErrConsumerNotStarted
	}
	err := r.c.Subscribe(t, consumer.MessageSelector{}, func(ctx context.Context, msgs ...*primitive.MessageExt) (consumer.ConsumeResult, error) {
		for _, m := range msgs {
			keys := m.GetKeys()
			start := time.Now()
			hErr := handler(ctx, &Message{
				Key:       keys,
				Value:     m.Body,
				Partition: int32(m.Queue.QueueId),
				Offset:    m.CommitLogOffset,
			})
			metrics.MQConsumeDurationSeconds.WithLabelValues(t).Observe(time.Since(start).Seconds())
			if hErr != nil {
				metrics.MQConsumeTotal.WithLabelValues(t, "error").Inc()
				logx.WithContext(ctx).Errorf("[mq-rocketmq] handler error topic=%s: %v", t, hErr)
				return consumer.ConsumeRetryLater, hErr
			}
			metrics.MQConsumeTotal.WithLabelValues(t, "ok").Inc()
		}
		return consumer.ConsumeSuccess, nil
	})
	if err != nil {
		return err
	}
	return r.c.Start()
}

func (r *rocketmqConsumer) Close() error {
	if r.c == nil {
		return nil
	}
	return r.c.Shutdown()
}

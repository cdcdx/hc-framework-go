package mq

import (
	"context"
	"fmt"
	"strings"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/zeromicro/go-zero/core/logx"
)

// resolveRabbitMQURL 返回连接 URL（直接从 RabbitMQ.URL 读取）。
func resolveRabbitMQURL(cfg Config) string {
	if cfg.RabbitMQ.URL != "" {
		return cfg.RabbitMQ.URL
	}
	return "amqp://guest:guest@127.0.0.1:5672/"
}

func resolveRabbitMQExchangeType(cfg Config) string {
	t := cfg.RabbitMQ.ExchangeType
	if t == "" {
		return "topic"
	}
	return t
}

func resolveRabbitMQPrefetch(cfg Config) int {
	if cfg.RabbitMQ.Prefetch <= 0 {
		return 1
	}
	return cfg.RabbitMQ.Prefetch
}

// defaultRoutingKey 配置中的默认 routing key（发布/订阅都可能需要）。
func defaultRoutingKey(cfg Config) string {
	if cfg.RabbitMQ.RoutingKey != "" {
		return cfg.RabbitMQ.RoutingKey
	}
	return cfg.RabbitMQ.Topic
}

// rabbitmqProducer RabbitMQ 消息生产者。
//
// RabbitMQ 无内建 topic 概念，这里以 Exchange + RoutingKey 实现发布/订阅。
// topic 参数映射为 routing key，缺省时回落到配置中的默认 routing key。
type rabbitmqProducer struct {
	conn         *amqp.Connection
	channel      *amqp.Channel
	exchange     string
	exchangeType string
	defaultKey   string
}

func newRabbitMQProducer(cfg Config) *rabbitmqProducer {
	url := resolveRabbitMQURL(cfg)
	conn, err := amqp.Dial(url)
	if err != nil {
		logx.Errorf("[mq-rabbitmq] dial error: %v", err)
		return &rabbitmqProducer{}
	}
	ch, err := conn.Channel()
	if err != nil {
		logx.Errorf("[mq-rabbitmq] open channel error: %v", err)
		_ = conn.Close()
		return &rabbitmqProducer{}
	}
	ex := cfg.RabbitMQ.Exchange
	exType := resolveRabbitMQExchangeType(cfg)
	if ex != "" {
		if err := ch.ExchangeDeclare(ex, exType, true, false, false, false, nil); err != nil {
			logx.Errorf("[mq-rabbitmq] declare exchange error: %v", err)
		}
	}
	dk := cfg.RabbitMQ.RoutingKey
	if dk == "" {
		dk = cfg.RabbitMQ.Topic
	}
	return &rabbitmqProducer{
		conn:         conn,
		channel:      ch,
		exchange:     ex,
		exchangeType: exType,
		defaultKey:   dk,
	}
}

func (r *rabbitmqProducer) publish(ctx context.Context, topic string, key string, value []byte) error {
	if r.channel == nil {
		return ErrProducerNotStarted
	}
	routingKey := topic
	if routingKey == "" {
		routingKey = r.defaultKey
	}
	if routingKey == "" {
		return ErrTopicEmpty
	}
	return r.channel.PublishWithContext(
		ctx,
		r.exchange,
		routingKey,
		false, // mandatory
		false, // immediate
		amqp.Publishing{
			ContentType: "application/octet-stream",
			Body:        value,
			MessageId:   key,
		},
	)
}

func (r *rabbitmqProducer) Send(ctx context.Context, topic, key string, value []byte) error {
	return r.publish(ctx, topic, key, value)
}

func (r *rabbitmqProducer) SendAsync(ctx context.Context, topic, key string, value []byte) error {
	go func() {
		// 保留上游 ctx 用于日志关联，但用独立的 Background 避免 ctx 取消后
		// goroutine 被提前终止导致消息丢失。
		if err := r.publish(context.Background(), topic, key, value); err != nil {
			logx.WithContext(ctx).Errorf("[mq-rabbitmq] async publish error: %v", err)
		}
	}()
	return nil
}

func (r *rabbitmqProducer) Close() error {
	if r.channel != nil {
		_ = r.channel.Close()
	}
	if r.conn != nil {
		return r.conn.Close()
	}
	return nil
}

// rabbitmqConsumer RabbitMQ 消息消费者。
type rabbitmqConsumer struct {
	conn     *amqp.Connection
	channel  *amqp.Channel
	queue    string
	exchange string
}

func newRabbitMQConsumer(cfg Config) *rabbitmqConsumer {
	url := resolveRabbitMQURL(cfg)
	conn, err := amqp.Dial(url)
	if err != nil {
		logx.Errorf("[mq-rabbitmq] dial error: %v", err)
		return &rabbitmqConsumer{}
	}
	ch, err := conn.Channel()
	if err != nil {
		logx.Errorf("[mq-rabbitmq] open channel error: %v", err)
		_ = conn.Close()
		return &rabbitmqConsumer{}
	}
	if err := ch.Qos(resolveRabbitMQPrefetch(cfg), 0, false); err != nil {
		logx.Errorf("[mq-rabbitmq] set qos error: %v", err)
	}
	ex := cfg.RabbitMQ.Exchange
	if ex != "" {
		if err := ch.ExchangeDeclare(ex, resolveRabbitMQExchangeType(cfg), true, false, false, false, nil); err != nil {
			logx.Errorf("[mq-rabbitmq] declare exchange error: %v", err)
		}
	}
	return &rabbitmqConsumer{conn: conn, channel: ch, queue: cfg.RabbitMQ.Queue, exchange: ex}
}

func (r *rabbitmqConsumer) Subscribe(ctx context.Context, topic string, handler Handler) error {
	if r.channel == nil {
		return ErrConsumerNotStarted
	}
	queue := r.queue
	if queue == "" {
		queue = fmt.Sprintf("hc-%s", strings.ReplaceAll(topic, ".", "-"))
	}
	q, err := r.channel.QueueDeclare(queue, true, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("[mq-rabbitmq] declare queue error: %w", err)
	}
	// 若配置了 exchange，则把队列绑定到该 exchange 的 topic routing key。
	if r.exchange != "" {
		if err := r.channel.QueueBind(q.Name, topic, r.exchange, false, nil); err != nil {
			return fmt.Errorf("[mq-rabbitmq] bind queue error: %w", err)
		}
	}
	deliveries, err := r.channel.Consume(q.Name, "hc-rabbitmq-consumer", true, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("[mq-rabbitmq] consume error: %w", err)
	}
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case d, ok := <-deliveries:
				if !ok {
					return
				}
				if err := handler(ctx, &Message{
					Key:   d.MessageId,
					Value: d.Body,
				}); err != nil {
					logx.WithContext(ctx).Errorf("[mq-rabbitmq] handler error topic=%s: %v", topic, err)
				}
			}
		}
	}()
	return nil
}

func (r *rabbitmqConsumer) Close() error {
	if r.channel != nil {
		_ = r.channel.Close()
	}
	if r.conn != nil {
		return r.conn.Close()
	}
	return nil
}

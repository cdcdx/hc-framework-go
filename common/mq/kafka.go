package mq

import (
	"context"

	"github.com/segmentio/kafka-go"
	"github.com/zeromicro/go-zero/core/logx"
)

// kafkaProducer Kafka 消息生产者。
type kafkaProducer struct {
	writer *kafka.Writer
}

func newKafkaProducer(cfg Config) *kafkaProducer {
	w := &kafka.Writer{
		Addr:     kafka.TCP(cfg.Brokers...),
		Balancer: &kafka.LeastBytes{},
	}
	return &kafkaProducer{writer: w}
}

func (p *kafkaProducer) Send(ctx context.Context, topic, key string, value []byte) error {
	return p.writer.WriteMessages(ctx, kafka.Message{
		Topic: topic,
		Key:   []byte(key),
		Value: value,
	})
}

func (p *kafkaProducer) SendAsync(ctx context.Context, topic, key string, value []byte) error {
	go func() {
		if err := p.writer.WriteMessages(context.Background(), kafka.Message{
			Topic: topic,
			Key:   []byte(key),
			Value: value,
		}); err != nil {
			logx.WithContext(ctx).Errorf("[mq-kafka] async send error: %v", err)
		}
	}()
	return nil
}

func (p *kafkaProducer) Close() error {
	return p.writer.Close()
}

// kafkaConsumer Kafka 消息消费者。
type kafkaConsumer struct {
	reader *kafka.Reader
	done   chan struct{}
}

func newKafkaConsumer(cfg Config) *kafkaConsumer {
	group := cfg.ConsumerGroup
	if group == "" {
		group = "hc-framework"
	}
	return &kafkaConsumer{
		reader: kafka.NewReader(kafka.ReaderConfig{
			Brokers: cfg.Brokers,
			GroupID: group,
		}),
		done: make(chan struct{}),
	}
}

func (c *kafkaConsumer) Subscribe(ctx context.Context, topic string, handler Handler) error {
	_ = c.reader.SetOffset(kafka.LastOffset)
	go func() {
		defer c.reader.Close()
		for {
			select {
			case <-c.done:
				return
			case <-ctx.Done():
				return
			default:
			}
			msg, err := c.reader.ReadMessage(ctx)
			if err != nil {
				logx.WithContext(ctx).Errorf("[mq-kafka] read error: %v", err)
				continue
			}
			if err := handler(ctx, &Message{
				Key:       string(msg.Key),
				Value:     msg.Value,
				Partition: int32(msg.Partition),
				Offset:    msg.Offset,
			}); err != nil {
				logx.WithContext(ctx).Errorf("[mq-kafka] handler error topic=%s: %v", topic, err)
			}
		}
	}()
	return nil
}

func (c *kafkaConsumer) Close() error {
	close(c.done)
	return c.reader.Close()
}

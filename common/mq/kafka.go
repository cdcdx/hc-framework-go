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
	kc := cfg.Kafka
	brokers := kc.Brokers
	if len(brokers) == 0 {
		brokers = []string{"127.0.0.1:9092"}
	}
	acks := kc.RequiredAcks
	if acks < -1 || acks > 1 {
		acks = 1
	}
	batchSize := kc.BatchSize
	if batchSize <= 0 {
		batchSize = 100
	}
	batchBytes := kc.BatchBytes
	if batchBytes <= 0 {
		batchBytes = 1_048_576
	}
	w := &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Balancer:     &kafka.LeastBytes{},
		RequiredAcks: kafka.RequiredAcks(acks),
		BatchSize:    batchSize,
		BatchBytes:   int64(batchBytes),
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
	kc := cfg.Kafka
	brokers := kc.Brokers
	if len(brokers) == 0 {
		brokers = []string{"127.0.0.1:9092"}
	}
	group := kc.ConsumerGroup
	if group == "" {
		group = "hc-framework"
	}
	return &kafkaConsumer{
		reader: kafka.NewReader(kafka.ReaderConfig{
			Brokers: brokers,
			GroupID: group,
		}),
		done: make(chan struct{}),
	}
}

func (c *kafkaConsumer) Subscribe(ctx context.Context, topic string, handler Handler) error {
	if topic == "" {
		return ErrTopicEmpty
	}
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
			msg, err := c.reader.FetchMessage(ctx)
			if err != nil {
				if err.Error() == "EOF" || err.Error() == "context canceled" {
					return
				}
				logx.WithContext(ctx).Errorf("[mq-kafka] fetch error: %v", err)
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
			if err := c.reader.CommitMessages(ctx, msg); err != nil {
				logx.WithContext(ctx).Errorf("[mq-kafka] commit error: %v", err)
			}
		}
	}()
	return nil
}

func (c *kafkaConsumer) Close() error {
	close(c.done)
	return c.reader.Close()
}

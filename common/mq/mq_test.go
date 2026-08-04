package mq

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestMemoryQueue_ProducerConsumer(t *testing.T) {
	cfg := Config{Enabled: true, Type: "memory", BufferSize: 64}
	prod := NewProducer(cfg)
	cons := NewConsumer(cfg)
	defer prod.Close()
	defer cons.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var mu sync.Mutex
	received := make(map[string]bool)

	_ = cons.Subscribe(ctx, "test-topic", func(ctx context.Context, msg *Message) error {
		mu.Lock()
		received[string(msg.Value)] = true
		mu.Unlock()
		return nil
	})

	// 发送几条消息
	for _, v := range []string{"a", "b", "c"} {
		if err := prod.Send(ctx, "test-topic", v, []byte(v)); err != nil {
			t.Fatalf("send %s: %v", v, err)
		}
	}

	// 等待消费
	time.Sleep(200 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	for _, v := range []string{"a", "b", "c"} {
		if !received[v] {
			t.Errorf("message %q not received", v)
		}
	}
}

func TestMemoryQueue_AsyncSend(t *testing.T) {
	cfg := Config{Enabled: true, Type: "memory", BufferSize: 64}
	prod := NewProducer(cfg)
	cons := NewConsumer(cfg)
	defer prod.Close()
	defer cons.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var mu sync.Mutex
	received := make(map[string]bool)
	_ = cons.Subscribe(ctx, "async-topic", func(ctx context.Context, msg *Message) error {
		mu.Lock()
		received[string(msg.Value)] = true
		mu.Unlock()
		return nil
	})

	if err := prod.SendAsync(ctx, "async-topic", "k", []byte("v")); err != nil {
		t.Fatal(err)
	}
	time.Sleep(200 * time.Millisecond)

	mu.Lock()
	if !received["v"] {
		t.Error("async message not received")
	}
	mu.Unlock()
}

func TestNoopQueue(t *testing.T) {
	cfg := Config{Enabled: false}
	prod := NewProducer(cfg)
	cons := NewConsumer(cfg)
	ctx := context.Background()

	if err := prod.Send(ctx, "t", "k", []byte("v")); err != nil {
		t.Fatal(err)
	}
	if err := prod.SendAsync(ctx, "t", "k", []byte("v")); err != nil {
		t.Fatal(err)
	}
	if err := cons.Subscribe(ctx, "t", func(ctx context.Context, msg *Message) error { return nil }); err != nil {
		t.Fatal(err)
	}
	prod.Close()
	cons.Close()
}

// TestGormxDrivers 移至 common/gormx 包内测试

package mq

import (
	"reflect"
	"testing"

	"github.com/cdcdx/hc-framework-go/internal/config"
)

// partitionTestConsumer 构造仅用于白盒测试的消费者：不连接 Broker，使用 nopLogger。
func partitionTestConsumer(id, count int) *KafkaConsumer {
	return &KafkaConsumer{
		cfg: &config.KafkaConfig{Consumer: config.KafkaConsumerConfig{InstanceID: id, InstanceCount: count}},
		log: nopLogger{},
	}
}

// TestAssignPartitions_SingleInstanceNoOp 单实例（instance_count<=1）不裁剪，全量消费。
func TestAssignPartitions_SingleInstanceNoOp(t *testing.T) {
	parts := []int{0, 1, 2, 3, 4, 5}
	if got := partitionTestConsumer(0, 0).assignPartitions(parts); !reflect.DeepEqual(got, parts) {
		t.Fatalf("instance_count=0 should keep all partitions, got %v", got)
	}
	if got := partitionTestConsumer(0, 1).assignPartitions(parts); !reflect.DeepEqual(got, parts) {
		t.Fatalf("instance_count=1 should keep all partitions, got %v", got)
	}
}

// TestAssignPartitions_MultiInstance 多实例静态分区分配：子集互斥且并集覆盖全部。
func TestAssignPartitions_MultiInstance(t *testing.T) {
	parts := []int{0, 1, 2, 3, 4, 5, 6, 7}

	var got [][]int
	for id := 0; id < 2; id++ {
		got = append(got, partitionTestConsumer(id, 2).assignPartitions(parts))
	}
	want0 := []int{0, 2, 4, 6}
	want1 := []int{1, 3, 5, 7}
	if !reflect.DeepEqual(got[0], want0) {
		t.Fatalf("instance 0 want %v, got %v", want0, got[0])
	}
	if !reflect.DeepEqual(got[1], want1) {
		t.Fatalf("instance 1 want %v, got %v", want1, got[1])
	}

	// 并集覆盖全部、无重叠
	seen := map[int]bool{}
	for _, g := range got {
		for _, p := range g {
			if seen[p] {
				t.Fatalf("partition %d assigned to multiple instances (overlap)", p)
			}
			seen[p] = true
		}
	}
	if len(seen) != len(parts) {
		t.Fatalf("union of assigned partitions should cover all %d, got %d", len(parts), len(seen))
	}
}

// TestAssignPartitions_Count3 三实例分配：0/1/2 各取模子集，并集覆盖。
func TestAssignPartitions_Count3(t *testing.T) {
	parts := []int{0, 1, 2, 3, 4, 5}
	want := [][]int{{0, 3}, {1, 4}, {2, 5}}
	seen := map[int]bool{}
	for id := 0; id < 3; id++ {
		got := partitionTestConsumer(id, 3).assignPartitions(parts)
		if !reflect.DeepEqual(got, want[id]) {
			t.Fatalf("instance %d want %v, got %v", id, want[id], got)
		}
		for _, p := range got {
			if seen[p] {
				t.Fatalf("partition %d overlap across instances", p)
			}
			seen[p] = true
		}
	}
	if len(seen) != len(parts) {
		t.Fatalf("union should cover all partitions, got %d", len(seen))
	}
}

// TestAssignPartitions_OutOfRange 越界 instance_id 退化为全量消费（避免静默不消费）。
func TestAssignPartitions_OutOfRange(t *testing.T) {
	parts := []int{0, 1, 2, 3}
	// instance_id >= instance_count
	if got := partitionTestConsumer(2, 2).assignPartitions(parts); !reflect.DeepEqual(got, parts) {
		t.Fatalf("out-of-range id should fall back to all partitions, got %v", got)
	}
	// instance_id < 0
	if got := partitionTestConsumer(-1, 2).assignPartitions(parts); !reflect.DeepEqual(got, parts) {
		t.Fatalf("negative id should fall back to all partitions, got %v", got)
	}
}

// TestInstanceOffsetPath 多实例 offset 文件路径按实例区分（扩展名前加 .<id>）。
func TestInstanceOffsetPath(t *testing.T) {
	if got := partitionTestConsumer(0, 1).instanceOffsetPath("./data/kafka-offsets.json"); got != "./data/kafka-offsets.json" {
		t.Fatalf("single instance should not change path, got %q", got)
	}

	multi := partitionTestConsumer(2, 3)
	if got := multi.instanceOffsetPath("./data/kafka-offsets.json"); got != "data/kafka-offsets.2.json" {
		t.Fatalf("multi instance path want data/kafka-offsets.2.json, got %q", got)
	}
	if got := multi.instanceOffsetPath("/var/lib/off/kafka-offsets"); got != "/var/lib/off/kafka-offsets.2" {
		t.Fatalf("multi instance path (no ext) want /var/lib/off/kafka-offsets.2, got %q", got)
	}
}

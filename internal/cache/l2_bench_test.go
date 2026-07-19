package cache

import (
	"bytes"
	"testing"
)

// BenchmarkNullMarker_StringConv 复现优化前 L2.Get / GetMulti 的空值标记检测：
// 每次命中都把整段 value 字节拷贝成 string 再与 8 字节哨兵比较。value 越大，
// 这个不必要的堆分配越可观（典型缓存 JSON 负载在 KB 级）。
func BenchmarkNullMarker_StringConv(b *testing.B) {
	data := bytes.Repeat([]byte("x"), 4096) // 模拟约 4KB 的缓存 JSON 负载
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = string(data) == NullMarker
	}
}

// BenchmarkNullMarker_BytesEqual 优化后的零分配比较：长度不等（value 远大于
// 哨兵）时 bytes.Equal 直接短路返回，不再分配整段 value 拷贝。
func BenchmarkNullMarker_BytesEqual(b *testing.B) {
	data := bytes.Repeat([]byte("x"), 4096)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = bytes.Equal(data, nullMarkerBytes)
	}
}

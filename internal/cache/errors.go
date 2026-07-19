package cache

import "errors"

var (
	errNotProtoMessage = errors.New("cache: value does not implement proto.Message")

	// NullMarker 空值标记（防缓存穿透）
	NullMarker = "__NULL__"

	// nullMarkerBytes 是 NullMarker 的字节形式，用于命中判断时避免
	// `string(data) == NullMarker` 把整段 value 字节拷贝成 string 造成的不必要分配
	// （value 可达数 KB，而哨兵仅 8 字节，长度不等本应直接短路）。
	nullMarkerBytes = []byte(NullMarker)
)

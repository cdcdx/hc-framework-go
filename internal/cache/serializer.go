package cache

import (
	jsoniter "github.com/json-iterator/go"

	"google.golang.org/protobuf/proto"
)

// json 兼容 encoding/json 的高性能实现（jsoniter），用于 L2 缓存序列化热路径。
// ConfigCompatibleWithStandardLibrary 保持与标准库一致的 EscapeHTML/SortMapKeys 行为，
// 避免滚动发布期间新旧版本序列化字节不一致。
var json = jsoniter.ConfigCompatibleWithStandardLibrary

// JSONSerializer JSON 序列化器
type JSONSerializer struct{}

func (s *JSONSerializer) Marshal(v interface{}) ([]byte, error) {
	return json.Marshal(v)
}

func (s *JSONSerializer) Unmarshal(data []byte, v interface{}) error {
	return json.Unmarshal(data, v)
}

// ProtoSerializer Protobuf 序列化器（仅支持 proto.Message）
type ProtoSerializer struct{}

func (s *ProtoSerializer) Marshal(v interface{}) ([]byte, error) {
	msg, ok := v.(proto.Message)
	if !ok {
		return nil, errNotProtoMessage
	}
	return proto.Marshal(msg)
}

func (s *ProtoSerializer) Unmarshal(data []byte, v interface{}) error {
	msg, ok := v.(proto.Message)
	if !ok {
		return errNotProtoMessage
	}
	return proto.Unmarshal(data, msg)
}

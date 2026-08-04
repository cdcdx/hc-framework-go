package logic

import (
	"context"
	"encoding/json"

	"github.com/cdcdx/hc-framework-go/common/jwt"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// toData 把 rpc 返回结构转为 snake_case map（protojson UseProtoNames），
// 保持与 gin 版 API 契约字段名一致（user_id / access_token 等）。
func toData(v proto.Message) any {
	if v == nil {
		return nil
	}
	raw, err := protojson.MarshalOptions{UseProtoNames: true}.Marshal(v)
	if err != nil {
		return v
	}
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		return v
	}
	return m
}

// toDataList 把 rpc 列表转为 snake_case map 列表
func toDataList[T proto.Message](list []T) []any {
	out := make([]any, 0, len(list))
	for _, it := range list {
		out = append(out, toData(it))
	}
	return out
}

// requireUser 从 request context 提取已鉴权 user_id
func requireUser(ctx context.Context) (string, bool) {
	return jwt.UserIDFromContext(ctx)
}

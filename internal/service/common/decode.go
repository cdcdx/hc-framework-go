package common

import (
	"encoding/json"
	"fmt"
)

// DecodePayload 将事件 Payload（JSON 反序列化后通常是 map）转换为具体结构。
// 原位于 task 子包，因 points_outbox 需使用而 common 不应反向依赖 task，故上移到 common。
func DecodePayload(payload any, out any) error {
	raw, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal payload: %w", err)
	}
	if err := json.Unmarshal(raw, out); err != nil {
		return fmt.Errorf("unmarshal payload: %w", err)
	}
	return nil
}

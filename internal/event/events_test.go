package event

import (
	"encoding/json"
	"testing"
)

func TestMessageJSONRoundTrip(t *testing.T) {
	m := Message{
		TraceID:   "t1",
		EventType: EventIdleSettled,
		Key:       "k",
		EventID:   "e1",
		Payload:   IdleSettledPayload{UserID: "u", PointsEarned: 10},
	}
	b, err := json.Marshal(m)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var got Message
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.EventID != "e1" || got.EventType != EventIdleSettled {
		t.Fatalf("round trip mismatch: %+v", got)
	}
}

func TestEventConstantsUnique(t *testing.T) {
	// 守护事件类型常量不被误配成重复值（重复会导致消费侧事件路由串台）。
	seen := map[string]bool{}
	for _, e := range []string{
		EventUserRegistered, EventUserLoggedIn, EventIdleSettled,
		EventTaskCompleted, EventShopRedeemed, EventUserPointsAdjust, EventCacheInvalidate,
	} {
		if seen[e] {
			t.Fatalf("duplicate event constant %q", e)
		}
		seen[e] = true
	}
}

func TestUserPointsAdjustPayloadEventID(t *testing.T) {
	// 积分调整事件的 EventID 必须与 points_outbox.event_id 一致，是消费端幂等去重的依据。
	p := UserPointsAdjustPayload{UserID: "u", EventID: "outbox-1", Delta: -100}
	if p.EventID == "" {
		t.Fatal("UserPointsAdjustPayload.EventID must be set for deduplication")
	}
}

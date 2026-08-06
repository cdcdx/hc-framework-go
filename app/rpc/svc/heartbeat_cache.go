package svc

import (
	"context"
	"strconv"
	"time"

	"github.com/cdcdx/hc-framework-go/common/model"
)

// idleHeartbeatKey 心跳续期 key（Redis）。存某设备最后心跳的 unix 秒，
// TTL = 超时阈值 + 余量。有 Redis 时心跳只写该 key 不落库，避免 idle 压测
// 下高频心跳打满 business 库连接池导致的慢 SQL。
func idleHeartbeatKey(deviceID string) string {
	return "idle:hb:" + deviceID
}

// LastHeartbeat 读取某设备最后心跳时间。
// 优先读 Redis 心跳续期 key；Redis 不可用或 miss 时回退到 DB 的 last_heartbeat_at 字段。
func (svc *ServiceContext) LastHeartbeat(ctx context.Context, deviceID string, dbVal *time.Time) *time.Time {
	if svc.Cache != nil {
		if v, err := svc.Cache.Get(ctx, idleHeartbeatKey(deviceID)); err == nil && v != nil {
			if sec, perr := strconv.ParseInt(string(v), 10, 64); perr == nil {
				t := time.Unix(sec, 0)
				return &t
			}
		}
	}
	return dbVal
}

// TouchHeartbeat 续期心跳：有 Redis 则只写 Redis（不落库），否则回退写 DB last_heartbeat_at。
func (svc *ServiceContext) TouchHeartbeat(ctx context.Context, rec *model.IdleRecord, deviceID string, now time.Time) error {
	if svc.Cache != nil {
		ttl := time.Duration(svc.Config.Idle.TimeoutMinutes+1) * time.Minute
		if ttl <= 0 {
			ttl = 6 * time.Minute
		}
		if err := svc.Cache.Set(ctx, idleHeartbeatKey(deviceID), []byte(strconv.FormatInt(now.Unix(), 10)), ttl); err == nil {
			return nil
		}
	}
	// 兜底：Redis 不可用时直接落库。
	return svc.Db.Model(rec).UpdateColumn("last_heartbeat_at", now).Error
}

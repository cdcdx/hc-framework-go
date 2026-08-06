package svc

import (
	"context"
	"sync"
	"time"

	"github.com/cdcdx/hc-framework-go/common/metrics"
	"github.com/cdcdx/hc-framework-go/common/model"
	"github.com/zeromicro/go-zero/core/logx"
)

// startIdleScan 后台 goroutine，定期扫描超时未心跳的活跃挂机记录并结算。
// 每 scanInterval 执行一次。方案 A 下心跳只写 Redis（idle:hb:{device_id}），
// DB 的 last_heartbeat_at 仅作为粗筛与 Redis 不可用时的回退判定源。
// stop 通道关闭或 ticker 触发时退出；退出前调用 wg.Done() 以便 ServiceContext.Close 等待。
func startIdleScan(svcCtx *ServiceContext, stop <-chan struct{}, wg *sync.WaitGroup) {
	defer wg.Done()

	interval := svcCtx.Config.Idle.ScanInterval
	if interval <= 0 {
		interval = 30 * time.Second
	}
	timeoutMin := svcCtx.Config.Idle.TimeoutMinutes
	if timeoutMin <= 0 {
		timeoutMin = 5
	}
	timeout := time.Duration(timeoutMin) * time.Minute

	logx.Infof("[idle-scan] background scanner started (interval=%v, timeout=%v)", interval, timeout)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-stop:
			logx.Infof("[idle-scan] background scanner stopped")
			return
		case <-ticker.C:
			scanAndSettleTimeout(svcCtx, timeout)
		}
	}
}

// scanAndSettleTimeout 单次扫描：查询 last_heartbeat_at < cutoff 的活跃记录作为候选，
// 逐条依据心跳时间源判定是否真正超时。
//
// 注意：方案 A 下心跳只写 Redis（idle:hb:{device_id}），不再更新 DB 的 last_heartbeat_at，
// 因此该字段会停留在 start 时刻的值，所有存活超过 timeout 的活跃记录都会被本查询捞起。
// 为避免误结算，循环内以 Redis 心跳时间为准：Redis 仍有有效心跳则跳过（keep-alive），
// 仅当 Redis key 缺失/过期且 DB 字段也过期时才结算为 timeout。
// 若 Redis 不可用（Cache==nil），LastHeartbeat 自动回退到 DB 字段，行为与原逻辑一致。
func scanAndSettleTimeout(svcCtx *ServiceContext, timeout time.Duration) {
	start := time.Now()
	defer func() {
		metrics.IdleScanDurationSeconds.Observe(time.Since(start).Seconds())
	}()

	cutoff := time.Now().Add(-timeout)
	ctx := context.Background()

	if svcCtx.Db == nil {
		return
	}

	var recs []model.IdleRecord
	if err := svcCtx.Db.WithContext(ctx).
		Where("status = ? AND last_heartbeat_at < ?", model.IdleStatusActive, cutoff).
		Order("id ASC").
		Limit(200).
		Find(&recs).Error; err != nil {
		logx.WithContext(ctx).Errorf("[idle-scan] query timeout records failed: %v", err)
		return
	}

	if len(recs) == 0 {
		return
	}

	logx.WithContext(ctx).Infof("[idle-scan] %d timeout candidates (pre-filter), checking heartbeat source...", len(recs))
	settled := 0
	keepAlive := 0
	for i := range recs {
		// 心跳时间源：优先 Redis 续期 key，缺失时回退 DB 字段。
		hb := svcCtx.LastHeartbeat(ctx, recs[i].DeviceID, recs[i].LastHeartbeatAt)
		// Redis 仍有有效心跳（未真正超时）→ 保活，跳过结算。
		if hb != nil && time.Since(*hb) <= timeout {
			keepAlive++
			continue
		}
		// 真正超时：以心跳时间（Redis 或 DB）作为结算结束点。
		until := time.Now()
		if hb != nil {
			until = *hb
		}
		if _, err := settleRecord(ctx, svcCtx, &recs[i], until, model.IdleStatusTimeout); err != nil {
			logx.WithContext(ctx).Errorf("[idle-scan] settle record %d failed: %v", recs[i].ID, err)
			continue
		}
		settled++
	}
	logx.WithContext(ctx).Infof("[idle-scan] settled %d, keep-alive skipped %d, in %.2fs",
		settled, keepAlive, time.Since(start).Seconds())
}

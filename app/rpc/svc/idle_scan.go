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
// 每 scanInterval 执行一次，结算 last_heartbeat_at < now-timeout 的记录。
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

// scanAndSettleTimeout 单次扫描：查询 last_heartbeat_at < cutoff 的活跃记录，逐条结算。
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

	logx.WithContext(ctx).Infof("[idle-scan] found %d timeout records, settling...", len(recs))
	settled := 0
	for i := range recs {
		until := time.Now()
		if recs[i].LastHeartbeatAt != nil {
			until = *recs[i].LastHeartbeatAt
		}
		if _, err := settleRecord(ctx, svcCtx, &recs[i], until, model.IdleStatusTimeout); err != nil {
			logx.WithContext(ctx).Errorf("[idle-scan] settle record %d failed: %v", recs[i].ID, err)
			continue
		}
		settled++
	}
	logx.WithContext(ctx).Infof("[idle-scan] settled %d/%d records in %.2fs",
		settled, len(recs), time.Since(start).Seconds())
}

package idle

import (
	"context"
	"sync"
	"time"

	"github.com/cdcdx/hc-framework-go/internal/repository"
	"go.uber.org/zap"
)

// HeartbeatFlusher 每 Pod 本地心跳聚合器。
//
// 高频心跳（用户上报 / 启动 / 宽限重触）仅调用 Record 更新内存 map（O(1)、无 DB 写），
// 由后台 ticker 每 ~interval 调用一次 Flush，将聚合后的最新心跳时间批量写入 DB
// （单事务内批量 UPDATE last_heartbeat_at），从而把「逐次 UPDATE」替换为「定时批量 flush」，
// 将 DB 写压力从「每次心跳一次」降为「每 interval 一次」。
//
// 多 Pod 各自聚合、各自 flush：批量 UPDATE 仅按 user+device+active 定位本 Pod 负责的行；
// 已结算（status!=active）的会话因 WHERE 条件自动跳过。原地重启/发布时，Stop 会做最后一次 flush
// 排空内存，避免丢失未落库心跳（最坏丢失一个 interval 窗口内的近似心跳，对判活无实质影响）。
type HeartbeatFlusher struct {
	repo     *repository.IdleRepository
	mu       sync.Mutex
	pending  map[string]repository.HeartbeatPersistItem
	interval time.Duration
	stopCh   chan struct{}
	wg       sync.WaitGroup
	log      *zap.Logger
}

const hbFlusherSep = "\x1f"

// NewHeartbeatFlusher 创建聚合器。interval<=0 时回退为 10s。
func NewHeartbeatFlusher(repo *repository.IdleRepository, interval time.Duration, log *zap.Logger) *HeartbeatFlusher {
	if interval <= 0 {
		interval = 10 * time.Second
	}
	if log == nil {
		log = zap.NewNop()
	}
	return &HeartbeatFlusher{
		repo:     repo,
		pending:  make(map[string]repository.HeartbeatPersistItem),
		interval: interval,
		stopCh:   make(chan struct{}),
		log:      log,
	}
}

// Record 聚合某会话的最新心跳时间（仅更新内存 map，无 DB 写；同 user+device 后者覆盖前者）。
func (f *HeartbeatFlusher) Record(userID, deviceID string, ts time.Time) {
	if f == nil || userID == "" {
		return
	}
	key := userID + hbFlusherSep + deviceID
	f.mu.Lock()
	f.pending[key] = repository.HeartbeatPersistItem{UserID: userID, DeviceID: deviceID, TS: ts}
	f.mu.Unlock()
}

// Flush 将聚合后的心跳时间批量落库，并清空 pending。返回实际更新的行数。
func (f *HeartbeatFlusher) Flush(ctx context.Context) (int, error) {
	items := f.drain()
	if len(items) == 0 {
		return 0, nil
	}
	return f.repo.BatchUpdateHeartbeat(ctx, items)
}

func (f *HeartbeatFlusher) drain() []repository.HeartbeatPersistItem {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.pending) == 0 {
		return nil
	}
	items := make([]repository.HeartbeatPersistItem, 0, len(f.pending))
	for _, v := range f.pending {
		items = append(items, v)
	}
	f.pending = make(map[string]repository.HeartbeatPersistItem)
	return items
}

// Start 启动定时 flush（每 interval 一次）。由 ctx 取消或 Stop 停止。nil flusher 为 no-op。
func (f *HeartbeatFlusher) Start(ctx context.Context) {
	if f == nil {
		return
	}
	f.wg.Add(1)
	go func() {
		defer f.wg.Done()
		ticker := time.NewTicker(f.interval)
		defer ticker.Stop()
		for {
			select {
			case <-f.stopCh:
				return
			case <-ctx.Done():
				return
			case <-ticker.C:
				if _, err := f.Flush(ctx); err != nil {
					f.log.Warn("heartbeat flush failed", zap.Error(err))
				}
			}
		}
	}()
}

// Stop 停止定时 flush 并做最后一次 flush（排空内存，避免丢失未落库心跳），随后等待 goroutine 退出。
func (f *HeartbeatFlusher) Stop(ctx context.Context) {
	if f == nil {
		return
	}
	select {
	case <-f.stopCh:
	default:
		close(f.stopCh)
	}
	if _, err := f.Flush(ctx); err != nil {
		f.log.Warn("heartbeat final flush failed", zap.Error(err))
	}
	f.wg.Wait()
}

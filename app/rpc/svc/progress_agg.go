package svc

import (
	"context"
	"sync"
	"time"

	"github.com/cdcdx/hc-framework-go/common/model"
	"github.com/zeromicro/go-zero/core/logx"
	"gorm.io/gorm"
)

// progressAggKey 聚合 key：用户 + 任务（同一用户同任务的进度增量合并）。
type progressAggKey struct {
	UserID  string
	TaskKey string
}

// progressAggregator 任务进度事件聚合器。
//
// 消费端（handleTaskProgress）收到 task_progress 事件后仅做内存累加，
// 由后台 ticker 定时或累积到阈值时 flush 为单事务批量 DB 写入。
// 这样将「每次兑换 3×(查Task+查/写Progress)」的高频随机写，
// 合并为低频批量更新，显著降低 DB 压力与事务开销。
type progressAggregator struct {
	svcCtx       *ServiceContext
	mu           sync.Mutex
	buf          map[progressAggKey]int64 // (user,task) -> 累加 delta
	batch        int
	flushInterval time.Duration
	stop         chan struct{}
	wg           sync.WaitGroup
}

func newProgressAggregator(svcCtx *ServiceContext) *progressAggregator {
	return &progressAggregator{
		svcCtx:       svcCtx,
		buf:          make(map[progressAggKey]int64),
		batch:        100,
		flushInterval: 200 * time.Millisecond,
		stop:         make(chan struct{}),
	}
}

// start 启动后台 flush goroutine。
func (a *progressAggregator) start() {
	a.wg.Add(1)
	go func() {
		defer a.wg.Done()
		ticker := time.NewTicker(a.flushInterval)
		defer ticker.Stop()
		for {
			select {
			case <-a.stop:
				a.flush() // 退出前冲刷剩余缓冲
				return
			case <-ticker.C:
				a.flush()
			}
		}
	}()
}

// stopAndDrain 停止并等待剩余缓冲冲刷完成。
func (a *progressAggregator) stopAndDrain() {
	close(a.stop)
	a.wg.Wait()
}

// add 累加一条进度事件；达到阈值时同步触发一次 flush。
func (a *progressAggregator) add(userID, taskKey string, delta int64) {
	if delta <= 0 {
		return
	}
	a.mu.Lock()
	a.buf[progressAggKey{UserID: userID, TaskKey: taskKey}] += delta
	overflow := len(a.buf) >= a.batch
	a.mu.Unlock()
	if overflow {
		a.flush()
	}
}

// Add 导出方法，供 logic 层（MQ 未启用时）直接投递事件到聚合器。
func (a *progressAggregator) Add(userID, taskKey string, delta int64) {
	a.add(userID, taskKey, delta)
}

// flush 将缓冲的增量批量写入 DB（单事务）。
func (a *progressAggregator) flush() {
	a.mu.Lock()
	if len(a.buf) == 0 {
		a.mu.Unlock()
		return
	}
	items := make([]progressAggKey, 0, len(a.buf))
	deltas := make(map[progressAggKey]int64, len(a.buf))
	for k, v := range a.buf {
		items = append(items, k)
		deltas[k] = v
	}
	a.buf = make(map[progressAggKey]int64)
	a.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	// 批量查 task 信息（task_key -> id + target），避免逐条查询。
	keys := make([]string, 0, len(items))
	for _, it := range items {
		keys = append(keys, it.TaskKey)
	}
	var tasks []model.Task
	if err := a.svcCtx.Db.WithContext(ctx).Where("task_key IN ?", keys).Find(&tasks).Error; err != nil {
		logx.Errorf("[progress-agg] load tasks failed: %v", err)
		return
	}
	taskMap := make(map[string]*model.Task, len(tasks))
	for i := range tasks {
		taskMap[tasks[i].TaskKey] = &tasks[i]
	}

	err := a.svcCtx.Db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		now := time.Now()
		for _, it := range items {
			task, ok := taskMap[it.TaskKey]
			if !ok {
				continue
			}
			period := periodOfTask(task, now)
			delta := deltas[it]
			// 原子累加当前进度；不存在则插入新行。
			res := tx.Model(&model.UserTaskProgress{}).
				Where("user_id = ? AND task_id = ? AND period = ?", it.UserID, task.ID, period).
				UpdateColumn("current_progress", gorm.Expr("current_progress + ?", delta))
			if res.Error != nil {
				return res.Error
			}
			if res.RowsAffected == 0 {
				pro := &model.UserTaskProgress{
					UserID:          it.UserID,
					TaskID:          task.ID,
					Period:          period,
					CurrentProgress: int(delta),
					IsCompleted:     int(delta) >= task.TargetValue,
				}
				if err := tx.Create(pro).Error; err != nil {
					return err
				}
				continue
			}
			// 已存在：进度更新后再判定是否达成完成（用更新后的值）。
			if err := tx.Model(&model.UserTaskProgress{}).
				Where("user_id = ? AND task_id = ? AND period = ? AND is_completed = ?", it.UserID, task.ID, period, false).
				Where("current_progress >= ?", task.TargetValue).
				UpdateColumn("is_completed", true).Error; err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		// 失败不重投（避免无限重试）；生产可接入 DLQ。
		logx.Errorf("[progress-agg] flush failed: %v", err)
	}
}

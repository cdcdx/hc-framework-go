package svc

import (
	"context"
	"encoding/json"
	"time"

	"github.com/cdcdx/hc-framework-go/common/errorx"
	"github.com/cdcdx/hc-framework-go/common/model"
	"github.com/zeromicro/go-zero/core/logx"
	"gorm.io/gorm"
)

// SettleRecord 结算一条活跃挂机记录。logic 层通过此导出函数调用，避免 svc ↔ logic 循环引用。
// 返回结算的积分数量。
func SettleRecord(ctx context.Context, svcCtx *ServiceContext, rec *model.IdleRecord, until time.Time, status string) (int64, error) {
	return settleRecord(ctx, svcCtx, rec, until, status)
}

// AccumulateDaily 按 (user_id, day) 累加今日挂机积分，超过每日上限返回 CodeDailyPointsLimit。
// 导出供测试直接调用。
func AccumulateDaily(ctx context.Context, svcCtx *ServiceContext, userID, day string, points int64) error {
	return accumulateDaily(ctx, svcCtx, userID, day, points)
}

// settleRecord 结算一条活跃挂机记录（合并为单 rpc 后为进程内调用）：
//  1. 按 [StartTime, until] 分钟数 × PointsPerMinute 计算积分；
//  2. 每日上限（IdleDailyPoints 表累加）拦截；
//  3. 进程内调用 AddPoints 入账（user 域）；
//  4. 上报挂机任务进度（daily_idle_30 / weekly_idle_300，delta=分钟数）；
//  5. 落库更新记录状态/时长/积分。
func settleRecord(ctx context.Context, svcCtx *ServiceContext, rec *model.IdleRecord, until time.Time, status string) (int64, error) {
	durSec := int(until.Sub(rec.StartTime).Seconds())
	if durSec < 0 {
		durSec = 0
	}
	points := int64(durSec/60) * int64(svcCtx.Config.Idle.PointsPerMinute)
	if points < 0 {
		points = 0
	}

	if points > 0 {
		// 每日上限校验（先于入账，确保上限不通过时不产生积分）
		day := until.Format("2006-01-02")
		if err := accumulateDaily(ctx, svcCtx, rec.UserID, day, points); err != nil {
			return 0, err
		}
		// 积分入账（通过 logic 层的 AddPoints 函数）
		if err := addPoints(ctx, svcCtx, rec.UserID, points, "idle"); err != nil {
			return 0, err
		}
		// 挂机任务进度上报（失败不阻塞结算）
		delta := int32(durSec / 60)
		if delta > 0 {
			reportIdleProgress(ctx, svcCtx, rec.UserID, delta)
		}
	}

	updates := map[string]any{
		"status":            status,
		"end_time":          until,
		"duration_seconds":  durSec,
		"points_earned":     points,
		"last_heartbeat_at": until,
	}
	if err := svcCtx.Db.Model(rec).Updates(updates).Error; err != nil {
		return 0, errorx.New(errorx.CodeDBError, err.Error())
	}

	// 发布结算事件（MQ 禁用时为 no-op），异步通知 task/log 等下游域
	publishSettleEvent(ctx, svcCtx, rec, points, status)

	return points, nil
}

// publishSettleEvent 发布挂机结算事件到 MQ。
// 事件体为 JSON，含 user_id / points / status / duration 等关键字段。
func publishSettleEvent(ctx context.Context, svcCtx *ServiceContext, rec *model.IdleRecord, points int64, status string) {
	if svcCtx.MQProducer == nil {
		return
	}
	body, err := json.Marshal(map[string]any{
		"user_id":      rec.UserID,
		"device_id":    rec.DeviceID,
		"points":       points,
		"status":       status,
		"duration_sec": rec.DurationSeconds,
	})
	if err != nil {
		logx.WithContext(ctx).Errorf("[settle] marshal event failed: %v", err)
		return
	}
	_ = svcCtx.MQProducer.SendAsync(ctx, "hc.idle.settled", rec.UserID, body)
}

// accumulateDaily 按 (user_id, day) 累加今日挂机积分，超过每日上限返回 CodeDailyPointsLimit。
// 该函数在 addPoints 入账之前调用，确保"上限校验不通过"时不会产生积分。
func accumulateDaily(ctx context.Context, svcCtx *ServiceContext, userID, day string, points int64) error {
	limit := svcCtx.Config.Idle.DailyPointsLimit

	var dp model.IdleDailyPoints
	// FirstOrCreate 原子地确保当日记录存在，避免并发下两个 Create 触发唯一约束冲突。
	err := svcCtx.Db.WithContext(ctx).
		Where("user_id = ? AND day = ?", userID, day).
		Attrs(model.IdleDailyPoints{UserID: userID, Day: day, Total: 0}).
		FirstOrCreate(&dp).Error
	if err != nil {
		return errorx.New(errorx.CodeDBError, err.Error())
	}
	if dp.Total+points > limit {
		return errorx.New(errorx.CodeDailyPointsLimit)
	}
	if err := svcCtx.Db.WithContext(ctx).Model(&dp).
		UpdateColumn("total", gorm.Expr("total + ?", points)).Error; err != nil {
		return errorx.New(errorx.CodeDBError, err.Error())
	}
	return nil
}

// addPoints 进程内调用 AddPoints logic 入账。
func addPoints(ctx context.Context, svcCtx *ServiceContext, userID string, points int64, reason string) error {
	// 直接操作 DB 入账，避免 import logic 包造成循环引用。
	// logic 包的 NewAddPointsLogic 逻辑等价于以下操作。
	tx := svcCtx.Db.WithContext(ctx).Begin()
	defer func() {
		if r := recover(); r != nil {
			tx.Rollback()
		}
	}()

	var user model.User
	if err := tx.Where("user_id = ?", userID).First(&user).Error; err != nil {
		tx.Rollback()
		return errorx.New(errorx.CodeNotFound, "user not found")
	}
	if err := tx.Model(&user).UpdateColumn("points_balance", gorm.Expr("points_balance + ?", points)).Error; err != nil {
		tx.Rollback()
		return errorx.New(errorx.CodeDBError, err.Error())
	}
	if err := tx.Commit().Error; err != nil {
		return errorx.New(errorx.CodeDBError, err.Error())
	}
	return nil
}

// reportIdleProgress 上报挂机任务进度（失败不阻塞结算，仅记日志）。
func reportIdleProgress(ctx context.Context, svcCtx *ServiceContext, userID string, delta int32) {
	for _, key := range []string{model.TaskKeyDailyIdle30, model.TaskKeyWeeklyIdle300} {
		// 任务进度上报：查找 key 对应的 task，累加进度。
		var task model.Task
		if err := svcCtx.Db.WithContext(ctx).Where("task_key = ?", key).First(&task).Error; err != nil {
			// 任务未配置不阻塞结算，仅 debug 记录。
			logx.WithContext(ctx).Debugf("[settle] task key %s not found: %v", key, err)
			continue
		}
		if err := svcCtx.Db.WithContext(ctx).Model(&model.UserTaskProgress{}).
			Where("user_id = ? AND task_id = ?", userID, task.ID).
			UpdateColumn("current_progress", gorm.Expr("current_progress + ?", delta)).Error; err != nil {
			logx.WithContext(ctx).Debugf("[settle] report progress %s delta=%d failed: %v", key, delta, err)
		}
	}
}

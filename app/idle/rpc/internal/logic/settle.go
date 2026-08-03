package logic

import (
	"context"
	"time"

	"github.com/cdcdx/hc-framework-go/app/idle/rpc/internal/svc"
	userpb "github.com/cdcdx/hc-framework-go/app/user/rpc/user"
	"github.com/cdcdx/hc-framework-go/common/errorx"
	"github.com/cdcdx/hc-framework-go/common/model"
	"gorm.io/gorm"
)

// settle 结算一条活跃挂机记录：
//  1. 按 [StartTime, until] 分钟数 × PointsPerMinute 计算积分；
//  2. 每日上限（IdleDailyPoints 表累加）拦截；
//  3. 调用 user-rpc AddPoints 入账；
//  4. 落库更新记录状态/时长/积分。
//
// until 语义：正常停止 = now；心跳超时 = 最后心跳时间。
func settle(ctx context.Context, svcCtx *svc.ServiceContext, rec *model.IdleRecord, until time.Time, status string) (int64, error) {
	durSec := int(until.Sub(rec.StartTime).Seconds())
	if durSec < 0 {
		durSec = 0
	}
	points := int64(durSec/60) * int64(svcCtx.Config.Idle.PointsPerMinute)
	if points < 0 {
		points = 0
	}

	if points > 0 {
		// 每日上限
		day := until.Format("2006-01-02")
		if err := accumulateDaily(ctx, svcCtx, rec.UserID, day, points); err != nil {
			return 0, err
		}
		// 积分入账（user-rpc）
		if _, err := svcCtx.UserRpc.AddPoints(ctx, &userpb.AddPointsRequest{
			UserId: rec.UserID,
			Points: points,
			Reason: "idle",
		}); err != nil {
			return 0, err
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
	return points, nil
}

// accumulateDaily 按 (user_id, day) 累加今日挂机积分，超过每日上限返回 CodeDailyPointsLimit
func accumulateDaily(ctx context.Context, svcCtx *svc.ServiceContext, userID, day string, points int64) error {
	limit := svcCtx.Config.Idle.DailyPointsLimit

	var dp model.IdleDailyPoints
	err := svcCtx.Db.Where("user_id = ? AND day = ?", userID, day).First(&dp).Error
	if err == nil {
		if dp.Total+points > limit {
			return errorx.New(errorx.CodeDailyPointsLimit)
		}
		if err := svcCtx.Db.Model(&dp).
			UpdateColumn("total", gorm.Expr("total + ?", points)).Error; err != nil {
			return errorx.New(errorx.CodeDBError, err.Error())
		}
		return nil
	}
	if err != gorm.ErrRecordNotFound {
		return errorx.New(errorx.CodeDBError, err.Error())
	}
	if points > limit {
		return errorx.New(errorx.CodeDailyPointsLimit)
	}
	if err := svcCtx.Db.Create(&model.IdleDailyPoints{
		UserID: userID,
		Day:    day,
		Total:  points,
	}).Error; err != nil {
		return errorx.New(errorx.CodeDBError, err.Error())
	}
	return nil
}

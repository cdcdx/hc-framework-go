package logic

import (
	"context"
	"time"

	"github.com/cdcdx/hc-framework-go/app/task/rpc/internal/svc"
	"github.com/cdcdx/hc-framework-go/app/task/rpc/task"
	"github.com/cdcdx/hc-framework-go/common/errorx"
	"github.com/cdcdx/hc-framework-go/common/model"
	"github.com/zeromicro/go-zero/core/logx"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// ReportProgressLogic 上报进度增量（跨领域事件驱动：挂机结算 / 兑换完成）
type ReportProgressLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewReportProgressLogic(ctx context.Context, svcCtx *svc.ServiceContext) *ReportProgressLogic {
	return &ReportProgressLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *ReportProgressLogic) ReportProgress(in *task.ReportProgressRequest) (*task.Empty, error) {
	if in.UserId == "" || in.TaskKey == "" {
		return nil, errorx.New(errorx.CodeInvalidParam, "user_id and task_key are required")
	}
	if in.Delta <= 0 {
		in.Delta = 1
	}

	// 按业务 key 定位任务（无此任务则静默忽略，避免跨域调用被脏 key 打爆）
	var t model.Task
	err := l.svcCtx.Db.Where("task_key = ? AND is_active = ?", in.TaskKey, true).First(&t).Error
	if err == gorm.ErrRecordNotFound {
		return &task.Empty{}, nil
	}
	if err != nil {
		return nil, errorx.New(errorx.CodeDBError, err.Error())
	}

	period := periodOf(&t, time.Now())
	delta := int(in.Delta)

	// 原子 upsert 累加进度（三方言 ON CONFLICT），并发上报不丢
	p := model.UserTaskProgress{
		UserID:          in.UserId,
		TaskID:          t.ID,
		Period:          period,
		CurrentProgress: delta,
	}
	err = l.svcCtx.Db.Clauses(clause.OnConflict{
		Columns: []clause.Column{{Name: "user_id"}, {Name: "task_id"}, {Name: "period"}},
		DoUpdates: clause.Assignments(map[string]any{
			"current_progress": gorm.Expr("user_task_progress.current_progress + ?", delta),
		}),
	}).Create(&p).Error
	if err != nil {
		return nil, errorx.New(errorx.CodeDBError, err.Error())
	}

	// 读回，若达标补完成标记
	if err := l.svcCtx.Db.
		Where("user_id = ? AND task_id = ? AND period = ?", in.UserId, t.ID, period).
		First(&p).Error; err != nil {
		return nil, errorx.New(errorx.CodeDBError, err.Error())
	}
	if p.CurrentProgress >= t.TargetValue && !p.IsCompleted {
		now := time.Now()
		if err := l.svcCtx.Db.Model(&p).Updates(map[string]any{
			"is_completed": true,
			"completed_at": now,
		}).Error; err != nil {
			return nil, errorx.New(errorx.CodeDBError, err.Error())
		}
	}

	return &task.Empty{}, nil
}

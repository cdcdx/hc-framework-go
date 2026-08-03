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
)

// ReportProgressLogic 上报任务进度（累加并自动判定完成）
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

func (l *ReportProgressLogic) ReportProgress(in *task.ReportProgressRequest) (*task.ReportProgressResponse, error) {
	if in.Delta <= 0 {
		return nil, errorx.New(errorx.CodeInvalidParam, "delta must be positive")
	}

	var define model.Task
	err := l.svcCtx.Db.Where("task_key = ?", in.TaskKey).First(&define).Error
	if err == gorm.ErrRecordNotFound {
		return nil, errorx.New(errorx.CodeNotFound, "task not found")
	}
	if err != nil {
		return nil, errorx.New(errorx.CodeDBError, err.Error())
	}

	var pro model.UserTaskProgress
	period := periodOf(&define, time.Now())
	err = l.svcCtx.Db.Where("user_id = ? AND task_id = ?", in.UserId, define.ID).First(&pro).Error
	if err == gorm.ErrRecordNotFound {
		pro = model.UserTaskProgress{
			UserID: in.UserId,
			TaskID: define.ID,
			Period: period,
		}
	} else if err != nil {
		return nil, errorx.New(errorx.CodeDBError, err.Error())
	}

	// 已完成且已领取的，不再累加
	if pro.IsClaimed {
		return &task.ReportProgressResponse{
			Task:      toTaskInfo(&define, &pro, period),
			Completed: pro.IsCompleted,
		}, nil
	}

	pro.CurrentProgress += int(in.Delta)
	pro.UpdatedAt = time.Now()
	if !pro.IsCompleted && pro.CurrentProgress >= define.TargetValue {
		pro.IsCompleted = true
	}

	if err := l.svcCtx.Db.Save(&pro).Error; err != nil {
		return nil, errorx.New(errorx.CodeDBError, err.Error())
	}

	return &task.ReportProgressResponse{
		Task:      toTaskInfo(&define, &pro, period),
		Completed: pro.IsCompleted,
	}, nil
}

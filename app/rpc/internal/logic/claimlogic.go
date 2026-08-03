package logic

import (
	"context"
	"time"

	"github.com/cdcdx/hc-framework-go/app/rpc/hc"
	"github.com/cdcdx/hc-framework-go/app/rpc/internal/svc"
	"github.com/cdcdx/hc-framework-go/common/errorx"
	"github.com/cdcdx/hc-framework-go/common/model"
	"github.com/zeromicro/go-zero/core/logx"
	"gorm.io/gorm"
)

// ClaimLogic 领取任务奖励（奖励入账为进程内调用 user 域 AddPoints）
type ClaimLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewClaimLogic(ctx context.Context, svcCtx *svc.ServiceContext) *ClaimLogic {
	return &ClaimLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *ClaimLogic) Claim(in *hc.TaskClaimRequest) (*hc.TaskClaimResponse, error) {
	var t model.Task
	err := l.svcCtx.Db.Where("id = ?", in.TaskId).First(&t).Error
	if err == gorm.ErrRecordNotFound {
		return nil, errorx.New(errorx.CodeNotFound, "task not found")
	}
	if err != nil {
		return nil, errorx.New(errorx.CodeDBError, err.Error())
	}

	period := periodOf(&t, time.Now())
	var p model.UserTaskProgress
	err = l.svcCtx.Db.
		Where("user_id = ? AND task_id = ? AND period = ?", in.UserId, in.TaskId, period).
		First(&p).Error
	if err == gorm.ErrRecordNotFound || (err == nil && !p.IsCompleted) {
		return nil, errorx.New(errorx.CodeTaskNotCompleted)
	}
	if err != nil {
		return nil, errorx.New(errorx.CodeDBError, err.Error())
	}
	if p.IsClaimed {
		return nil, errorx.New(errorx.CodeTaskClaimed)
	}

	// 奖励入账（进程内 user 域）
	if _, err := NewAddPointsLogic(l.ctx, l.svcCtx).AddPoints(&hc.AddPointsRequest{
		UserId: in.UserId,
		Points: t.RewardPoints,
		Reason: "task:" + t.TaskKey,
	}); err != nil {
		return nil, err
	}

	if err := l.svcCtx.Db.Model(&p).UpdateColumn("is_claimed", true).Error; err != nil {
		return nil, errorx.New(errorx.CodeDBError, err.Error())
	}

	return &hc.TaskClaimResponse{
		Task:         toTaskInfo(&t, &p, period),
		RewardPoints: t.RewardPoints,
	}, nil
}

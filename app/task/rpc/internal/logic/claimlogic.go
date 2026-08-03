package logic

import (
	"context"
	"time"

	"github.com/cdcdx/hc-framework-go/app/task/rpc/internal/svc"
	"github.com/cdcdx/hc-framework-go/app/task/rpc/task"
	userpb "github.com/cdcdx/hc-framework-go/app/user/rpc/user"
	"github.com/cdcdx/hc-framework-go/common/errorx"
	"github.com/cdcdx/hc-framework-go/common/model"
	"github.com/zeromicro/go-zero/core/logx"
	"gorm.io/gorm"
)

// ClaimLogic 领取任务奖励
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

func (l *ClaimLogic) Claim(in *task.ClaimRequest) (*task.ClaimResponse, error) {
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

	// 奖励入账（user-rpc）
	if _, err := l.svcCtx.UserRpc.AddPoints(l.ctx, &userpb.AddPointsRequest{
		UserId: in.UserId,
		Points: t.RewardPoints,
		Reason: "task:" + t.TaskKey,
	}); err != nil {
		return nil, err
	}

	if err := l.svcCtx.Db.Model(&p).UpdateColumn("is_claimed", true).Error; err != nil {
		return nil, errorx.New(errorx.CodeDBError, err.Error())
	}

	return &task.ClaimResponse{
		Task:         toTaskInfo(&t, &p, period),
		RewardPoints: t.RewardPoints,
	}, nil
}

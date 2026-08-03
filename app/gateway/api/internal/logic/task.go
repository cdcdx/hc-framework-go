package logic

import (
	"context"

	"github.com/cdcdx/hc-framework-go/app/gateway/api/internal/svc"
	"github.com/cdcdx/hc-framework-go/app/gateway/api/internal/types"
	hcpb "github.com/cdcdx/hc-framework-go/app/rpc/hc"
	"github.com/zeromicro/go-zero/core/logx"
)

// TaskListLogic 任务列表
type TaskListLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewTaskListLogic(ctx context.Context, svcCtx *svc.ServiceContext) *TaskListLogic {
	return &TaskListLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *TaskListLogic) TaskList() (any, error) {
	uid, ok := requireUser(l.ctx)
	if !ok {
		return nil, errUserUnauthorized
	}
	resp, err := l.svcCtx.HcRpc.TaskList(l.ctx, &hcpb.TaskListRequest{UserId: uid})
	if err != nil {
		return nil, err
	}
	return toData(resp), nil
}

// TaskProgressLogic 任务进度
type TaskProgressLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewTaskProgressLogic(ctx context.Context, svcCtx *svc.ServiceContext) *TaskProgressLogic {
	return &TaskProgressLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *TaskProgressLogic) TaskProgress() (any, error) {
	uid, ok := requireUser(l.ctx)
	if !ok {
		return nil, errUserUnauthorized
	}
	resp, err := l.svcCtx.HcRpc.TaskProgress(l.ctx, &hcpb.TaskProgressRequest{UserId: uid})
	if err != nil {
		return nil, err
	}
	return toData(resp), nil
}

// TaskClaimLogic 领取任务奖励
type TaskClaimLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewTaskClaimLogic(ctx context.Context, svcCtx *svc.ServiceContext) *TaskClaimLogic {
	return &TaskClaimLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *TaskClaimLogic) TaskClaim(req *types.TaskClaimReq) (any, error) {
	uid, ok := requireUser(l.ctx)
	if !ok {
		return nil, errUserUnauthorized
	}
	resp, err := l.svcCtx.HcRpc.TaskClaim(l.ctx, &hcpb.TaskClaimRequest{
		UserId: uid,
		TaskId: req.ID,
	})
	if err != nil {
		return nil, err
	}
	return toData(resp), nil
}

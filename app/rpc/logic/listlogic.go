package logic

import (
	"context"

	"github.com/cdcdx/hc-framework-go/app/rpc/hc"
	"github.com/cdcdx/hc-framework-go/app/rpc/svc"
	"github.com/cdcdx/hc-framework-go/common/errorx"
	"github.com/zeromicro/go-zero/core/logx"
)

// ListLogic 任务列表（含当前用户进度）
type ListLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewListLogic(ctx context.Context, svcCtx *svc.ServiceContext) *ListLogic {
	return &ListLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *ListLogic) List(in *hc.TaskListRequest) (*hc.TaskListResponse, error) {
	tasks, err := loadTaskInfos(l.svcCtx, in.UserId)
	if err != nil {
		return nil, errorx.NewErr(errorx.CodeDBError, err)
	}
	return &hc.TaskListResponse{Tasks: tasks}, nil
}

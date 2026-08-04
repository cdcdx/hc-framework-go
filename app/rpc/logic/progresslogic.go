package logic

import (
	"context"

	"github.com/cdcdx/hc-framework-go/app/rpc/hc"
	"github.com/cdcdx/hc-framework-go/app/rpc/svc"
	"github.com/cdcdx/hc-framework-go/common/errorx"
	"github.com/zeromicro/go-zero/core/logx"
)

// ProgressLogic 任务进度总览（结构同 List，客户端可统一渲染）
type ProgressLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewProgressLogic(ctx context.Context, svcCtx *svc.ServiceContext) *ProgressLogic {
	return &ProgressLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *ProgressLogic) Progress(in *hc.TaskProgressRequest) (*hc.TaskProgressResponse, error) {
	tasks, err := loadTaskInfos(l.svcCtx.Db, in.UserId)
	if err != nil {
		return nil, errorx.NewErr(errorx.CodeDBError, err)
	}
	return &hc.TaskProgressResponse{Tasks: tasks}, nil
}

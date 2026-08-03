package logic

import (
	"context"
	"time"

	"github.com/cdcdx/hc-framework-go/app/task/rpc/internal/svc"
	"github.com/cdcdx/hc-framework-go/app/task/rpc/task"
	"github.com/cdcdx/hc-framework-go/common/errorx"
	"github.com/cdcdx/hc-framework-go/common/model"
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

func (l *ProgressLogic) Progress(in *task.ProgressRequest) (*task.ProgressResponse, error) {
	var tasks []model.Task
	if err := l.svcCtx.Db.Where("is_active = ?", true).Order("id ASC").Find(&tasks).Error; err != nil {
		return nil, errorx.New(errorx.CodeDBError, err.Error())
	}

	var pros []model.UserTaskProgress
	if err := l.svcCtx.Db.Where("user_id = ?", in.UserId).Find(&pros).Error; err != nil {
		return nil, errorx.New(errorx.CodeDBError, err.Error())
	}
	proMap := make(map[int64]*model.UserTaskProgress, len(pros))
	for i := range pros {
		proMap[pros[i].TaskID] = &pros[i]
	}

	now := time.Now()
	out := make([]*task.TaskInfo, 0, len(tasks))
	for i := range tasks {
		out = append(out, toTaskInfo(&tasks[i], proMap[tasks[i].ID], periodOf(&tasks[i], now)))
	}
	return &task.ProgressResponse{Tasks: out}, nil
}

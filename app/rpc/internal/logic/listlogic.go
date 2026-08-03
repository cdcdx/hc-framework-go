package logic

import (
	"context"
	"time"

	"github.com/cdcdx/hc-framework-go/app/rpc/internal/svc"
	"github.com/cdcdx/hc-framework-go/app/rpc/hc"
	"github.com/cdcdx/hc-framework-go/common/errorx"
	"github.com/cdcdx/hc-framework-go/common/model"
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
	out := make([]*hc.TaskInfo, 0, len(tasks))
	for i := range tasks {
		out = append(out, toTaskInfo(&tasks[i], proMap[tasks[i].ID], periodOf(&tasks[i], now)))
	}
	return &hc.TaskListResponse{Tasks: out}, nil
}

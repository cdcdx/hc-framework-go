package logic

import (
	"context"
	"encoding/json"

	"github.com/cdcdx/hc-framework-go/app/rpc/hc"
	"github.com/cdcdx/hc-framework-go/app/rpc/svc"
	"github.com/cdcdx/hc-framework-go/common/errorx"
	"github.com/cdcdx/hc-framework-go/common/gormx"
	"github.com/cdcdx/hc-framework-go/common/model"
	"github.com/zeromicro/go-zero/core/logx"
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

func (l *ReportProgressLogic) ReportProgress(in *hc.ReportProgressRequest) (*hc.Empty, error) {
	if in.Delta <= 0 {
		return nil, errorx.New(errorx.CodeInvalidParam, "delta must be positive")
	}

	// 仅同步校验任务定义是否存在；进度累加走异步聚合，避免每次请求命中 user_task_progress 的读写。
	var define model.Task
	err := l.svcCtx.Db.Where("task_key = ?", in.TaskKey).First(&define).Error
	if gormx.IsRecordNotFound(err) {
		return nil, errorx.New(errorx.CodeNotFound, "task not found")
	}
	if err != nil {
		return nil, errorx.NewErr(errorx.CodeDBError, err)
	}

	// 已领取的任务不再累加（读一次轻量判定，必要时可缓存）。
	var claimed int64
	if err := l.svcCtx.Db.Model(&model.UserTaskProgress{}).
		Where("user_id = ? AND task_id = ? AND is_claimed = ?", in.UserId, define.ID, true).
		Count(&claimed).Error; err != nil {
		return nil, errorx.NewErr(errorx.CodeDBError, err)
	}
	if claimed > 0 {
		return &hc.Empty{}, nil
	}

	// 异步化：投递 task_progress 事件，由 progressAgg 聚合后批量写入，
	// 与兑换链路共用同一套解耦管道，彻底消除逐请求随机写与 record-not-found 噪声。
	payload, err := json.Marshal(svc.TaskProgressEvent{
		UserID:  in.UserId,
		TaskKey: in.TaskKey,
		Delta:   int64(in.Delta),
	})
	if err != nil {
		return nil, errorx.NewErr(errorx.CodeUnknownError, err)
	}
	// MQ 未启用时退化为同步聚合（直接入缓冲，等效于原同步写）。
	if l.svcCtx.MQProducer == nil {
		l.svcCtx.ProgressAgg().Add(in.UserId, in.TaskKey, int64(in.Delta))
		return &hc.Empty{}, nil
	}
	if err := l.svcCtx.Publish(l.ctx, "task_progress", in.UserId, payload); err != nil {
		l.Errorf("publish task progress %s failed: %v", in.TaskKey, err)
		return nil, errorx.NewErr(errorx.CodeUnknownError, err)
	}

	return &hc.Empty{}, nil
}

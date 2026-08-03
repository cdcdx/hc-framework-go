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

// StartLogic 开始挂机
type StartLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewStartLogic(ctx context.Context, svcCtx *svc.ServiceContext) *StartLogic {
	return &StartLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *StartLogic) Start(in *hc.IdleStartRequest) (*hc.IdleRecord, error) {
	if in.DeviceId == "" {
		return nil, errorx.New(errorx.CodeInvalidParam, "device_id is required")
	}

	// 同设备已挂机 → 幂等返回现有记录
	var existing model.IdleRecord
	err := l.svcCtx.Db.
		Where("user_id = ? AND device_id = ? AND status = ?", in.UserId, in.DeviceId, model.IdleStatusActive).
		First(&existing).Error
	if err == nil {
		return toIdleRecord(&existing), nil
	}
	if err != gorm.ErrRecordNotFound {
		return nil, errorx.New(errorx.CodeDBError, err.Error())
	}

	// 活跃设备数上限
	var count int64
	if err := l.svcCtx.Db.Model(&model.IdleRecord{}).
		Where("user_id = ? AND status = ?", in.UserId, model.IdleStatusActive).
		Count(&count).Error; err != nil {
		return nil, errorx.New(errorx.CodeDBError, err.Error())
	}
	if count >= int64(l.svcCtx.Config.Idle.MaxActiveDevices) {
		return nil, errorx.New(errorx.CodeUnknownError, "maximum active devices reached")
	}

	now := time.Now()
	rec := &model.IdleRecord{
		UserID:          in.UserId,
		DeviceID:        in.DeviceId,
		StartTime:       now,
		LastHeartbeatAt: &now,
		Status:          model.IdleStatusActive,
	}
	if err := l.svcCtx.Db.Create(rec).Error; err != nil {
		return nil, errorx.New(errorx.CodeDBError, err.Error())
	}
	return toIdleRecord(rec), nil
}

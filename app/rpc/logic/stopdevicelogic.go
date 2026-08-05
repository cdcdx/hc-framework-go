package logic

import (
	"context"
	"time"

	"github.com/cdcdx/hc-framework-go/app/rpc/hc"
	"github.com/cdcdx/hc-framework-go/app/rpc/svc"
	"github.com/cdcdx/hc-framework-go/common/errorx"
	"github.com/cdcdx/hc-framework-go/common/gormx"
	"github.com/cdcdx/hc-framework-go/common/model"
	"github.com/zeromicro/go-zero/core/logx"
)

// StopDeviceLogic 停止指定设备挂机并结算
type StopDeviceLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewStopDeviceLogic(ctx context.Context, svcCtx *svc.ServiceContext) *StopDeviceLogic {
	return &StopDeviceLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *StopDeviceLogic) StopDevice(in *hc.IdleStopDeviceRequest) (*hc.IdleRecord, error) {
	if in.DeviceId == "" {
		return nil, errorx.New(errorx.CodeInvalidParam, "device_id is required")
	}

	var rec model.IdleRecord
	err := l.svcCtx.Db.
		Where("user_id = ? AND device_id = ? AND status = ?", in.UserId, in.DeviceId, model.IdleStatusActive).
		First(&rec).Error
	if gormx.IsRecordNotFound(err) {
		return nil, errorx.New(errorx.CodeNotIdle)
	}
	if err != nil {
		return nil, errorx.NewErr(errorx.CodeDBError, err)
	}

	if _, err := settle(l.ctx, l.svcCtx, &rec, time.Now(), model.IdleStatusCompleted); err != nil {
		return nil, err
	}
	return toIdleRecord(&rec), nil
}

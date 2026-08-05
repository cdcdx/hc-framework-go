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

// HeartbeatLogic 心跳上报
type HeartbeatLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewHeartbeatLogic(ctx context.Context, svcCtx *svc.ServiceContext) *HeartbeatLogic {
	return &HeartbeatLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *HeartbeatLogic) Heartbeat(in *hc.IdleHeartbeatRequest) (*hc.Empty, error) {
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

	now := time.Now()
	// 心跳超时（超过阈值未续命）：结算为 timeout，需重新 start
	if rec.LastHeartbeatAt != nil && now.Sub(*rec.LastHeartbeatAt) > 5*time.Minute {
		if _, err := settle(l.ctx, l.svcCtx, &rec, *rec.LastHeartbeatAt, model.IdleStatusTimeout); err != nil {
			return nil, err
		}
		return nil, errorx.New(errorx.CodeHeartbeatTimeout)
	}

	if err := l.svcCtx.Db.Model(&rec).UpdateColumn("last_heartbeat_at", now).Error; err != nil {
		return nil, errorx.NewErr(errorx.CodeDBError, err)
	}
	return &hc.Empty{}, nil
}

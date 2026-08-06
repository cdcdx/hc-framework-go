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
	// 心跳超时（超过阈值未续命）：结算为 timeout，需重新 start。
	// 超时判定的时间源优先 Redis（心跳续期 key），缺失时回退 DB 的 last_heartbeat_at。
	timeout := time.Duration(l.svcCtx.Config.Idle.TimeoutMinutes) * time.Minute
	if timeout <= 0 {
		timeout = 5 * time.Minute
	}
	if hb := l.svcCtx.LastHeartbeat(l.ctx, in.DeviceId, rec.LastHeartbeatAt); hb != nil && now.Sub(*hb) > timeout {
		if _, err := settle(l.ctx, l.svcCtx, &rec, *hb, model.IdleStatusTimeout); err != nil {
			return nil, err
		}
		return nil, errorx.New(errorx.CodeHeartbeatTimeout)
	}

	// 续期心跳：有 Redis 则只写 Redis（不落库），否则回退写 DB。
	if err := l.svcCtx.TouchHeartbeat(l.ctx, &rec, in.DeviceId, now); err != nil {
		return nil, errorx.NewErr(errorx.CodeDBError, err)
	}
	return &hc.Empty{}, nil
}

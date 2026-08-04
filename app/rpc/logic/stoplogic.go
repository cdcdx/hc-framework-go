package logic

import (
	"context"
	"time"

	"github.com/cdcdx/hc-framework-go/app/rpc/hc"
	"github.com/cdcdx/hc-framework-go/app/rpc/svc"
	"github.com/cdcdx/hc-framework-go/common/errorx"
	"github.com/cdcdx/hc-framework-go/common/model"
	"github.com/zeromicro/go-zero/core/logx"
)

// StopLogic 停止全部设备挂机并结算
type StopLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewStopLogic(ctx context.Context, svcCtx *svc.ServiceContext) *StopLogic {
	return &StopLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *StopLogic) Stop(in *hc.IdleStopRequest) (*hc.IdleRecord, error) {
	var recs []model.IdleRecord
	if err := l.svcCtx.Db.
		Where("user_id = ? AND status = ?", in.UserId, model.IdleStatusActive).
		Order("id ASC").
		Find(&recs).Error; err != nil {
		return nil, errorx.New(errorx.CodeDBError, err.Error())
	}
	if len(recs) == 0 {
		return nil, errorx.New(errorx.CodeNotIdle)
	}

	now := time.Now()
	var last *hc.IdleRecord
	for i := range recs {
		if _, err := settle(l.ctx, l.svcCtx, &recs[i], now, model.IdleStatusCompleted); err != nil {
			return nil, err
		}
		last = toIdleRecord(&recs[i])
	}
	return last, nil
}

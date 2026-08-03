package logic

import (
	"context"
	"time"

	"github.com/cdcdx/hc-framework-go/app/rpc/hc"
	"github.com/cdcdx/hc-framework-go/app/rpc/internal/svc"
	"github.com/cdcdx/hc-framework-go/common/errorx"
	"github.com/cdcdx/hc-framework-go/common/model"
	"github.com/zeromicro/go-zero/core/logx"
)

// StatusLogic 挂机状态（活跃会话 + 今日积分）
type StatusLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewStatusLogic(ctx context.Context, svcCtx *svc.ServiceContext) *StatusLogic {
	return &StatusLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *StatusLogic) Status(in *hc.IdleStatusRequest) (*hc.IdleStatusResponse, error) {
	var recs []model.IdleRecord
	if err := l.svcCtx.Db.
		Where("user_id = ? AND status = ?", in.UserId, model.IdleStatusActive).
		Order("id ASC").
		Find(&recs).Error; err != nil {
		return nil, errorx.New(errorx.CodeDBError, err.Error())
	}

	// 今日已赚积分（IdleDailyPoints 已结算累计）
	day := time.Now().Format("2006-01-02")
	var dp model.IdleDailyPoints
	todayPoints := int64(0)
	err := l.svcCtx.Db.Where("user_id = ? AND day = ?", in.UserId, day).First(&dp).Error
	if err == nil {
		todayPoints = dp.Total
	} else if err != nil && !isRecordNotFound(err) {
		return nil, errorx.New(errorx.CodeDBError, err.Error())
	}

	out := make([]*hc.IdleRecord, 0, len(recs))
	for i := range recs {
		out = append(out, toIdleRecord(&recs[i]))
	}
	return &hc.IdleStatusResponse{
		Records:     out,
		TodayPoints: todayPoints,
	}, nil
}

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

// FlashActivitiesLogic 进行中的抢购活动列表（时间窗内、状态生效）
type FlashActivitiesLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewFlashActivitiesLogic(ctx context.Context, svcCtx *svc.ServiceContext) *FlashActivitiesLogic {
	return &FlashActivitiesLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *FlashActivitiesLogic) FlashActivities(in *hc.FlashActivitiesRequest) (*hc.FlashActivitiesResponse, error) {
	limit := int(in.Limit)
	if limit <= 0 {
		limit = 20
	}
	if limit > 100 {
		limit = 100
	}

	now := time.Now()
	var acts []model.ShopFlashActivity
	err := l.svcCtx.Db.
		Where("status = ? AND start_time <= ? AND (end_time IS NULL OR end_time >= ?)",
			model.FlashSaleStatusActive, now, now).
		Order("start_time ASC").
		Limit(limit).
		Find(&acts).Error
	if err != nil {
		return nil, errorx.New(errorx.CodeDBError, err.Error())
	}

	out := make([]*hc.FlashActivityInfo, 0, len(acts))
	for i := range acts {
		out = append(out, toFlashActivity(&acts[i]))
	}
	return &hc.FlashActivitiesResponse{Activities: out}, nil
}

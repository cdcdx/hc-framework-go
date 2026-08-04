package logic

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/cdcdx/hc-framework-go/app/rpc/hc"
	"github.com/cdcdx/hc-framework-go/app/rpc/svc"
	"github.com/cdcdx/hc-framework-go/common/errorx"
	"github.com/cdcdx/hc-framework-go/common/model"
	"github.com/zeromicro/go-zero/core/logx"
)

// FlashActivitiesLogic 进行中的抢购活动列表（时间窗内 + 两级缓存）。
//
// 缓存策略: TTL 5s（抢购期间高并发，短 TTL 保证实时性 + 大幅降低 DB 压力）。
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

	cacheKey := fmt.Sprintf("flash:activities:%d", limit)
	if cached, err := l.svcCtx.CachedGet(l.ctx, cacheKey, func(ctx context.Context) ([]byte, error) {
		return l.loadAndMarshal(limit)
	}); err == nil && cached != nil {
		var resp hc.FlashActivitiesResponse
		if err := json.Unmarshal(cached, &resp); err == nil {
			return &resp, nil
		}
	}

	return l.load(limit)
}

func (l *FlashActivitiesLogic) loadAndMarshal(limit int) ([]byte, error) {
	resp, err := l.load(limit)
	if err != nil {
		return nil, err
	}
	return json.Marshal(resp)
}

func (l *FlashActivitiesLogic) load(limit int) (*hc.FlashActivitiesResponse, error) {
	now := time.Now()
	var acts []model.ShopFlashActivity
	err := l.svcCtx.Db.
		Where("status = ? AND start_time <= ? AND (end_time IS NULL OR end_time >= ?)",
			model.FlashSaleStatusActive, now, now).
		Order("start_time ASC").
		Limit(limit).
		Find(&acts).Error
	if err != nil {
		return nil, errorx.NewErr(errorx.CodeDBError, err)
	}

	out := make([]*hc.FlashActivityInfo, 0, len(acts))
	for i := range acts {
		out = append(out, toFlashActivity(&acts[i]))
	}
	return &hc.FlashActivitiesResponse{Activities: out}, nil
}

package logic

import (
	"context"

	"github.com/cdcdx/hc-framework-go/app/rpc/svc"
	"github.com/cdcdx/hc-framework-go/app/rpc/hc"
	"github.com/cdcdx/hc-framework-go/common/errorx"
	"github.com/cdcdx/hc-framework-go/common/model"
	"github.com/zeromicro/go-zero/core/logx"
)

// ItemsLogic 商品列表（游标分页 + 分类筛选）
type ItemsLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewItemsLogic(ctx context.Context, svcCtx *svc.ServiceContext) *ItemsLogic {
	return &ItemsLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *ItemsLogic) Items(in *hc.ShopItemsRequest) (*hc.ShopItemsResponse, error) {
	limit := int(in.Limit)
	if limit <= 0 {
		limit = 20
	}
	if limit > 100 {
		limit = 100
	}

	query := l.svcCtx.Db.Model(&model.ShopItem{}).Where("is_active = ?", true)
	if in.Category != "" {
		query = query.Where("category = ?", in.Category)
	}
	if in.Cursor > 0 {
		query = query.Where("id < ?", in.Cursor)
	}

	var items []model.ShopItem
	if err := query.Order("id DESC").Limit(limit + 1).Find(&items).Error; err != nil {
		return nil, errorx.New(errorx.CodeDBError, err.Error())
	}

	hasMore := len(items) > limit
	if hasMore {
		items = items[:limit]
	}

	out := make([]*hc.ItemInfo, 0, len(items))
	var nextCursor int64
	for i := range items {
		out = append(out, toItemInfo(&items[i]))
		nextCursor = items[i].ID
	}
	return &hc.ShopItemsResponse{
		Items:      out,
		NextCursor: nextCursor,
		HasMore:    hasMore,
	}, nil
}

package logic

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/cdcdx/hc-framework-go/app/rpc/hc"
	"github.com/cdcdx/hc-framework-go/app/rpc/svc"
	"github.com/cdcdx/hc-framework-go/common/errorx"
	"github.com/cdcdx/hc-framework-go/common/model"
	"github.com/zeromicro/go-zero/core/logx"
)

// ItemsLogic 商品列表（游标分页 + 分类筛选 + 两级缓存）。
//
// 缓存策略:
//   - 默认分类（空 category）首页（无 cursor）走缓存，TTL 30s
//   - 带分类/游标的请求直接查 DB（分类组合太多，缓存命中率低）
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

	// 热点路径：默认分类首页走缓存
	if in.Category == "" && in.Cursor == 0 {
		cacheKey := fmt.Sprintf("shop:items:default:%d", limit)
		if cached, err := l.svcCtx.CachedGet(l.ctx, cacheKey, func(ctx context.Context) ([]byte, error) {
			return l.loadItemsAndMarshal(limit, "", 0)
		}); err == nil && cached != nil {
			var resp hc.ShopItemsResponse
			if err := json.Unmarshal(cached, &resp); err == nil {
				return &resp, nil
			}
		}
	}

	// 非热点路径：直接查 DB
	return l.loadItems(limit, in.Category, in.Cursor)
}

func (l *ItemsLogic) loadItemsAndMarshal(limit int, category string, cursor int64) ([]byte, error) {
	resp, err := l.loadItems(limit, category, cursor)
	if err != nil {
		return nil, err
	}
	return json.Marshal(resp)
}

func (l *ItemsLogic) loadItems(limit int, category string, cursor int64) (*hc.ShopItemsResponse, error) {
	query := l.svcCtx.Db.Model(&model.ShopItem{}).Where("is_active = ?", true)
	if category != "" {
		query = query.Where("category = ?", category)
	}
	if cursor > 0 {
		query = query.Where("id < ?", cursor)
	}

	var items []model.ShopItem
	if err := query.Order("id DESC").Limit(limit + 1).Find(&items).Error; err != nil {
		return nil, errorx.NewErr(errorx.CodeDBError, err)
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

// InvalidateItemsCache 商品变更时清除默认首页缓存（在 redeem/create/update 等写操作后调用）。
func InvalidateItemsCache(svcCtx *svc.ServiceContext) {
	if svcCtx.Cache == nil {
		return
	}
	for _, limit := range []int{10, 20} {
		_ = svcCtx.Cache.Delete(context.Background(), fmt.Sprintf("shop:items:default:%d", limit))
	}
}

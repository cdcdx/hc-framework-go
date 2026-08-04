package logic

import (
	"context"

	"github.com/cdcdx/hc-framework-go/app/rpc/hc"
	"github.com/cdcdx/hc-framework-go/app/rpc/svc"
	"github.com/cdcdx/hc-framework-go/common/errorx"
	"github.com/cdcdx/hc-framework-go/common/model"
	"github.com/zeromicro/go-zero/core/logx"
)

// OrdersLogic 兑换订单列表（游标分页）
type OrdersLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewOrdersLogic(ctx context.Context, svcCtx *svc.ServiceContext) *OrdersLogic {
	return &OrdersLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *OrdersLogic) Orders(in *hc.ShopOrdersRequest) (*hc.ShopOrdersResponse, error) {
	limit := int(in.Limit)
	if limit <= 0 {
		limit = 20
	}
	if limit > 100 {
		limit = 100
	}

	query := l.svcCtx.Db.Model(&model.RedeemOrder{}).Where("user_id = ?", in.UserId)
	if in.Cursor > 0 {
		query = query.Where("id < ?", in.Cursor)
	}

	var orders []model.RedeemOrder
	if err := query.Order("id DESC").Limit(limit + 1).Find(&orders).Error; err != nil {
		return nil, errorx.NewErr(errorx.CodeDBError, err)
	}

	hasMore := len(orders) > limit
	if hasMore {
		orders = orders[:limit]
	}

	out := make([]*hc.OrderInfo, 0, len(orders))
	var nextCursor int64
	for i := range orders {
		out = append(out, toOrderInfo(&orders[i]))
		nextCursor = orders[i].ID
	}
	return &hc.ShopOrdersResponse{
		Orders:     out,
		NextCursor: nextCursor,
		HasMore:    hasMore,
	}, nil
}

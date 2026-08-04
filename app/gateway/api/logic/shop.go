package logic

import (
	"context"
	"strconv"

	"github.com/cdcdx/hc-framework-go/app/gateway/api/svc"
	"github.com/cdcdx/hc-framework-go/app/gateway/api/types"
	hcpb "github.com/cdcdx/hc-framework-go/app/rpc/hc"
	"github.com/zeromicro/go-zero/core/logx"
)

// ShopItemsLogic 商品列表（分页）
type ShopItemsLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewShopItemsLogic(ctx context.Context, svcCtx *svc.ServiceContext) *ShopItemsLogic {
	return &ShopItemsLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *ShopItemsLogic) ShopItems(req *types.ShopItemsReq) (items any, nextCursor string, hasMore bool, err error) {
	if _, ok := requireUser(l.ctx); !ok {
		return nil, "", false, errUserUnauthorized
	}
	resp, err := l.svcCtx.HcRpc.ShopItems(l.ctx, &hcpb.ShopItemsRequest{
		Cursor:   req.Cursor,
		Limit:    int32(req.Limit),
		Category: req.Category,
	})
	if err != nil {
		return nil, "", false, err
	}
	return toDataList(resp.Items), strconv.FormatInt(resp.NextCursor, 10), resp.HasMore, nil
}

// ShopRedeemLogic 积分兑换
type ShopRedeemLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewShopRedeemLogic(ctx context.Context, svcCtx *svc.ServiceContext) *ShopRedeemLogic {
	return &ShopRedeemLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *ShopRedeemLogic) ShopRedeem(req *types.ShopRedeemReq) (any, error) {
	uid, ok := requireUser(l.ctx)
	if !ok {
		return nil, errUserUnauthorized
	}
	resp, err := l.svcCtx.HcRpc.ShopRedeem(l.ctx, &hcpb.ShopRedeemRequest{
		UserId:   uid,
		ItemId:   req.ItemID,
		Quantity: req.Quantity,
	})
	if err != nil {
		return nil, err
	}
	return toData(resp), nil
}

// ShopOrdersLogic 兑换订单列表（分页）
type ShopOrdersLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewShopOrdersLogic(ctx context.Context, svcCtx *svc.ServiceContext) *ShopOrdersLogic {
	return &ShopOrdersLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *ShopOrdersLogic) ShopOrders(req *types.ShopOrdersReq) (items any, nextCursor string, hasMore bool, err error) {
	uid, ok := requireUser(l.ctx)
	if !ok {
		return nil, "", false, errUserUnauthorized
	}
	resp, err := l.svcCtx.HcRpc.ShopOrders(l.ctx, &hcpb.ShopOrdersRequest{
		UserId: uid,
		Cursor: req.Cursor,
		Limit:  int32(req.Limit),
	})
	if err != nil {
		return nil, "", false, err
	}
	return toDataList(resp.Orders), strconv.FormatInt(resp.NextCursor, 10), resp.HasMore, nil
}

// ShopOrderDetailLogic 订单详情
type ShopOrderDetailLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewShopOrderDetailLogic(ctx context.Context, svcCtx *svc.ServiceContext) *ShopOrderDetailLogic {
	return &ShopOrderDetailLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *ShopOrderDetailLogic) ShopOrderDetail(req *types.ShopOrderDetailReq) (any, error) {
	uid, ok := requireUser(l.ctx)
	if !ok {
		return nil, errUserUnauthorized
	}
	resp, err := l.svcCtx.HcRpc.ShopOrderDetail(l.ctx, &hcpb.ShopOrderDetailRequest{
		UserId:  uid,
		OrderId: req.ID,
	})
	if err != nil {
		return nil, err
	}
	return toData(resp), nil
}

// ShopFlashActivitiesLogic 抢购活动列表
type ShopFlashActivitiesLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewShopFlashActivitiesLogic(ctx context.Context, svcCtx *svc.ServiceContext) *ShopFlashActivitiesLogic {
	return &ShopFlashActivitiesLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *ShopFlashActivitiesLogic) ShopFlashActivities(req *types.FlashActivitiesReq) (any, error) {
	if _, ok := requireUser(l.ctx); !ok {
		return nil, errUserUnauthorized
	}
	resp, err := l.svcCtx.HcRpc.FlashActivities(l.ctx, &hcpb.FlashActivitiesRequest{
		Limit: int32(req.Limit),
	})
	if err != nil {
		return nil, err
	}
	return toData(resp), nil
}

// ShopFlashRedeemLogic 定时抢购
type ShopFlashRedeemLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewShopFlashRedeemLogic(ctx context.Context, svcCtx *svc.ServiceContext) *ShopFlashRedeemLogic {
	return &ShopFlashRedeemLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *ShopFlashRedeemLogic) ShopFlashRedeem(req *types.FlashRedeemReq) (any, error) {
	uid, ok := requireUser(l.ctx)
	if !ok {
		return nil, errUserUnauthorized
	}
	resp, err := l.svcCtx.HcRpc.FlashRedeem(l.ctx, &hcpb.FlashRedeemRequest{
		UserId:     uid,
		ActivityId: req.ActivityID,
	})
	if err != nil {
		return nil, err
	}
	return toData(resp), nil
}

package handler

import (
	"net/http"

	"github.com/cdcdx/hc-framework-go/app/gateway/api/internal/logic"
	"github.com/cdcdx/hc-framework-go/app/gateway/api/internal/svc"
	"github.com/cdcdx/hc-framework-go/app/gateway/api/internal/types"
	"github.com/cdcdx/hc-framework-go/common/response"
	"github.com/zeromicro/go-zero/rest/httpx"
)

// ShopItemsHandler 商品列表（分页）
func ShopItemsHandler(svcCtx *svc.ServiceContext) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req types.ShopItemsReq
		if err := httpx.Parse(r, &req); err != nil {
			response.BadRequest(w, r, "invalid query params")
			return
		}
		l := logic.NewShopItemsLogic(r.Context(), svcCtx)
		items, nextCursor, hasMore, err := l.ShopItems(&req)
		if err != nil {
			handleError(w, r, err)
			return
		}
		response.SuccessPage(w, r, items, formatCursor(nextCursor), hasMore)
	}
}

// ShopRedeemHandler 积分兑换
func ShopRedeemHandler(svcCtx *svc.ServiceContext) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req types.ShopRedeemReq
		if err := httpx.Parse(r, &req); err != nil {
			response.BadRequest(w, r, "invalid request body")
			return
		}
		l := logic.NewShopRedeemLogic(r.Context(), svcCtx)
		resp, err := l.ShopRedeem(&req)
		if err != nil {
			handleError(w, r, err)
			return
		}
		response.Success(w, r, resp)
	}
}

// ShopOrdersHandler 兑换订单列表（分页）
func ShopOrdersHandler(svcCtx *svc.ServiceContext) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req types.ShopOrdersReq
		if err := httpx.Parse(r, &req); err != nil {
			response.BadRequest(w, r, "invalid query params")
			return
		}
		l := logic.NewShopOrdersLogic(r.Context(), svcCtx)
		items, nextCursor, hasMore, err := l.ShopOrders(&req)
		if err != nil {
			handleError(w, r, err)
			return
		}
		response.SuccessPage(w, r, items, formatCursor(nextCursor), hasMore)
	}
}

// ShopOrderDetailHandler 订单详情
func ShopOrderDetailHandler(svcCtx *svc.ServiceContext) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req types.ShopOrderDetailReq
		if err := httpx.Parse(r, &req); err != nil {
			response.BadRequest(w, r, "invalid order id")
			return
		}
		l := logic.NewShopOrderDetailLogic(r.Context(), svcCtx)
		resp, err := l.ShopOrderDetail(&req)
		if err != nil {
			handleError(w, r, err)
			return
		}
		response.Success(w, r, resp)
	}
}

// ShopFlashActivitiesHandler 抢购活动列表
func ShopFlashActivitiesHandler(svcCtx *svc.ServiceContext) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req types.FlashActivitiesReq
		if err := httpx.Parse(r, &req); err != nil {
			response.BadRequest(w, r, "invalid query params")
			return
		}
		l := logic.NewShopFlashActivitiesLogic(r.Context(), svcCtx)
		resp, err := l.ShopFlashActivities(&req)
		if err != nil {
			handleError(w, r, err)
			return
		}
		response.Success(w, r, resp)
	}
}

// ShopFlashRedeemHandler 定时抢购
func ShopFlashRedeemHandler(svcCtx *svc.ServiceContext) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req types.FlashRedeemReq
		if err := httpx.Parse(r, &req); err != nil {
			response.BadRequest(w, r, "invalid request body")
			return
		}
		l := logic.NewShopFlashRedeemLogic(r.Context(), svcCtx)
		resp, err := l.ShopFlashRedeem(&req)
		if err != nil {
			handleError(w, r, err)
			return
		}
		response.Success(w, r, resp)
	}
}

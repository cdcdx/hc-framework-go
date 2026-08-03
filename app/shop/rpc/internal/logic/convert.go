package logic

import (
	"github.com/cdcdx/hc-framework-go/app/shop/rpc/shop"
	"github.com/cdcdx/hc-framework-go/common/model"
)

// toItemInfo 商品模型 → rpc 结构（派生销售状态）
func toItemInfo(i *model.ShopItem) *shop.ItemInfo {
	i.SalesStatus = i.SalesStatusValue()
	return &shop.ItemInfo{
		Id:           i.ID,
		Name:         i.Name,
		Description:  i.Description,
		PricePoints:  i.PricePoints,
		Stock:        int32(i.Stock),
		ImageUrl:     i.ImageURL,
		Category:     i.Category,
		IsActive:     i.IsActive,
		SalesStatus:  i.SalesStatus,
	}
}

// toOrderInfo 订单模型 → rpc 结构
func toOrderInfo(o *model.RedeemOrder) *shop.OrderInfo {
	return &shop.OrderInfo{
		Id:          o.ID,
		UserId:      o.UserID,
		ItemId:      o.ItemID,
		ItemName:    o.ItemName,
		PointsSpent: o.PointsSpent,
		OrderStatus: o.OrderStatus,
		ActivityId:  o.ActivityID,
		CreatedAt:   o.CreatedAt.Unix(),
	}
}

// toFlashActivity 抢购活动模型 → rpc 结构（派生销售状态）
func toFlashActivity(a *model.ShopFlashActivity) *shop.FlashActivityInfo {
	a.SalesStatus = a.SalesStatusValue()
	info := &shop.FlashActivityInfo{
		Id:            a.ID,
		ItemId:        a.ItemID,
		Name:          a.Name,
		StartTime:     a.StartTime.Unix(),
		LimitQty:      int32(a.LimitQty),
		SoldQty:       int32(a.SoldQty),
		PerUserLimit:  int32(a.PerUserLimit),
		PricePoints:   a.PricePoints,
		Status:        a.Status,
		SalesStatus:   a.SalesStatus,
	}
	if a.EndTime != nil {
		info.EndTime = a.EndTime.Unix()
	}
	return info
}

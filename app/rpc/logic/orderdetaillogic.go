package logic

import (
	"context"

	"github.com/cdcdx/hc-framework-go/app/rpc/hc"
	"github.com/cdcdx/hc-framework-go/app/rpc/svc"
	"github.com/cdcdx/hc-framework-go/common/errorx"
	"github.com/cdcdx/hc-framework-go/common/gormx"
	"github.com/cdcdx/hc-framework-go/common/model"
	"github.com/zeromicro/go-zero/core/logx"
)

// OrderDetailLogic 订单详情（校验归属）
type OrderDetailLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewOrderDetailLogic(ctx context.Context, svcCtx *svc.ServiceContext) *OrderDetailLogic {
	return &OrderDetailLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *OrderDetailLogic) OrderDetail(in *hc.ShopOrderDetailRequest) (*hc.OrderInfo, error) {
	var order model.RedeemOrder
	err := l.svcCtx.Db.Where("id = ?", in.OrderId).First(&order).Error
	if gormx.IsRecordNotFound(err) {
		return nil, errorx.New(errorx.CodeNotFound, "order not found")
	}
	if err != nil {
		return nil, errorx.NewErr(errorx.CodeDBError, err)
	}

	// 订单归属校验
	if order.UserID != in.UserId {
		return nil, errorx.New(errorx.CodePermissionDenied)
	}

	return toOrderInfo(&order), nil
}

package logic

import (
	"context"
	"time"

	"github.com/cdcdx/hc-framework-go/app/rpc/svc"
	"github.com/cdcdx/hc-framework-go/common/model"
)

// settle 委托给 svc.SettleRecord（避免 svc ↔ logic 循环引用）。
// 保留此薄包装以维持 logic 包内所有调用方（heartbeat/stop/stopdevice）无需改动。
func settle(ctx context.Context, svcCtx *svc.ServiceContext, rec *model.IdleRecord, until time.Time, status string) (int64, error) {
	return svc.SettleRecord(ctx, svcCtx, rec, until, status)
}

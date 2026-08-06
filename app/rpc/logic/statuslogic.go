package logic

import (
	"context"
	"encoding/json"
	"time"

	"github.com/cdcdx/hc-framework-go/app/rpc/hc"
	"github.com/cdcdx/hc-framework-go/app/rpc/svc"
	"github.com/cdcdx/hc-framework-go/common/errorx"
	"github.com/cdcdx/hc-framework-go/common/model"
	"github.com/zeromicro/go-zero/core/logx"
)

// StatusLogic 挂机状态（活跃会话 + 今日积分）
type StatusLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewStatusLogic(ctx context.Context, svcCtx *svc.ServiceContext) *StatusLogic {
	return &StatusLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

// statusKey 挂机状态缓存 key。状态轮询高频，使用较短 TTL（3s）降低 DB 压力。
func statusKey(userID string) string {
	return "idle:status:" + userID
}

func (l *StatusLogic) Status(in *hc.IdleStatusRequest) (*hc.IdleStatusResponse, error) {
	key := statusKey(in.UserId)
	cached, err := l.svcCtx.CachedGet(l.ctx, key, func(ctx context.Context) ([]byte, error) {
		var recs []model.IdleRecord
		if err := l.svcCtx.Db.
			Where("user_id = ? AND status = ?", in.UserId, model.IdleStatusActive).
			Order("id ASC").
			Find(&recs).Error; err != nil {
			return nil, errorx.NewErr(errorx.CodeDBError, err)
		}

		// 今日已赚积分（IdleDailyPoints 已结算累计）
		day := time.Now().Format("2006-01-02")
		var dp model.IdleDailyPoints
		todayPoints := int64(0)
		err := l.svcCtx.Db.Where("user_id = ? AND day = ?", in.UserId, day).First(&dp).Error
		if err == nil {
			todayPoints = dp.Total
		} else if err != nil && !isRecordNotFound(err) {
			return nil, errorx.NewErr(errorx.CodeDBError, err)
		}

		out := make([]*hc.IdleRecord, 0, len(recs))
		for i := range recs {
			out = append(out, toIdleRecord(&recs[i]))
		}
		return json.Marshal(&hc.IdleStatusResponse{Records: out, TodayPoints: todayPoints})
	}, 3*time.Second)
	if err != nil {
		return nil, err
	}
	var resp hc.IdleStatusResponse
	if err := json.Unmarshal(cached, &resp); err != nil {
		return nil, errorx.NewErr(errorx.CodeDBError, err)
	}
	return &resp, nil
}

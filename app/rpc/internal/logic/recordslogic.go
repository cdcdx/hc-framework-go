package logic

import (
	"context"

	"github.com/cdcdx/hc-framework-go/app/rpc/hc"
	"github.com/cdcdx/hc-framework-go/app/rpc/internal/svc"
	"github.com/cdcdx/hc-framework-go/common/errorx"
	"github.com/cdcdx/hc-framework-go/common/model"
	"github.com/zeromicro/go-zero/core/logx"
)

// RecordsLogic 挂机历史记录（游标分页）
type RecordsLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewRecordsLogic(ctx context.Context, svcCtx *svc.ServiceContext) *RecordsLogic {
	return &RecordsLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *RecordsLogic) Records(in *hc.IdleRecordsRequest) (*hc.IdleRecordsResponse, error) {
	limit := int(in.Limit)
	if limit <= 0 {
		limit = 20
	}
	if limit > 100 {
		limit = 100
	}

	query := l.svcCtx.Db.Model(&model.IdleRecord{}).Where("user_id = ?", in.UserId)
	if in.Cursor > 0 {
		query = query.Where("id < ?", in.Cursor)
	}

	var recs []model.IdleRecord
	if err := query.Order("id DESC").Limit(limit + 1).Find(&recs).Error; err != nil {
		return nil, errorx.New(errorx.CodeDBError, err.Error())
	}

	hasMore := len(recs) > limit
	if hasMore {
		recs = recs[:limit]
	}

	out := make([]*hc.IdleRecord, 0, len(recs))
	var nextCursor int64
	for i := range recs {
		out = append(out, toIdleRecord(&recs[i]))
		nextCursor = recs[i].ID
	}

	return &hc.IdleRecordsResponse{
		Records:    out,
		NextCursor: nextCursor,
		HasMore:    hasMore,
	}, nil
}

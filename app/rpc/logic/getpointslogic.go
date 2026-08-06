package logic

import (
	"context"
	"encoding/json"
	"time"

	"github.com/cdcdx/hc-framework-go/app/rpc/hc"
	"github.com/cdcdx/hc-framework-go/app/rpc/svc"
	"github.com/cdcdx/hc-framework-go/common/errorx"
	"github.com/cdcdx/hc-framework-go/common/gormx"
	"github.com/cdcdx/hc-framework-go/common/model"
	"github.com/zeromicro/go-zero/core/logx"
)

// pointsBalanceKey 积分余额缓存 key。余额变动频繁但读多，使用较短 TTL（5s）。
func pointsBalanceKey(userID string) string {
	return "points:balance:" + userID
}

// GetPointsLogic 查询积分余额
type GetPointsLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewGetPointsLogic(ctx context.Context, svcCtx *svc.ServiceContext) *GetPointsLogic {
	return &GetPointsLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *GetPointsLogic) GetPoints(in *hc.GetPointsRequest) (*hc.GetPointsResponse, error) {
	key := pointsBalanceKey(in.UserId)
	cached, err := l.svcCtx.CachedGet(l.ctx, key, func(ctx context.Context) ([]byte, error) {
		var u model.User
		err := l.svcCtx.Db.Where("user_id = ?", in.UserId).First(&u).Error
		if gormx.IsRecordNotFound(err) {
			return nil, errorx.New(errorx.CodeNotFound, "user not found")
		}
		if err != nil {
			return nil, errorx.NewErr(errorx.CodeDBError, err)
		}
		return json.Marshal(&hc.GetPointsResponse{UserId: u.UserID, PointsBalance: u.PointsBalance})
	}, 5*time.Second)
	if err != nil {
		return nil, err
	}
	var resp hc.GetPointsResponse
	if err := json.Unmarshal(cached, &resp); err != nil {
		return nil, errorx.NewErr(errorx.CodeDBError, err)
	}
	return &resp, nil
}

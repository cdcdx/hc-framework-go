package logic

import (
	"context"
	"strings"

	"github.com/cdcdx/hc-framework-go/app/rpc/internal/svc"
	"github.com/cdcdx/hc-framework-go/app/rpc/hc"
	"github.com/cdcdx/hc-framework-go/common/errorx"
	"github.com/cdcdx/hc-framework-go/common/model"
	"github.com/zeromicro/go-zero/core/logx"
	"gorm.io/gorm"
)

// RefreshTokenLogic 刷新 Token
type RefreshTokenLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewRefreshTokenLogic(ctx context.Context, svcCtx *svc.ServiceContext) *RefreshTokenLogic {
	return &RefreshTokenLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *RefreshTokenLogic) RefreshToken(in *hc.RefreshTokenRequest) (*hc.RefreshTokenResponse, error) {
	if in.RefreshToken == "" {
		return nil, errorx.New(errorx.CodeTokenInvalid)
	}

	claims, err := l.svcCtx.JwtMgr.ValidateToken(in.RefreshToken)
	if err != nil {
		return nil, errorx.New(errorx.CodeTokenInvalid)
	}
	// 仅接受 refresh token（jti 带 refresh- 前缀），防止用 access token 刷新
	if !strings.HasPrefix(claims.ID, "refresh-") {
		return nil, errorx.New(errorx.CodeTokenInvalid)
	}

	var u model.User
	err = l.svcCtx.Db.Where("user_id = ?", claims.UserID).First(&u).Error
	if err == gorm.ErrRecordNotFound {
		return nil, errorx.New(errorx.CodeTokenInvalid)
	}
	if err != nil {
		return nil, errorx.New(errorx.CodeDBError, err.Error())
	}
	if u.Status != "active" {
		return nil, errorx.New(errorx.CodeAccountLocked)
	}

	access, refresh, err := l.svcCtx.JwtMgr.GenerateTokenPair(u.UserID, u.Email)
	if err != nil {
		return nil, errorx.New(errorx.CodeUnknownError, err.Error())
	}

	return &hc.RefreshTokenResponse{
		AccessToken:  access,
		RefreshToken: refresh,
	}, nil
}

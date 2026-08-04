package logic

import (
	"context"

	"github.com/cdcdx/hc-framework-go/app/rpc/svc"
	"github.com/cdcdx/hc-framework-go/app/rpc/hc"
	"github.com/cdcdx/hc-framework-go/common/bcrypt"
	"github.com/cdcdx/hc-framework-go/common/errorx"
	"github.com/cdcdx/hc-framework-go/common/model"
	"github.com/google/uuid"
	"github.com/zeromicro/go-zero/core/logx"
)

// RegisterLogic 邮箱注册
type RegisterLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewRegisterLogic(ctx context.Context, svcCtx *svc.ServiceContext) *RegisterLogic {
	return &RegisterLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *RegisterLogic) Register(in *hc.RegisterRequest) (*hc.RegisterResponse, error) {
	// 参数校验
	if in.Email == "" || in.Password == "" {
		return nil, errorx.New(errorx.CodeInvalidParam, "email and password are required")
	}
	if len(in.Password) < 8 {
		return nil, errorx.New(errorx.CodeInvalidParam, "password must be at least 8 characters")
	}

	// 邮箱唯一校验（并发下由 users.email 唯一索引兜底）
	var count int64
	if err := l.svcCtx.Db.Model(&model.User{}).Where("email = ?", in.Email).Count(&count).Error; err != nil {
		return nil, errorx.New(errorx.CodeDBError, err.Error())
	}
	if count > 0 {
		return nil, errorx.New(errorx.CodeEmailRegistered)
	}

	hash, err := bcrypt.Hash(in.Password, l.svcCtx.Config.BcryptCost)
	if err != nil {
		return nil, errorx.New(errorx.CodeUnknownError, err.Error())
	}

	username := in.Username
	if username == "" {
		username = in.Email
	}

	u := &model.User{
		UserID:       uuid.NewString(),
		Username:     username,
		Email:        in.Email,
		PasswordHash: hash,
		Status:       "active",
	}
	if err := l.svcCtx.Db.Create(u).Error; err != nil {
		return nil, errorx.New(errorx.CodeDBError, err.Error())
	}

	// 注册成功直接签发 token（与 gin 版一致，避免二次登录）
	access, refresh, err := l.svcCtx.JwtMgr.GenerateTokenPair(u.UserID, u.Email)
	if err != nil {
		return nil, errorx.New(errorx.CodeUnknownError, err.Error())
	}

	return &hc.RegisterResponse{
		User:         toUserInfo(u),
		AccessToken:  access,
		RefreshToken: refresh,
	}, nil
}

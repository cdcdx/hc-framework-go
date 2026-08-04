package logic

import (
	"context"

	"github.com/cdcdx/hc-framework-go/app/rpc/svc"
	"github.com/cdcdx/hc-framework-go/app/rpc/hc"
	"github.com/cdcdx/hc-framework-go/common/bcrypt"
	"github.com/cdcdx/hc-framework-go/common/errorx"
	"github.com/cdcdx/hc-framework-go/common/model"
	"github.com/zeromicro/go-zero/core/logx"
	"gorm.io/gorm"
)

// LoginLogic 邮箱密码登录
type LoginLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewLoginLogic(ctx context.Context, svcCtx *svc.ServiceContext) *LoginLogic {
	return &LoginLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *LoginLogic) Login(in *hc.LoginRequest) (*hc.LoginResponse, error) {
	if in.Email == "" || in.Password == "" {
		return nil, errorx.New(errorx.CodeInvalidParam, "email and password are required")
	}

	var u model.User
	err := l.svcCtx.Db.Where("email = ?", in.Email).First(&u).Error
	if err == gorm.ErrRecordNotFound {
		// 不泄露用户是否存在，统一报密码错误
		return nil, errorx.New(errorx.CodePasswordWrong)
	}
	if err != nil {
		return nil, errorx.New(errorx.CodeDBError, err.Error())
	}

	if u.Status != "active" {
		return nil, errorx.New(errorx.CodeAccountLocked)
	}
	if bcrypt.Compare(u.PasswordHash, in.Password) != nil {
		return nil, errorx.New(errorx.CodePasswordWrong)
	}

	access, refresh, err := l.svcCtx.JwtMgr.GenerateTokenPair(u.UserID, u.Email)
	if err != nil {
		return nil, errorx.New(errorx.CodeUnknownError, err.Error())
	}

	return &hc.LoginResponse{
		User:         toUserInfo(&u),
		AccessToken:  access,
		RefreshToken: refresh,
	}, nil
}

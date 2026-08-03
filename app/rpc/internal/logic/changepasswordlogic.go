package logic

import (
	"context"
	"time"

	"github.com/cdcdx/hc-framework-go/app/rpc/internal/svc"
	"github.com/cdcdx/hc-framework-go/app/rpc/hc"
	"github.com/cdcdx/hc-framework-go/common/bcrypt"
	"github.com/cdcdx/hc-framework-go/common/errorx"
	"github.com/cdcdx/hc-framework-go/common/model"
	"github.com/zeromicro/go-zero/core/logx"
	"gorm.io/gorm"
)

// ChangePasswordLogic 修改密码（校验旧密码，标记改密时间使已签发 token 失效）
type ChangePasswordLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewChangePasswordLogic(ctx context.Context, svcCtx *svc.ServiceContext) *ChangePasswordLogic {
	return &ChangePasswordLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *ChangePasswordLogic) ChangePassword(in *hc.ChangePasswordRequest) (*hc.ChangePasswordResponse, error) {
	if in.OldPassword == "" || in.NewPassword == "" {
		return nil, errorx.New(errorx.CodeInvalidParam, "old_password and new_password are required")
	}
	if len(in.NewPassword) < 8 {
		return nil, errorx.New(errorx.CodeInvalidParam, "new password must be at least 8 characters")
	}

	var u model.User
	err := l.svcCtx.Db.Where("user_id = ?", in.UserId).First(&u).Error
	if err == gorm.ErrRecordNotFound {
		return nil, errorx.New(errorx.CodeNotFound, "user not found")
	}
	if err != nil {
		return nil, errorx.New(errorx.CodeDBError, err.Error())
	}

	if bcrypt.Compare(u.PasswordHash, in.OldPassword) != nil {
		return nil, errorx.New(errorx.CodePasswordWrong, "old password incorrect")
	}

	hash, err := bcrypt.Hash(in.NewPassword, 0)
	if err != nil {
		return nil, errorx.New(errorx.CodeUnknownError, err.Error())
	}

	now := time.Now()
	if err := l.svcCtx.Db.Model(&u).Updates(map[string]any{
		"password_hash":       hash,
		"password_changed_at": now,
	}).Error; err != nil {
		return nil, errorx.New(errorx.CodeDBError, err.Error())
	}

	return &hc.ChangePasswordResponse{}, nil
}

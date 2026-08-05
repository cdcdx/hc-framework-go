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

// UpdateProfileLogic 更新用户资料
type UpdateProfileLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewUpdateProfileLogic(ctx context.Context, svcCtx *svc.ServiceContext) *UpdateProfileLogic {
	return &UpdateProfileLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *UpdateProfileLogic) UpdateProfile(in *hc.UpdateProfileRequest) (*hc.UpdateProfileResponse, error) {
	var u model.User
	err := l.svcCtx.Db.Where("user_id = ?", in.UserId).First(&u).Error
	if gormx.IsRecordNotFound(err) {
		return nil, errorx.New(errorx.CodeNotFound, "user not found")
	}
	if err != nil {
		return nil, errorx.NewErr(errorx.CodeDBError, err)
	}

	updates := map[string]any{}
	if in.Username != "" {
		if len(in.Username) > 50 {
			return nil, errorx.New(errorx.CodeInvalidParam, "nickname too long (max 50 characters)")
		}
		updates["username"] = in.Username
	}
	if in.AvatarUrl != "" {
		if len(in.AvatarUrl) > 500 {
			return nil, errorx.New(errorx.CodeInvalidParam, "avatar url too long (max 500 characters)")
		}
		updates["avatar_url"] = in.AvatarUrl
	}
	if len(updates) == 0 {
		return &hc.UpdateProfileResponse{User: toUserInfo(&u)}, nil
	}

	if err := l.svcCtx.Db.Model(&u).Updates(updates).Error; err != nil {
		return nil, errorx.NewErr(errorx.CodeDBError, err)
	}

	if err := l.svcCtx.Db.Where("user_id = ?", u.UserID).First(&u).Error; err != nil {
		return nil, errorx.NewErr(errorx.CodeDBError, err)
	}
	return &hc.UpdateProfileResponse{User: toUserInfo(&u)}, nil
}

package logic

import (
	"github.com/cdcdx/hc-framework-go/app/user/rpc/user"
	"github.com/cdcdx/hc-framework-go/common/model"
)

// toUserInfo 领域模型 → rpc 返回结构
func toUserInfo(u *model.User) *user.UserInfo {
	return &user.UserInfo{
		UserId:        u.UserID,
		Username:      u.Username,
		Email:         u.Email,
		AvatarUrl:     u.AvatarURL,
		PointsBalance: u.PointsBalance,
		Status:        u.Status,
		CreatedAt:     u.CreatedAt.Unix(),
	}
}

// Package types HTTP 请求类型（与 gateway.api 描述一致，goctl 生成等价物）
package types

type (
	RegisterReq struct {
		Email    string `json:"email"`
		Password string `json:"password"`
		Username string `json:"username,optional"`
	}

	LoginReq struct {
		Email    string `json:"email"`
		Password string `json:"password"`
	}

	GoogleOAuthReq struct {
		Code string `json:"code"`
	}

	RefreshTokenReq struct {
		RefreshToken string `json:"refresh_token"`
	}

	ChangePasswordReq struct {
		OldPassword string `json:"old_password"`
		NewPassword string `json:"new_password"`
	}

	UpdateProfileReq struct {
		Username  string `json:"username,optional"`
		AvatarURL string `json:"avatar,optional"`
	}

	IdleStartReq struct {
		DeviceID string `json:"device_id"`
	}

	IdleHeartbeatReq struct {
		DeviceID string `json:"device_id"`
	}

	IdleStopDeviceReq struct {
		DeviceID string `json:"device_id"`
	}

	IdleRecordsReq struct {
		Cursor int64 `form:"cursor,optional"`
		Limit  int   `form:"limit,default=20,optional"`
	}

	TaskClaimReq struct {
		ID int64 `path:"id"`
	}

	ShopItemsReq struct {
		Cursor   int64  `form:"cursor,optional"`
		Limit    int    `form:"limit,default=20,optional"`
		Category string `form:"category,optional"`
	}

	ShopRedeemReq struct {
		ItemID   int64 `json:"item_id"`
		Quantity int32 `json:"quantity,optional"`
	}

	ShopOrdersReq struct {
		Cursor int64 `form:"cursor,optional"`
		Limit  int   `form:"limit,default=20,optional"`
	}

	ShopOrderDetailReq struct {
		ID int64 `path:"id"`
	}

	FlashActivitiesReq struct {
		Limit int `form:"limit,default=20,optional"`
	}

	FlashRedeemReq struct {
		ActivityID int64 `json:"activity_id"`
	}
)

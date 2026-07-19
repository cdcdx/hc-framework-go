package handler

import (
	"github.com/cdcdx/hc-framework-go/internal/model"
	"github.com/cdcdx/hc-framework-go/internal/repository"
	"github.com/cdcdx/hc-framework-go/pkg/response"
	"github.com/gin-gonic/gin"
)

// UserHandler 用户处理器
type UserHandler struct {
	userRepo repository.UserRepository
}

// NewUserHandler 创建用户处理器
func NewUserHandler(userRepo repository.UserRepository) *UserHandler {
	return &UserHandler{
		userRepo: userRepo,
	}
}

// GetProfile 获取用户信息
// @Summary      获取用户信息
// @Description  获取当前登录用户的个人资料
// @Tags         用户
// @Security     BearerAuth
// @Produce      json
// @Success      200  {object}  map[string]interface{}
// @Failure      401  {object}  map[string]interface{}
// @Router       /api/v1/user/profile [get]
func (h *UserHandler) GetProfile(c *gin.Context) {
	userID, ok := requireUser(c)
	if !ok {
		return
	}

	user, err := h.userRepo.FindByID(c.Request.Context(), userID)
	if err != nil {
		response.Error(c, model.CodeDBError, err.Error())
		return
	}
	if user == nil {
		response.Error(c, model.CodeNotFound, "user not found")
		return
	}

	response.Success(c, user)
}

// UpdateProfile 更新用户信息
// @Summary      更新用户信息
// @Description  更新当前登录用户的个人资料
// @Tags         用户
// @Security     BearerAuth
// @Accept       json
// @Produce      json
// @Param        body  body      object{nickname=string,avatar=string}  true  "更新信息"
// @Success      200   {object}  map[string]interface{}
// @Failure      401   {object}  map[string]interface{}
// @Router       /api/v1/user/profile [put]
func (h *UserHandler) UpdateProfile(c *gin.Context) {
	userID, ok := requireUser(c)
	if !ok {
		return
	}

	user, err := h.userRepo.FindByID(c.Request.Context(), userID)
	if err != nil {
		response.Error(c, model.CodeDBError, err.Error())
		return
	}
	if user == nil {
		response.Error(c, model.CodeNotFound, "user not found")
		return
	}

	var req struct {
		Nickname string `json:"nickname"`
		Avatar   string `json:"avatar"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		response.BadRequest(c, "invalid request body")
		return
	}

	if req.Nickname != "" {
		if len(req.Nickname) > 50 {
			response.BadRequest(c, "nickname too long (max 50 characters)")
			return
		}
		user.Username = req.Nickname
	}
	if req.Avatar != "" {
		if len(req.Avatar) > 500 {
			response.BadRequest(c, "avatar url too long (max 500 characters)")
			return
		}
		user.AvatarURL = req.Avatar
	}

	if err := h.userRepo.Update(c.Request.Context(), user); err != nil {
		response.Error(c, model.CodeDBError, err.Error())
		return
	}

	response.Success(c, user)
}

// GetPoints 积分余额与流水
// @Summary      积分余额与流水
// @Description  查询当前用户的积分余额和积分变动记录
// @Tags         用户
// @Security     BearerAuth
// @Produce      json
// @Success      200  {object}  map[string]interface{}
// @Failure      401  {object}  map[string]interface{}
// @Router       /api/v1/user/points [get]
func (h *UserHandler) GetPoints(c *gin.Context) {
	userID, ok := requireUser(c)
	if !ok {
		return
	}

	user, err := h.userRepo.FindByID(c.Request.Context(), userID)
	if err != nil {
		response.Error(c, model.CodeDBError, err.Error())
		return
	}
	if user == nil {
		response.Error(c, model.CodeNotFound, "user not found")
		return
	}

	response.Success(c, gin.H{
		"user_id":        user.UserID,
		"points_balance": user.PointsBalance,
	})
}

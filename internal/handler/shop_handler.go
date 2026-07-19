package handler

import (
	"context"
	"errors"
	"strconv"

	"github.com/cdcdx/hc-framework-go/internal/model"
	"github.com/cdcdx/hc-framework-go/internal/service/shop"
	"github.com/cdcdx/hc-framework-go/pkg/response"
	"github.com/gin-gonic/gin"
)

// ShopHandler 商城处理器
type ShopHandler struct {
	svc *shop.ShopService
}

// NewShopHandler 创建商城处理器
func NewShopHandler(svc *shop.ShopService) *ShopHandler {
	return &ShopHandler{svc: svc}
}

// Items 商品列表
// @Summary      商品列表
// @Description  获取商城所有可兑换商品列表（游标分页 + 分类筛选）
// @Tags         商城
// @Security     BearerAuth
// @Produce      json
// @Param        cursor    query     string  false  "游标"
// @Param        limit     query     int     false  "每页条数"
// @Param        category  query     string  false  "分类"
// @Success      200       {object}  map[string]interface{}
// @Failure      401       {object}  map[string]interface{}
// @Router       /api/v1/shop/items [get]
func (h *ShopHandler) Items(c *gin.Context) {
	if _, ok := requireUser(c); !ok {
		return
	}

	cursor, err := strconv.ParseInt(c.Query("cursor"), 10, 64)
	if c.Query("cursor") != "" && err != nil {
		response.BadRequest(c, "invalid cursor")
		return
	}
	limit, err := strconv.Atoi(c.DefaultQuery("limit", "20"))
	if err != nil {
		limit = 20
	}
	category := c.Query("category")

	items, nextCursor, hasMore, err := h.svc.Items(c.Request.Context(), cursor, limit, category)
	if err != nil {
		response.Error(c, model.CodeDBError, err.Error())
		return
	}

	response.SuccessPage(c, items, strconv.FormatInt(nextCursor, 10), hasMore)
}

// Redeem 积分兑换
// @Summary      积分兑换
// @Description  使用积分兑换指定商品（含乐观锁库存扣减）
// @Tags         商城
// @Security     BearerAuth
// @Accept       json
// @Produce      json
// @Param        body  body      object{item_id=int,quantity=int}  true  "兑换信息"
// @Success      200   {object}  map[string]interface{}
// @Failure      400   {object}  map[string]interface{}
// @Failure      401   {object}  map[string]interface{}
// @Router       /api/v1/shop/redeem [post]
func (h *ShopHandler) Redeem(c *gin.Context) {
	userID, ok := requireUser(c)
	if !ok {
		return
	}

	var req shop.RedeemRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		response.BadRequest(c, "invalid request body")
		return
	}

	order, err := h.svc.Redeem(c.Request.Context(), userID, &req)
	if err != nil {
		switch err {
		case shop.ErrItemNotFound:
			response.Error(c, model.CodeItemOffline)
		case shop.ErrStockInsufficient:
			response.Error(c, model.CodeStockInsufficient)
		case shop.ErrRedeemSoldOutPeak:
			response.Error(c, model.CodeRedeemSoldOutPeak)
		case shop.ErrPointsInsufficient:
			response.Error(c, model.CodePointsInsufficient)
		case shop.ErrUserNotFound:
			response.Error(c, model.CodeNotFound, "user not found")
		case shop.ErrRedeemConcurrent:
			// 同一用户并发兑换被分布式锁拦截：可重试（响应带 Retry-After），避免正常请求被直接丢弃。
			c.Header("Retry-After", "1")
			response.Error(c, model.CodeRedeemConcurrent, "concurrent redeem in progress, please retry later")
		default:
			response.Error(c, model.CodeUnknownError, err.Error())
		}
		return
	}

	response.Success(c, order)
}

// FlashActivities 进行中的抢购活动列表
// @Summary      抢购活动列表
// @Description  获取当前进行中的定时抢购活动（时间窗内、状态生效）
// @Tags         商城
// @Security     BearerAuth
// @Produce      json
// @Param        limit  query     int  false  "返回条数"
// @Success      200    {object}  map[string]interface{}
// @Failure      401    {object}  map[string]interface{}
// @Router       /api/v1/shop/flash/activities [get]
func (h *ShopHandler) FlashActivities(c *gin.Context) {
	if _, ok := requireUser(c); !ok {
		return
	}
	limit, err := strconv.Atoi(c.DefaultQuery("limit", "20"))
	if err != nil {
		limit = 20
	}
	acts, err := h.svc.FlashActivities(c.Request.Context(), limit)
	if err != nil {
		response.Error(c, model.CodeDBError, err.Error())
		return
	}
	response.Success(c, acts)
}

// FlashRedeem 定时抢购兑换
// @Summary      定时抢购
// @Description  在活动时间窗内抢购限量商品，先到先得；限量内入库兑换，超出限量直接拒绝不入库
// @Tags         商城
// @Security     BearerAuth
// @Accept       json
// @Produce      json
// @Param        body  body      object{activity_id=int}  true  "抢购活动ID"
// @Success      200   {object}  map[string]interface{}
// @Failure      400   {object}  map[string]interface{}
// @Failure      401   {object}  map[string]interface{}
// @Router       /api/v1/shop/flash/redeem [post]
func (h *ShopHandler) FlashRedeem(c *gin.Context) {
	userID, ok := requireUser(c)
	if !ok {
		return
	}

	var req struct {
		ActivityID int64 `json:"activity_id"`
	}
	if err := c.ShouldBindJSON(&req); err != nil || req.ActivityID <= 0 {
		response.BadRequest(c, "invalid request body")
		return
	}

	order, err := h.svc.FlashRedeem(c.Request.Context(), userID, req.ActivityID)
	if err != nil {
		// 尖峰期超时（写事务等待行锁/连接池超过 request_timeout deadline）单独剥离，不再混入
		// 10003 未知错误，避免掩盖真实故障。该错误必然伴随事务回滚、不会落库，客户端可安全
		// 重试（重试不会造成超卖）。错误体附带底层根因便于现场定位。
		if errors.Is(err, context.DeadlineExceeded) {
			response.Error(c, model.CodeFlashSaleTimeout, "抢购处理超时，请稍后重试: "+err.Error())
			return
		}
		switch err {
		case shop.ErrFlashSaleNotFound:
			response.Error(c, model.CodeNotFound, "flash sale activity not found")
		case shop.ErrFlashSaleNotStarted:
			response.Error(c, model.CodeFlashSaleNotStarted)
		case shop.ErrFlashSaleEnded:
			response.Error(c, model.CodeFlashSaleEnded)
		case shop.ErrFlashSaleSoldOut:
			response.Error(c, model.CodeFlashSaleSoldOut)
		case shop.ErrFlashSaleUserLimit:
			response.Error(c, model.CodeFlashSaleUserLimit)
		case shop.ErrItemNotFound:
			response.Error(c, model.CodeItemOffline)
		case shop.ErrPointsInsufficient:
			response.Error(c, model.CodePointsInsufficient)
		case shop.ErrUserNotFound:
			response.Error(c, model.CodeNotFound, "user not found")
		case shop.ErrRedeemConcurrent:
			// 同一用户并发抢购被分布式锁拦截：可重试（响应带 Retry-After）。
			c.Header("Retry-After", "1")
			response.Error(c, model.CodeRedeemConcurrent, "concurrent redeem in progress, please retry later")
		default:
			response.Error(c, model.CodeUnknownError, err.Error())
		}
		return
	}

	response.Success(c, order)
}

// ──────────────────────────────────────────────────────
// 以下为运营管理接口（需 X-Admin-Token，路由见 router.go /admin/flash）
// ──────────────────────────────────────────────────────

// ListFlashActivities 抢购活动列表（运营视角，含已结束全状态）
// @Summary      抢购活动列表(运营)
// @Tags         运营管理-抢购
// @Security     AdminToken
// @Produce      json
// @Param        limit  query     int  false  "返回条数"
// @Success      200    {object}  map[string]interface{}
// @Router       /api/v1/admin/flash/activities [get]
func (h *ShopHandler) ListFlashActivities(c *gin.Context) {
	limit, err := strconv.Atoi(c.DefaultQuery("limit", "50"))
	if err != nil || limit <= 0 {
		limit = 50
	}
	acts, err := h.svc.ListFlashActivities(c.Request.Context(), limit)
	if err != nil {
		response.Error(c, model.CodeDBError, err.Error())
		return
	}
	response.Success(c, acts)
}

// FlashActivityDetail 抢购活动详情（运营视角）
// @Summary      抢购活动详情(运营)
// @Tags         运营管理-抢购
// @Security     AdminToken
// @Produce      json
// @Param        id  path  int  true  "活动ID"
// @Success      200 {object}  map[string]interface{}
// @Router       /api/v1/admin/flash/activities/{id} [get]
func (h *ShopHandler) FlashActivityDetail(c *gin.Context) {
	id, err := strconv.ParseInt(c.Param("id"), 10, 64)
	if err != nil || id <= 0 {
		response.BadRequest(c, "invalid activity id")
		return
	}
	act, err := h.svc.FlashActivityDetail(c.Request.Context(), id)
	if err != nil {
		if err == shop.ErrFlashSaleNotFound {
			response.Error(c, model.CodeNotFound, "flash sale activity not found")
			return
		}
		response.Error(c, model.CodeDBError, err.Error())
		return
	}
	response.Success(c, act)
}

// CreateFlashActivity 创建抢购活动（运营）
// @Summary      创建抢购活动(运营)
// @Tags         运营管理-抢购
// @Security     AdminToken
// @Accept       json
// @Produce      json
// @Param        body  body  shop.FlashActivityInput  true  "活动配置"
// @Success      200   {object}  map[string]interface{}
// @Router       /api/v1/admin/flash/activities [post]
func (h *ShopHandler) CreateFlashActivity(c *gin.Context) {
	var in shop.FlashActivityInput
	if err := c.ShouldBindJSON(&in); err != nil {
		response.BadRequest(c, "invalid request body")
		return
	}
	act, err := h.svc.CreateFlashActivity(c.Request.Context(), &in)
	if err != nil {
		response.BadRequest(c, err.Error())
		return
	}
	response.Success(c, act)
}

// UpdateFlashActivity 更新抢购活动（运营）
// @Summary      更新抢购活动(运营)
// @Tags         运营管理-抢购
// @Security     AdminToken
// @Accept       json
// @Produce      json
// @Param        id   path  int                  true  "活动ID"
// @Param        body body  shop.FlashActivityInput  true  "活动配置"
// @Success      200   {object}  map[string]interface{}
// @Router       /api/v1/admin/flash/activities/{id} [put]
func (h *ShopHandler) UpdateFlashActivity(c *gin.Context) {
	id, err := strconv.ParseInt(c.Param("id"), 10, 64)
	if err != nil || id <= 0 {
		response.BadRequest(c, "invalid activity id")
		return
	}
	var in shop.FlashActivityInput
	if err := c.ShouldBindJSON(&in); err != nil {
		response.BadRequest(c, "invalid request body")
		return
	}
	act, err := h.svc.UpdateFlashActivity(c.Request.Context(), id, &in)
	if err != nil {
		switch err {
		case shop.ErrFlashSaleNotFound:
			response.Error(c, model.CodeNotFound, "flash sale activity not found")
		default:
			response.BadRequest(c, err.Error())
		}
		return
	}
	response.Success(c, act)
}

// EndFlashActivity 下架/结束抢购活动（运营）
// @Summary      结束抢购活动(运营)
// @Tags         运营管理-抢购
// @Security     AdminToken
// @Produce      json
// @Param        id  path  int  true  "活动ID"
// @Success      200 {object}  map[string]interface{}
// @Router       /api/v1/admin/flash/activities/{id}/end [post]
func (h *ShopHandler) EndFlashActivity(c *gin.Context) {
	id, err := strconv.ParseInt(c.Param("id"), 10, 64)
	if err != nil || id <= 0 {
		response.BadRequest(c, "invalid activity id")
		return
	}
	act, err := h.svc.EndFlashActivity(c.Request.Context(), id)
	if err != nil {
		switch err {
		case shop.ErrFlashSaleNotFound:
			response.Error(c, model.CodeNotFound, "flash sale activity not found")
		default:
			response.Error(c, model.CodeDBError, err.Error())
		}
		return
	}
	response.Success(c, act)
}

// WarmupFlashSale 手动预热/对齐 Redis 库存（运营）
// @Summary      预热抢购库存(运营)
// @Tags         运营管理-抢购
// @Security     AdminToken
// @Produce      json
// @Param        id  path  int  true  "活动ID"
// @Success      200 {object}  map[string]interface{}
// @Router       /api/v1/admin/flash/activities/{id}/warmup [post]
func (h *ShopHandler) WarmupFlashSale(c *gin.Context) {
	id, err := strconv.ParseInt(c.Param("id"), 10, 64)
	if err != nil || id <= 0 {
		response.BadRequest(c, "invalid activity id")
		return
	}
	if err := h.svc.WarmupFlashSale(c.Request.Context(), id); err != nil {
		response.Error(c, model.CodeDBError, err.Error())
		return
	}
	response.Success(c, gin.H{"activity_id": id, "warmed": true})
}

// SyncFlashSaleStock 库存对账（运营，事故后修复）
// @Summary      抢购库存对账(运营)
// @Tags         运营管理-抢购
// @Security     AdminToken
// @Produce      json
// @Param        id  path  int  true  "活动ID"
// @Success      200 {object}  map[string]interface{}
// @Router       /api/v1/admin/flash/activities/{id}/sync [post]
func (h *ShopHandler) SyncFlashSaleStock(c *gin.Context) {
	id, err := strconv.ParseInt(c.Param("id"), 10, 64)
	if err != nil || id <= 0 {
		response.BadRequest(c, "invalid activity id")
		return
	}
	if err := h.svc.SyncFlashSaleStock(c.Request.Context(), id); err != nil {
		response.Error(c, model.CodeDBError, err.Error())
		return
	}
	response.Success(c, gin.H{"activity_id": id, "synced": true})
}

// ReconcileItemStock 普通商品库存对账（运营，重置/事故后修复 Redis 预扣计数器）
// @Summary      普通商品库存对账(运营)
// @Tags         运营管理-商城
// @Security     AdminToken
// @Produce      json
// @Param        id  path  int  true  "商品ID"
// @Success      200 {object}  map[string]interface{}
// @Router       /api/v1/admin/shop/items/{id}/reconcile [post]
func (h *ShopHandler) ReconcileItemStock(c *gin.Context) {
	id, err := strconv.ParseInt(c.Param("id"), 10, 64)
	if err != nil || id <= 0 {
		response.BadRequest(c, "invalid item id")
		return
	}
	if err := h.svc.ReconcileItemRedeemStock(c.Request.Context(), id); err != nil {
		response.Error(c, model.CodeDBError, err.Error())
		return
	}
	response.Success(c, gin.H{"item_id": id, "synced": true})
}

// Orders 兑换订单记录
// @Summary      兑换记录
// @Description  分页查询当前用户的兑换订单列表（游标分页）
// @Tags         商城
// @Security     BearerAuth
// @Produce      json
// @Param        cursor  query     string  false  "游标"
// @Param        limit   query     int     false  "每页条数"
// @Success      200     {object}  map[string]interface{}
// @Failure      401     {object}  map[string]interface{}
// @Router       /api/v1/shop/orders [get]
func (h *ShopHandler) Orders(c *gin.Context) {
	userID, ok := requireUser(c)
	if !ok {
		return
	}

	cursor, err := strconv.ParseInt(c.Query("cursor"), 10, 64)
	if c.Query("cursor") != "" && err != nil {
		response.BadRequest(c, "invalid cursor")
		return
	}
	limit, err := strconv.Atoi(c.DefaultQuery("limit", "20"))
	if err != nil {
		limit = 20
	}

	orders, nextCursor, hasMore, err := h.svc.Orders(c.Request.Context(), userID, cursor, limit)
	if err != nil {
		response.Error(c, model.CodeDBError, err.Error())
		return
	}

	response.SuccessPage(c, orders, strconv.FormatInt(nextCursor, 10), hasMore)
}

// OrderDetail 订单详情
// @Summary      订单详情
// @Description  查询指定订单的详细信息
// @Tags         商城
// @Security     BearerAuth
// @Produce      json
// @Param        id   path      int  true  "订单ID"
// @Success      200  {object}  map[string]interface{}
// @Failure      401  {object}  map[string]interface{}
// @Failure      404  {object}  map[string]interface{}
// @Router       /api/v1/shop/orders/{id} [get]
func (h *ShopHandler) OrderDetail(c *gin.Context) {
	userID, ok := requireUser(c)
	if !ok {
		return
	}

	orderID, err := strconv.ParseInt(c.Param("id"), 10, 64)
	if err != nil {
		response.BadRequest(c, "invalid order id")
		return
	}

	order, err := h.svc.OrderDetail(c.Request.Context(), orderID)
	if err != nil {
		if err == shop.ErrOrderNotFound {
			response.Error(c, model.CodeNotFound, "order not found")
			return
		}
		response.Error(c, model.CodeDBError, err.Error())
		return
	}

	// 验证订单归属
	if order.UserID != userID {
		response.Forbidden(c, model.CodePermissionDenied)
		return
	}

	response.Success(c, order)
}

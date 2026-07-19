package handler

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/cdcdx/hc-framework-go/internal/model"
	"github.com/gin-gonic/gin"
)

// ginEngine 仅为 *gin.Engine 的便捷别名，避免本文件内重复书写。
type ginEngine = gin.Engine

func postJSON(r *ginEngine, path string, payload interface{}) *httptest.ResponseRecorder {
	body, _ := json.Marshal(payload)
	req := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	return w
}

// TestShopHandler_Unauthorized 验证未鉴权时 items / redeem 均返回 401。
func TestShopHandler_Unauthorized(t *testing.T) {
	svc, _ := newTestShop(t)
	h := NewShopHandler(svc)
	r := authedEngine("", func(r *ginEngine) {
		r.GET("/api/v1/shop/items", h.Items)
		r.POST("/api/v1/shop/redeem", h.Redeem)
	})

	w1 := httptest.NewRecorder()
	r.ServeHTTP(w1, httptest.NewRequest(http.MethodGet, "/api/v1/shop/items", nil))
	if codeOf(t, w1) != model.CodeTokenInvalid {
		t.Fatalf("items code = %v, want %d", codeOf(t, w1), model.CodeTokenInvalid)
	}

	w2 := postJSON(r, "/api/v1/shop/redeem", map[string]interface{}{"item_id": 1})
	if codeOf(t, w2) != model.CodeTokenInvalid {
		t.Fatalf("redeem code = %v, want %d", codeOf(t, w2), model.CodeTokenInvalid)
	}
}

// TestShopHandler_Redeem_MalformedBody 验证非法 JSON 返回 400。
func TestShopHandler_Redeem_MalformedBody(t *testing.T) {
	svc, _ := newTestShop(t)
	h := NewShopHandler(svc)
	r := authedEngine("u1", func(r *ginEngine) {
		r.POST("/api/v1/shop/redeem", h.Redeem)
	})
	req := httptest.NewRequest(http.MethodPost, "/api/v1/shop/redeem", bytes.NewReader([]byte("not-json")))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", w.Code)
	}
}

// TestShopHandler_Redeem_ItemNotFound 验证下架/不存在商品返回 10303。
func TestShopHandler_Redeem_ItemNotFound(t *testing.T) {
	svc, gdb := newTestShop(t)
	seedUser(t, gdb, "u1", 1000)
	h := NewShopHandler(svc)
	r := authedEngine("u1", func(r *ginEngine) {
		r.POST("/api/v1/shop/redeem", h.Redeem)
	})
	w := postJSON(r, "/api/v1/shop/redeem", map[string]interface{}{"item_id": 999999, "quantity": 1})
	if codeOf(t, w) != model.CodeItemOffline {
		t.Fatalf("code = %v, want %d (item offline/not found)", codeOf(t, w), model.CodeItemOffline)
	}
}

// TestShopHandler_Redeem_StockInsufficient 验证库存不足返回 10302。
func TestShopHandler_Redeem_StockInsufficient(t *testing.T) {
	svc, gdb := newTestShop(t)
	seedUser(t, gdb, "u1", 1000)
	item := &model.ShopItem{Name: "S", PricePoints: 100, Stock: 1, Version: 0, Category: "c", IsActive: true}
	gdb.Create(item)
	h := NewShopHandler(svc)
	r := authedEngine("u1", func(r *ginEngine) {
		r.POST("/api/v1/shop/redeem", h.Redeem)
	})
	w := postJSON(r, "/api/v1/shop/redeem", map[string]interface{}{"item_id": item.ID, "quantity": 5})
	if codeOf(t, w) != model.CodeStockInsufficient {
		t.Fatalf("code = %v, want %d", codeOf(t, w), model.CodeStockInsufficient)
	}
}

// TestShopHandler_Redeem_PointsInsufficient 验证积分不足返回 10301。
func TestShopHandler_Redeem_PointsInsufficient(t *testing.T) {
	svc, gdb := newTestShop(t)
	seedUser(t, gdb, "u1", 10)
	item := &model.ShopItem{Name: "S", PricePoints: 100, Stock: 10, Version: 0, Category: "c", IsActive: true}
	gdb.Create(item)
	h := NewShopHandler(svc)
	r := authedEngine("u1", func(r *ginEngine) {
		r.POST("/api/v1/shop/redeem", h.Redeem)
	})
	w := postJSON(r, "/api/v1/shop/redeem", map[string]interface{}{"item_id": item.ID, "quantity": 1})
	if codeOf(t, w) != model.CodePointsInsufficient {
		t.Fatalf("code = %v, want %d", codeOf(t, w), model.CodePointsInsufficient)
	}
}

// TestShopHandler_Redeem_Success 验证兑换成功返回订单与正确积分消耗。
func TestShopHandler_Redeem_Success(t *testing.T) {
	svc, gdb := newTestShop(t)
	seedUser(t, gdb, "u1", 1000)
	item := &model.ShopItem{Name: "S", PricePoints: 100, Stock: 10, Version: 0, Category: "c", IsActive: true}
	gdb.Create(item)
	h := NewShopHandler(svc)
	r := authedEngine("u1", func(r *ginEngine) {
		r.POST("/api/v1/shop/redeem", h.Redeem)
	})
	w := postJSON(r, "/api/v1/shop/redeem", map[string]interface{}{"item_id": item.ID, "quantity": 2})
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body=%s)", w.Code, w.Body.String())
	}
	var body map[string]interface{}
	_ = json.Unmarshal(w.Body.Bytes(), &body)
	data := body["data"].(map[string]interface{})
	if int64(data["points_spent"].(float64)) != 200 {
		t.Fatalf("points_spent = %v, want 200", data["points_spent"])
	}
}

// TestShopHandler_Redeem_UserNotFound 验证用户不存在时返回 10002。
func TestShopHandler_Redeem_UserNotFound(t *testing.T) {
	svc, _ := newTestShop(t)
	// 不 seed 用户 u1
	h := NewShopHandler(svc)
	r := authedEngine("u1", func(r *ginEngine) {
		r.POST("/api/v1/shop/redeem", h.Redeem)
	})
	w := postJSON(r, "/api/v1/shop/redeem", map[string]interface{}{"item_id": 1, "quantity": 1})
	if codeOf(t, w) != model.CodeNotFound {
		t.Fatalf("code = %v, want %d (user not found)", codeOf(t, w), model.CodeNotFound)
	}
}

// TestShopHandler_OrderDetail_Forbidden 验证查询他人订单返回 10103。
func TestShopHandler_OrderDetail_Forbidden(t *testing.T) {
	svc, gdb := newTestShop(t)
	seedUser(t, gdb, "u1", 1000)
	seedUser(t, gdb, "u2", 1000)
	order := &model.RedeemOrder{UserID: "u1", ItemID: 1, ItemName: "x", PointsSpent: 100, OrderStatus: "completed"}
	gdb.Create(order)
	h := NewShopHandler(svc)
	r := authedEngine("u2", func(r *ginEngine) {
		r.GET("/api/v1/shop/orders/:id", h.OrderDetail)
	})
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/v1/shop/orders/1", nil))
	if codeOf(t, w) != model.CodePermissionDenied {
		t.Fatalf("code = %v, want %d (forbidden)", codeOf(t, w), model.CodePermissionDenied)
	}
}

// TestShopHandler_OrderDetail_NotFound 验证订单不存在返回 10002。
func TestShopHandler_OrderDetail_NotFound(t *testing.T) {
	svc, gdb := newTestShop(t)
	seedUser(t, gdb, "u1", 1000)
	h := NewShopHandler(svc)
	r := authedEngine("u1", func(r *ginEngine) {
		r.GET("/api/v1/shop/orders/:id", h.OrderDetail)
	})
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/v1/shop/orders/999999", nil))
	if codeOf(t, w) != model.CodeNotFound {
		t.Fatalf("code = %v, want %d (order not found)", codeOf(t, w), model.CodeNotFound)
	}
}

// TestShopHandler_Items_InvalidCursor 验证非法游标返回 400。
func TestShopHandler_Items_InvalidCursor(t *testing.T) {
	svc, _ := newTestShop(t)
	h := NewShopHandler(svc)
	r := authedEngine("u1", func(r *ginEngine) {
		r.GET("/api/v1/shop/items", h.Items)
	})
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/v1/shop/items?cursor=abc", nil))
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 (invalid cursor)", w.Code)
	}
}

// TestShopHandler_Orders_Success 验证兑换后订单列表可查。
func TestShopHandler_Orders_Success(t *testing.T) {
	svc, gdb := newTestShop(t)
	seedUser(t, gdb, "u1", 1000)
	item := &model.ShopItem{Name: "S", PricePoints: 100, Stock: 10, Version: 0, Category: "c", IsActive: true}
	gdb.Create(item)
	h := NewShopHandler(svc)
	r := authedEngine("u1", func(r *ginEngine) {
		r.POST("/api/v1/shop/redeem", h.Redeem)
		r.GET("/api/v1/shop/orders", h.Orders)
	})
	postJSON(r, "/api/v1/shop/redeem", map[string]interface{}{"item_id": item.ID, "quantity": 1})

	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/v1/shop/orders", nil))
	if w.Code != http.StatusOK {
		t.Fatalf("orders status = %d, want 200", w.Code)
	}
	var body map[string]interface{}
	_ = json.Unmarshal(w.Body.Bytes(), &body)
	data := body["data"].(map[string]interface{})
	items := data["items"].([]interface{})
	if len(items) != 1 {
		t.Fatalf("orders len = %d, want 1", len(items))
	}
}

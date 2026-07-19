package model

import "time"

// ShopItem 商品表
type ShopItem struct {
	ID          int64     `json:"id" gorm:"primaryKey;autoIncrement"`
	Name        string    `json:"name" gorm:"not null;default:'';type:varchar(191)"`
	Description string    `json:"description" gorm:"type:text"`
	PricePoints int64     `json:"price_points" gorm:"default:0"`
	Stock       int       `json:"stock" gorm:"default:0"`
	Version     int       `json:"version" gorm:"default:0"` // 乐观锁版本号
	ImageURL    string    `json:"image_url" gorm:"type:text"`
	Category    string    `json:"category" gorm:"index;default:'';type:varchar(100)"`
	IsActive    bool      `json:"is_active" gorm:"index;default:true"`
	SalesStatus string    `json:"sales_status" gorm:"-"` // SalesStatus 派生销售状态（不落库，仅供展示）：offline 下架 / on_sale 销售中 / sold_out 已卖完
	CreatedAt   time.Time `json:"created_at" gorm:"autoCreateTime"`
	UpdatedAt   time.Time `json:"updated_at" gorm:"autoUpdateTime"`
}

func (ShopItem) TableName() string {
	return "shop_items"
}

// ShopItemStockBucket 库存分桶表（P2-5 根治写热点）。
//
// 普通兑换原本对 shop_items 单行做原子条件扣减（WHERE stock>=quantity），所有并发兑换串行于
// 同一行，形成行锁热点。分桶后把单商品库存拆到 N 个桶行（默认 16），兑换按 hash(userID)%N 选桶
// 做条件扣减，把写竞争分散到 N 行，显著降低抢兑高峰的行锁等待（见 docs）。
//
// 桶是库存的权威存储；shop_items.stock 仅作展示/兜底（FindItemByID/FindItems 会优先返回桶之和）。
// 任一桶成功扣减即返回，主桶不足时按顺序尝试其余桶，避免桶间库存倾斜导致的“假售罄”。
// 无桶商品（直建/未回填）走回退单行语义，保持 item.Stock 准确。
type ShopItemStockBucket struct {
	ItemID int64 `gorm:"column:item_id;primaryKey"`
	Bucket int   `gorm:"column:bucket;primaryKey"`
	Stock  int   `gorm:"column:stock;not null;default:0"`
}

func (ShopItemStockBucket) TableName() string {
	return "shop_item_stock_buckets"
}

// 普通商品销售状态（派生，不落库，仅供展示）
const (
	ItemSalesStatusOffline = "offline"  // 已下架
	ItemSalesStatusOnSale  = "on_sale"  // 销售中（可购买）
	ItemSalesStatusSoldOut = "sold_out" // 已卖完
)

// SalesStatus 根据权威字段（IsActive / Stock）派生销售状态。
//
// 重要：该状态“仅用于展示”，库存扣减与防超卖由 repository.DeductStock 的原子条件更新
// （WHERE stock>=quantity，行锁串行）兜底，绝不依赖此派生状态做放行判断——避免缓存/派生值
// 滞后（如 stock 已被并发扣到 0 但本状态仍为 on_sale）而误判放行导致超卖。
func (it ShopItem) SalesStatusValue() string {
	if !it.IsActive {
		return ItemSalesStatusOffline
	}
	if it.Stock <= 0 {
		return ItemSalesStatusSoldOut
	}
	return ItemSalesStatusOnSale
}

// RedeemOrder 兑换订单表
type RedeemOrder struct {
	ID          int64  `json:"id" gorm:"primaryKey;autoIncrement"`
	UserID      string `json:"user_id" gorm:"index:idx_user_created;not null;type:varchar(191)"`
	ItemID      int64  `json:"item_id" gorm:"index;not null"`
	ItemName    string `json:"item_name" gorm:"not null;default:'';type:varchar(191)"`
	PointsSpent int64  `json:"points_spent" gorm:"default:0"`
	OrderStatus string `json:"order_status" gorm:"default:completed;type:varchar(20)"` // pending / completed / cancelled
	// ActivityID 关联的抢购活动 ID（0 表示普通兑换，非抢购）。
	// 与 (activity_id, user_id) 联合索引配合，支撑抢购每人限购的 DB 权威校验。
	ActivityID int64     `json:"activity_id" gorm:"index:idx_activity_user,priority:1;not null;default:0"`
	CreatedAt  time.Time `json:"created_at" gorm:"index:idx_activity_user,priority:2;autoCreateTime"`
}

func (RedeemOrder) TableName() string {
	return "redeem_orders"
}

// 抢购活动状态常量
const (
	FlashSaleStatusActive  = "active"  // 生效中（到点即可抢）
	FlashSaleStatusEnded   = "ended"   // 已结束/下架
	FlashSaleStatusPending = "pending" // 未生效（预热中）
)

// ShopFlashActivity 商品定时抢购活动表。
//
// 语义：在 [StartTime, EndTime] 时间窗内开放对 ItemID 商品的抢购，全场限量 LimitQty 个，
// 先到先得；每个用户最多抢 PerUserLimit 个。抢购的限量由本表的 SoldQty/LimitQty 独立管理，
// 与 ShopItem.Stock 解耦（一个商品可挂多场不同限量的抢购）。
//
// 防超卖两道防线：
//   - Redis 预扣（削峰快速拦截，见 cache.Manager.TryAcquireFlashSale，可选）；
//   - DB 条件更新兜底（权威）：UPDATE ... SET sold_qty=sold_qty+1 WHERE id=? AND sold_qty<limit_qty，
//     RowsAffected==0 即售罄。行锁天然串行，绝不超卖。
type ShopFlashActivity struct {
	ID           int64      `json:"id" gorm:"primaryKey;autoIncrement"`
	ItemID       int64      `json:"item_id" gorm:"index;not null"`
	Name         string     `json:"name" gorm:"not null;default:'';type:varchar(191)"`
	StartTime    time.Time  `json:"start_time" gorm:"index;not null"`         // 开放抢购时间
	EndTime      *time.Time `json:"end_time"`                                 // 结束时间；nil 表示不限结束（存为 NULL，规避 MySQL NO_ZERO_DATE 拒绝零值）
	LimitQty     int        `json:"limit_qty" gorm:"not null;default:0"`      // 全场限量总数
	SoldQty      int        `json:"sold_qty" gorm:"not null;default:0"`       // 已抢数量（DB 权威计数）
	PerUserLimit int        `json:"per_user_limit" gorm:"not null;default:1"` // 每人限购（<=0 视为 1）
	PricePoints  int64      `json:"price_points" gorm:"not null;default:0"`   // 抢购价（积分），覆盖商品原价
	Status       string     `json:"status" gorm:"index;default:'active';type:varchar(20)"`
	SalesStatus  string     `json:"sales_status" gorm:"-"` // SalesStatus 派生销售状态（不落库，仅供展示）：presale 预售中 / on_sale 销售中 / sold_out 已卖完 / ended 已结束
	CreatedAt    time.Time  `json:"created_at" gorm:"autoCreateTime"`
	UpdatedAt    time.Time  `json:"updated_at" gorm:"autoUpdateTime"`
}

func (ShopFlashActivity) TableName() string {
	return "shop_flash_activities"
}

// 抢购活动销售状态（派生，不落库，仅供展示）
const (
	FlashSaleSalesStatusPresale = "presale"  // 预售中（定时前，尚未开抢）
	FlashSaleSalesStatusOnSale  = "on_sale"  // 销售中（定时后、时间窗内、未卖完）
	FlashSaleSalesStatusSoldOut = "sold_out" // 已卖完（sold_qty >= limit_qty）
	FlashSaleSalesStatusEnded   = "ended"    // 已结束/下架
)

// SalesStatus 根据权威字段（Status / StartTime / EndTime / SoldQty / LimitQty）与时间窗派生销售状态。
//
// 重要：该状态“仅用于展示”，抢购配额的扣减与防超卖由 repository.AcquireFlashQuota 的原子条件更新
// （WHERE sold_qty<limit_qty，行锁串行）兜底，绝不依赖此派生状态做放行判断——避免 Redis/DB 计数
// 漂移导致状态误判而放行超卖。
func (act ShopFlashActivity) SalesStatusValue() string {
	now := time.Now()
	switch {
	case act.Status == FlashSaleStatusEnded:
		return FlashSaleSalesStatusEnded
	case act.EndTime != nil && now.After(*act.EndTime):
		return FlashSaleSalesStatusEnded
	case act.Status == FlashSaleStatusPending || now.Before(act.StartTime):
		return FlashSaleSalesStatusPresale
	case act.SoldQty >= act.LimitQty:
		return FlashSaleSalesStatusSoldOut
	default:
		return FlashSaleSalesStatusOnSale
	}
}

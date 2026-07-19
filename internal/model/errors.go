// Package model 领域模型与数据库表结构定义。
package model

// ErrorCode 错误码定义
const (
	// 成功
	CodeSuccess = 0

	// 通用错误 10000-10099
	CodeInvalidParam       = 10001
	CodeNotFound           = 10002
	CodeUnknownError       = 10003
	CodeGatewayTimeout     = 10004 // 请求处理超时（内部超时，区别于网关 504）
	CodeServiceUnavailable = 10005 // 服务过载：应用层并发达上限，负载卸载（load shedding）快速失败

	// 认证授权 10100-10199
	CodeTokenExpired     = 10101
	CodeTokenInvalid     = 10102
	CodePermissionDenied = 10103
	CodeTokenBlacklisted = 10104

	// 用户相关 10200-10299
	CodeEmailRegistered = 10201
	CodePasswordWrong   = 10202
	CodeAccountLocked   = 10203
	CodeAccountDisabled = 10204
	CodeCaptchaRequired = 10205

	// 积分/兑换 10300-10399
	CodePointsInsufficient  = 10301
	CodeStockInsufficient   = 10302
	CodeItemOffline         = 10303
	CodeDuplicateRedeem     = 10304
	CodeFlashSaleNotStarted = 10305 // 抢购未开始
	CodeFlashSaleEnded      = 10306 // 抢购已结束
	CodeFlashSaleSoldOut    = 10307 // 已抢光（超出限量，直接拒绝不入库）
	CodeFlashSaleUserLimit  = 10308 // 超出每人限购
	CodeFlashSaleTimeout    = 10309 // 抢购处理超时（尖峰期行锁/连接池等待超过请求 deadline，事务已回滚，请重试）
	CodeRedeemConcurrent    = 10310 // 同一用户并发兑换被分布式锁拦截，可重试（响应带 Retry-After）
	CodeRedeemSoldOutPeak   = 10311 // 普通商品：削峰层（Redis 预扣）已售罄拦截，区别于 DB 行锁售罄(10302)

	// 挂机相关 10400-10499
	CodeAlreadyIdle      = 10401
	CodeNotIdle          = 10402
	CodeKickedOffline    = 10403
	CodeHeartbeatTimeout = 10404
	CodeDailyPointsLimit = 10405

	// 任务相关 10500-10599
	CodeTaskNotCompleted     = 10501
	CodeTaskClaimed          = 10502
	CodeTaskExpired          = 10503
	CodeProgressInsufficient = 10504

	// 限流/熔断 10600-10699
	CodeRateLimited   = 10601
	CodeCircuitOpen   = 10602
	CodeCaptchaVerify = 10603

	// 系统内部错误 10700-10799
	CodeDBError         = 10701
	CodeRedisError      = 10702
	CodeKafkaError      = 10703
	CodeThirdPartyError = 10704
)

// ErrorMessages 错误码 → 消息映射
var ErrorMessages = map[int]string{
	CodeSuccess:              "success",
	CodeInvalidParam:         "参数校验失败",
	CodeNotFound:             "资源不存在",
	CodeUnknownError:         "未知错误",
	CodeTokenExpired:         "Token 已过期",
	CodeTokenInvalid:         "Token 无效",
	CodePermissionDenied:     "权限不足",
	CodeTokenBlacklisted:     "Token 已失效",
	CodeEmailRegistered:      "邮箱已注册",
	CodePasswordWrong:        "密码错误",
	CodeAccountLocked:        "账号已锁定",
	CodeAccountDisabled:      "账号已禁用",
	CodeCaptchaRequired:      "需要验证码",
	CodePointsInsufficient:   "积分不足",
	CodeStockInsufficient:    "库存不足",
	CodeItemOffline:          "商品已下架",
	CodeDuplicateRedeem:      "重复兑换",
	CodeFlashSaleNotStarted:  "抢购未开始",
	CodeFlashSaleEnded:       "抢购已结束",
	CodeFlashSaleSoldOut:     "已抢光，来晚啦",
	CodeFlashSaleUserLimit:   "已达每人限购数量",
	CodeFlashSaleTimeout:     "抢购处理超时，请稍后重试",
	CodeRedeemConcurrent:     "兑换处理中，请稍后重试",
	CodeRedeemSoldOutPeak:    "已售罄（削峰拦截）",
	CodeAlreadyIdle:          "已在挂机中",
	CodeNotIdle:              "未在挂机",
	CodeKickedOffline:        "已被踢下线",
	CodeHeartbeatTimeout:     "心跳超时",
	CodeDailyPointsLimit:     "已达每日积分上限",
	CodeTaskNotCompleted:     "任务未完成",
	CodeTaskClaimed:          "已领取",
	CodeTaskExpired:          "任务已过期",
	CodeProgressInsufficient: "进度不足",
	CodeRateLimited:          "请求过于频繁",
	CodeCircuitOpen:          "服务降级中",
	CodeCaptchaVerify:        "需通过验证码",
	CodeDBError:              "数据库异常",
	CodeRedisError:           "Redis 异常",
	CodeKafkaError:           "Kafka 异常",
	CodeThirdPartyError:      "第三方服务异常",
	CodeGatewayTimeout:       "请求处理超时",
	CodeServiceUnavailable:   "服务繁忙，请稍后重试",
}

// Message 获取错误码对应的消息
func Message(code int) string {
	if msg, ok := ErrorMessages[code]; ok {
		return msg
	}
	return "未知错误"
}

// Package errorx 业务错误码与跨 rpc 错误传递。
// 错误码定义与 gin 版完全一致，通过 google.golang.org/grpc/status 编码
// 进 grpc status（code=业务码, message=中文消息），网关侧用 Code()/Msg() 还原。
package errorx

import (
	"fmt"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// 业务错误码（与 gin 版保持一致，数值不可变以保证兼容性）。
// 每个段独立一个 const 块，使用 iota 自增避免人工重复；
// 段内追加新码只需在末尾加一行，无需手动调整偏移。
const (
	// 成功
	CodeSuccess = 0
)

// 通用错误 10000-10099
const (
	_                      = iota + 10000 // 10000 占位
	CodeInvalidParam                      // 10001
	CodeNotFound                          // 10002
	CodeUnknownError                      // 10003
	CodeGatewayTimeout                    // 10004
	CodeServiceUnavailable                // 10005
)

// 认证授权 10100-10199
const (
	_                  = iota + 10100 // 10100 占位
	CodeTokenExpired                  // 10101
	CodeTokenInvalid                  // 10102
	CodePermissionDenied              // 10103
	CodeTokenBlacklisted              // 10104
)

// 用户相关 10200-10299
const (
	_                   = iota + 10200 // 10200 占位
	CodeEmailRegistered                 // 10201
	CodePasswordWrong                   // 10202
	CodeAccountLocked                   // 10203
	CodeAccountDisabled                 // 10204
	CodeCaptchaRequired                 // 10205
)

// 积分/兑换 10300-10399
const (
	_                      = iota + 10300 // 10300 占位
	CodePointsInsufficient                 // 10301
	CodeStockInsufficient                  // 10302
	CodeItemOffline                        // 10303
	CodeDuplicateRedeem                    // 10304
	CodeFlashSaleNotStarted                // 10305
	CodeFlashSaleEnded                     // 10306
	CodeFlashSaleSoldOut                   // 10307
	CodeFlashSaleUserLimit                 // 10308
	CodeFlashSaleTimeout                   // 10309
	CodeRedeemConcurrent                   // 10310
	CodeRedeemSoldOutPeak                  // 10311
)

// 挂机相关 10400-10499
const (
	_                = iota + 10400 // 10400 占位
	CodeAlreadyIdle                  // 10401
	CodeNotIdle                      // 10402
	CodeKickedOffline                // 10403
	CodeHeartbeatTimeout             // 10404
	CodeDailyPointsLimit             // 10405
)

// 任务相关 10500-10599
const (
	_                      = iota + 10500 // 10500 占位
	CodeTaskNotCompleted                   // 10501
	CodeTaskClaimed                        // 10502
	CodeTaskExpired                        // 10503
	CodeProgressInsufficient               // 10504
)

// 限流/熔断 10600-10699
const (
	_                = iota + 10600 // 10600 占位
	CodeRateLimited                  // 10601
	CodeCircuitOpen                  // 10602
	CodeCaptchaVerify                // 10603
)

// 系统内部错误 10700-10799
const (
	_                  = iota + 10700 // 10700 占位
	CodeDBError                        // 10701
	CodeRedisError                     // 10702
	CodeKafkaError                     // 10703
	CodeThirdPartyError                // 10704
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

// New 创建带业务错误码的 error；msg 为空时回退到错误码默认消息。
// 该错误经 zrpc 传输后，网关用 Code()/Msg() 还原。
func New(code int, msg ...string) error {
	m := ""
	if len(msg) > 0 {
		m = msg[0]
	}
	if m == "" {
		m = Message(code)
	}
	return status.Error(codes.Code(code), m)
}

// Newf 同 New，支持格式化消息。
func Newf(code int, format string, args ...any) error {
	return status.Error(codes.Code(code), fmt.Sprintf(format, args...))
}

// Code 从 error（含跨 rpc 传来的 error）解析业务错误码；
// 解析失败回退 CodeUnknownError。
func Code(err error) int {
	if err == nil {
		return CodeSuccess
	}
	if st, ok := status.FromError(err); ok && st != nil {
		return int(st.Code())
	}
	return CodeUnknownError
}

// Msg 从 error（含跨 rpc 传来的 error）解析业务错误消息；
// 为空时回退到错误码默认消息。
func Msg(err error) string {
	if err == nil {
		return Message(CodeSuccess)
	}
	if st, ok := status.FromError(err); ok && st != nil {
		if st.Message() != "" {
			return st.Message()
		}
	}
	return Message(Code(err))
}

package errorx

import (
	"errors"
	"testing"
)

func TestNew_CodeMsgRoundTrip(t *testing.T) {
	tests := []struct {
		code int
		msg  string
	}{
		{CodeInvalidParam, "参数校验失败"},
		{CodeTokenExpired, "Token 已过期"},
		{CodePointsInsufficient, "积分不足"},
		{CodeDailyPointsLimit, "已达每日积分上限"},
		{CodeDBError, "数据库异常"},
	}
	for _, tt := range tests {
		err := New(tt.code)
		if Code(err) != tt.code {
			t.Errorf("Code(New(%d)) = %d, want %d", tt.code, Code(err), tt.code)
		}
		if Msg(err) != tt.msg {
			t.Errorf("Msg(New(%d)) = %q, want %q", tt.code, Msg(err), tt.msg)
		}
	}
}

func TestNew_CustomMessage(t *testing.T) {
	err := New(CodePasswordWrong, "密码错误啦")
	if Code(err) != CodePasswordWrong {
		t.Errorf("Code = %d, want %d", Code(err), CodePasswordWrong)
	}
	if Msg(err) != "密码错误啦" {
		t.Errorf("Msg = %q, want 密码错误啦", Msg(err))
	}
}

func TestNewf_Formatting(t *testing.T) {
	err := Newf(CodeRateLimited, "IP %s 限流", "1.2.3.4")
	if Msg(err) != "IP 1.2.3.4 限流" {
		t.Errorf("Msg = %q, want formatted", Msg(err))
	}
	if Code(err) != CodeRateLimited {
		t.Errorf("Code = %d, want %d", Code(err), CodeRateLimited)
	}
}

func TestCode_NilAndPlainError(t *testing.T) {
	if Code(nil) != CodeSuccess {
		t.Errorf("Code(nil) = %d, want %d", Code(nil), CodeSuccess)
	}
	// 普通 error 无 status 信息，应回退到 CodeUnknownError。
	if Code(errors.New("boom")) != CodeUnknownError {
		t.Errorf("Code(plain) = %d, want %d", Code(errors.New("boom")), CodeUnknownError)
	}
	if Msg(errors.New("boom")) != Message(CodeUnknownError) {
		t.Errorf("Msg(plain) mismatch")
	}
}

func TestMessage_Fallback(t *testing.T) {
	if Message(999999) != "未知错误" {
		t.Errorf("Message(unknown) = %q, want 未知错误", Message(999999))
	}
}

func TestErrorMessages_AllCodesHaveMessage(t *testing.T) {
	codes := []int{
		CodeSuccess, CodeInvalidParam, CodeNotFound, CodeGatewayTimeout, CodeServiceUnavailable,
		CodeTokenExpired, CodeTokenInvalid, CodePermissionDenied, CodeTokenBlacklisted,
		CodeEmailRegistered, CodePasswordWrong, CodeAccountLocked, CodeAccountDisabled, CodeCaptchaRequired,
		CodePointsInsufficient, CodeStockInsufficient, CodeItemOffline, CodeDuplicateRedeem,
		CodeFlashSaleNotStarted, CodeFlashSaleEnded, CodeFlashSaleSoldOut, CodeFlashSaleUserLimit,
		CodeFlashSaleTimeout, CodeRedeemConcurrent, CodeRedeemSoldOutPeak,
		CodeAlreadyIdle, CodeNotIdle, CodeKickedOffline, CodeHeartbeatTimeout, CodeDailyPointsLimit,
		CodeTaskNotCompleted, CodeTaskClaimed, CodeTaskExpired, CodeProgressInsufficient,
		CodeRateLimited, CodeCircuitOpen, CodeCaptchaVerify,
		CodeDBError, CodeRedisError, CodeKafkaError, CodeThirdPartyError,
	}
	for _, c := range codes {
		if Message(c) == "" {
			t.Errorf("code %d has empty message", c)
		}
		if Message(c) == "未知错误" {
			t.Errorf("code %d falls back to 未知错误 (missing mapping)", c)
		}
	}
	// CodeUnknownError 的消息本就是 "未知错误"，单独验证其映射存在且正确。
	if Message(CodeUnknownError) != "未知错误" {
		t.Errorf("Message(CodeUnknownError) = %q, want 未知错误", Message(CodeUnknownError))
	}
}

// TestNewNeverReturnsNil 回归测试：错误构造函数恒不返回 nil。
//
// 背景：CodeSuccess = 0 与 gRPC codes.OK 数值相同，status.Error(codes.OK, ...)
// 按规范返回 nil。若不做兜底，New(CodeSuccess) 会构造出"消失的错误"，
// 导致调用方 if err != nil 被静默跳过。现统一降级为 CodeUnknownError。
//
// 注意：TestNew_CodeMsgRoundTrip 原本包含 {CodeSuccess, "success"} 用例，
// 它当时"通过"正是因为 New 返回 nil 且 Code(nil)==CodeSuccess 恰好相等，
// 掩盖了该缺陷。该用例已移出，语义由本测试正确覆盖。
func TestNewNeverReturnsNil(t *testing.T) {
	cases := []struct {
		name string
		err  error
	}{
		{"New(CodeSuccess)", New(CodeSuccess)},
		{"New(CodeSuccess, msg)", New(CodeSuccess, "boom")},
		{"Newf(CodeSuccess)", Newf(CodeSuccess, "boom %d", 1)},
		{"NewErr(CodeSuccess)", NewErr(CodeSuccess, errors.New("cause"))},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.err == nil {
				t.Fatalf("%s returned nil, expected a non-nil error", tc.name)
			}
			if got := Code(tc.err); got != CodeUnknownError {
				t.Errorf("Code() = %d, want CodeUnknownError(%d)", got, CodeUnknownError)
			}
		})
	}
}

// TestNewErrHidesCause NewErr 不得把底层错误细节透传给客户端。
func TestNewErrHidesCause(t *testing.T) {
	cause := errors.New("SELECT * FROM users WHERE secret='leak'")
	err := NewErr(CodeDBError, cause)
	if Code(err) != CodeDBError {
		t.Errorf("Code = %d, want %d", Code(err), CodeDBError)
	}
	if msg := Msg(err); msg != Message(CodeDBError) {
		t.Errorf("Msg() = %q, expected standard message %q (cause must not leak)", msg, Message(CodeDBError))
	}
}

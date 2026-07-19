// ============================================================
// 挂机结算压测脚本（idle.settled 结算洪峰验证）
// ------------------------------------------------------------
// 直击真实鉴权端点 /api/v1/idle/start 与 /api/v1/idle/stop：
//   - 本地用 HS256 签发 JWT（与 config signing_key 一致），免去外部依赖；
//   - 先批量 start 建立 N 个活跃挂机会话；
//   - 再按模式制造结算洪峰：
//     mode=stop     : 批量调用 /idle/stop，模拟「主动停止」集中结算（completed=true）；
//     mode=timeout  : 建会话后不发心跳，等 scanner 自然超时结算（timeout），
//     需临时调小 idle.timeout_threshold / offline_check_interval 才能看到明显洪峰。
//   - 统计 SETTLE（stop）阶段的 RPS、P50/P95/P99 延迟、成功/失败数。
//
// 前提（务必确认）：mq.type=kafka 且 broker 可达，否则 emitIdleSettled 静默跳过、
// 结算成功但事件丢失、无重试（详见 idle_service.go:978 / :995）。
//
// 用法示例（5000 会话主动停止洪峰）：
//
//	go run ./scripts/loadtest-settle \
//	  -base http://localhost:8080 -devices 5000 -mode stop -concurrency 200 \
//	  -secret 4836c0bf4d6d6ce80922e6c17327bfa5e78d29118f9a350d228fb79cc802ac6c
//
// 用法示例（timeout 洪峰，需先调小超时阈值）：
//
//	go run ./scripts/loadtest-settle -base http://localhost:8080 -devices 5000 -mode timeout -wait 60s
//
// ============================================================
package main

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

func main() {
	base := flag.String("base", "http://localhost:8080", "服务基址")
	devices := flag.Int("devices", 5000, "模拟会话数")
	mode := flag.String("mode", "stop", "stop=主动停止洪峰；timeout=停心跳等 scanner 超时")
	concurrency := flag.Int("concurrency", 200, "start/stop 并发上限")
	secret := flag.String("secret", "4836c0bf4d6d6ce80922e6c17327bfa5e78d29118f9a350d228fb79cc802ac6c", "HS256 签名密钥")
	wait := flag.Duration("wait", 60*time.Second, "timeout 模式：建会话后等待 scanner 结算的时长")
	flag.Parse()

	b := strings.TrimRight(*base, "/")
	startURL := b + "/api/v1/idle/start"
	stopURL := b + "/api/v1/idle/stop"

	// ---- 阶段 1：批量 start 建立活跃会话 ----
	fmt.Printf("[1/2] 批量 start 建立 %d 个活跃会话 (conc=%d)...\n", *devices, *concurrency)
	startOk, startFail := batchPost(startURL, *devices, *concurrency, *secret)
	if startFail > 0 {
		fmt.Printf("    [WARN] start 失败 %d 个（会话未建立，后续结算数会偏少）\n", startFail)
	}
	fmt.Printf("    start 完成: ok=%d fail=%d\n", startOk, startFail)

	// ---- 阶段 2：结算洪峰 ----
	if *mode == "timeout" {
		fmt.Printf("[2/2] timeout 模式：已停心跳，等待 scanner 自然超时结算 %s...\n", wait.String())
		fmt.Println("    观察指标：./scripts/capacity_verify.sh  或  Prometheus idle_settle_total{reason=\"timeout\"}")
		time.Sleep(*wait)
		fmt.Println("    等待结束。若未见结算，请确认 idle.timeout_threshold / offline_check_interval 已调小。")
		return
	}

	// mode=stop：批量 stop 主动结算
	fmt.Printf("[2/2] 批量 stop 制造结算洪峰 (devices=%d conc=%d)...\n", *devices, *concurrency)
	t0 := time.Now()
	ok, fail, lat := batchPostTimed(stopURL, *devices, *concurrency, *secret)
	elapsed := time.Since(t0)

	fmt.Println("================ 汇总 ================")
	fmt.Printf("模式          : stop（主动停止，completed=true）\n")
	fmt.Printf("会话数        : %d\n", *devices)
	fmt.Printf("成功/失败     : %d / %d\n", ok, fail)
	if elapsed.Seconds() > 0 {
		fmt.Printf("总耗时        : %s\n", elapsed.Round(time.Millisecond))
		fmt.Printf("峰值 RPS      : %.0f\n", float64(ok+fail)/elapsed.Seconds())
	}
	if len(lat) > 0 {
		sortLatencies(lat)
		fmt.Printf("P50 延迟      : %d ms\n", pct(lat, 50))
		fmt.Printf("P95 延迟      : %d ms\n", pct(lat, 95))
		fmt.Printf("P99 延迟      : %d ms\n", pct(lat, 99))
		fmt.Printf("最大延迟      : %d ms\n", lat[len(lat)-1]/int64(time.Millisecond))
	}
	fmt.Println("    每个成功 stop 触发一次 settleSession → emitIdleSettled(idle.settled)。")
	fmt.Println("    验证消费侧：Prometheus consumer lag / idle_settle_total；Kafka 不可达时事件静默丢失（_ = err）。")
}

// batchPost 并发 POST {device_id: settle_{i}}，uid 取模到 10 万用户池，返回成功/失败数。
func batchPost(url string, n, conc int, secret string) (int64, int64) {
	var ok, fail int64
	sem := make(chan struct{}, conc)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		sem <- struct{}{}
		go func(idx int) {
			defer wg.Done()
			defer func() { <-sem }()
			uid := fmt.Sprintf("u%d", idx%100000)
			did := fmt.Sprintf("settle_%d", idx)
			if postJSON(url, uid, did, secret) {
				atomic.AddInt64(&ok, 1)
			} else {
				atomic.AddInt64(&fail, 1)
			}
		}(i)
	}
	wg.Wait()
	return ok, fail
}

// batchPostTimed 同 batchPost 但额外采集每次请求延迟（纳秒）。
func batchPostTimed(url string, n, conc int, secret string) (int64, int64, []int64) {
	var ok, fail int64
	var mu sync.Mutex
	lat := make([]int64, 0, n)
	sem := make(chan struct{}, conc)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		sem <- struct{}{}
		go func(idx int) {
			defer wg.Done()
			defer func() { <-sem }()
			uid := fmt.Sprintf("u%d", idx%100000)
			did := fmt.Sprintf("settle_%d", idx)
			t0 := time.Now()
			success := postJSON(url, uid, did, secret)
			dt := time.Since(t0)
			mu.Lock()
			lat = append(lat, int64(dt))
			mu.Unlock()
			if success {
				atomic.AddInt64(&ok, 1)
			} else {
				atomic.AddInt64(&fail, 1)
			}
		}(i)
	}
	wg.Wait()
	return ok, fail, lat
}

func postJSON(url, uid, did, secret string) bool {
	token := mintHS256(secret, uid)
	body, _ := json.Marshal(map[string]string{"device_id": did})
	req, _ := http.NewRequest(http.MethodPost, url, strings.NewReader(string(body)))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	return resp.StatusCode == 200
}

// mintHS256 本地签发 JWT（仅 header+payload+signature，HS256），用于压测绕过鉴权。
func mintHS256(secret, userID string) string {
	header := b64([]byte(`{"alg":"HS256","typ":"JWT"}`))
	now := time.Now().Unix()
	payload := b64([]byte(fmt.Sprintf(
		`{"iss":"hc-framework","sub":"%s","exp":%d,"iat":%d}`, userID, now+7200, now)))
	signing := header + "." + payload
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(signing))
	sig := b64(mac.Sum(nil))
	return signing + "." + sig
}

func b64(b []byte) string {
	return strings.TrimRight(base64.URLEncoding.EncodeToString(b), "=")
}

func sortLatencies(s []int64) {
	for i := 1; i < len(s); i++ {
		for j := i; j > 0 && s[j-1] > s[j]; j-- {
			s[j-1], s[j] = s[j], s[j-1]
		}
	}
}

func pct(s []int64, p int) int64 {
	if len(s) == 0 {
		return 0
	}
	idx := (len(s) - 1) * p / 100
	return s[idx] / int64(time.Millisecond)
}

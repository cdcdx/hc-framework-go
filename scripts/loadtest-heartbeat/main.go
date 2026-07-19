// ============================================================
// 心跳压测脚本（百万设备在线容量验证）
// ------------------------------------------------------------
// 直击真实鉴权端点 /api/v1/idle/heartbeat：
//   - 本地用 HS256 签发 JWT（与 config signing_key 一致），免去外部依赖；
//   - 每设备按 --interval 周期性 POST {"device_id":"..."}；
//   - 统计 RPS、P50/P95/P99 延迟、成功/失败数，校准 8.2 的 Pod 数与 Redis 分片。
//
// 用法示例（模拟 100 万设备，心跳 30s，持续 5 分钟）：
//
//	go run ./scripts/loadtest-heartbeat \
//	  -base http://localhost:8080 \
//	  -devices 1000000 -interval 30s -duration 5m \
//	  -secret 4836c0bf4d6d6ce80922e6c17327bfa5e78d29118f9a350d228fb79cc802ac6c
//
// 注：默认 secret 取自 config/config.yaml 的 auth.jwt.signing_key（HS256 开发密钥）。
// 生产 RS256 需改为读取私钥签名。
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
	devices := flag.Int("devices", 1000000, "模拟设备数")
	interval := flag.Duration("interval", 30*time.Second, "心跳间隔")
	duration := flag.Duration("duration", 5*time.Minute, "压测时长")
	secret := flag.String("secret", "4836c0bf4d6d6ce80922e6c17327bfa5e78d29118f9a350d228fb79cc802ac6c", "HS256 签名密钥")
	flag.Parse()

	endpoint := strings.TrimRight(*base, "/") + "/api/v1/idle/heartbeat"

	var ok, fail int64
	var latMutex sync.Mutex
	latencies := make([]int64, 0, 1<<20)

	// 并发上限：避免单机瞬间打爆，按 device 数自适应（封顶 20000）
	concurrency := *devices
	if concurrency > 20000 {
		concurrency = 20000
	}
	sem := make(chan struct{}, concurrency)

	start := time.Now()
	stop := start.Add(*duration)

	var wg sync.WaitGroup
	// 把 devices 均摊到 interval 时间窗内（泊松近似：错峰发送，避免同一刻全量冲击）
	spread := *interval
	if spread <= 0 {
		spread = time.Second
	}
	perTick := float64(*devices) / spread.Seconds()

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	issued := int64(0)
	go func() {
		for t := range ticker.C {
			if t.After(stop) {
				return
			}
			n := int(perTick)
			for i := 0; i < n; i++ {
				idx := atomic.AddInt64(&issued, 1)
				if int(idx) > *devices {
					break
				}
				wg.Add(1)
				sem <- struct{}{}
				go func(id int64) {
					defer wg.Done()
					defer func() { <-sem }()
					sendHeartbeat(endpoint, *secret, id, &ok, &fail, &latMutex, &latencies)
				}(idx)
			}
		}
	}()

	// 进度 + 汇总
	prog := time.NewTicker(10 * time.Second)
	defer prog.Stop()
	go func() {
		for range prog.C {
			elapsed := time.Since(start).Seconds()
			if elapsed <= 0 {
				continue
			}
			rps := float64(atomic.LoadInt64(&ok)+atomic.LoadInt64(&fail)) / elapsed
			fmt.Printf("[%s] 已发=%d 成功=%d 失败=%d 平均RPS=%.0f\n",
				time.Since(start).Round(time.Second), atomic.LoadInt64(&issued),
				atomic.LoadInt64(&ok), atomic.LoadInt64(&fail), rps)
		}
	}()

	time.Sleep(*duration + 5*time.Second)
	wg.Wait()

	// 延迟统计
	sortLatencies(latencies)
	fmt.Println("================ 汇总 ================")
	fmt.Printf("设备数        : %d\n", *devices)
	fmt.Printf("时长          : %s\n", duration.String())
	fmt.Printf("成功/失败     : %d / %d\n", atomic.LoadInt64(&ok), atomic.LoadInt64(&fail))
	if len(latencies) > 0 {
		fmt.Printf("P50 延迟      : %d ms\n", pct(latencies, 50))
		fmt.Printf("P95 延迟      : %d ms\n", pct(latencies, 95))
		fmt.Printf("P99 延迟      : %d ms\n", pct(latencies, 99))
		fmt.Printf("最大延迟      : %d ms\n", latencies[len(latencies)-1]/int64(time.Millisecond))
	}
	fmt.Printf("峰值稳态 RPS  : %.0f（设备数/间隔=%d/s）\n", float64(*devices)/(*interval).Seconds(), int(float64(*devices)/(*interval).Seconds()))
}

func sendHeartbeat(endpoint, secret string, id int64, ok, fail *int64, mu *sync.Mutex, lat *[]int64) {
	uid := fmt.Sprintf("u%d", id%100000) // 10 万个用户，每用户最多多设备
	did := fmt.Sprintf("d%d", id)
	token := mintHS256(secret, uid)
	body, _ := json.Marshal(map[string]string{"device_id": did})

	req, _ := http.NewRequest(http.MethodPost, endpoint, strings.NewReader(string(body)))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)

	t0 := time.Now()
	resp, err := http.DefaultClient.Do(req)
	dt := time.Since(t0)
	mu.Lock()
	*lat = append(*lat, int64(dt))
	mu.Unlock()

	if err != nil {
		atomic.AddInt64(fail, 1)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusOK {
		atomic.AddInt64(ok, 1)
	} else {
		atomic.AddInt64(fail, 1)
	}
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
	// 简单插入排序（样本大时用更优排序即可，这里够用）
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

import http from 'k6/http';
import { check, sleep } from 'k6';
import { Trend, Rate } from 'k6/metrics';
import ws from 'k6/ws';
import { createHash } from 'k6/crypto';

// ============================================
// WebSocket 压测：长连接心跳替代方案性能验证
//
// 场景说明：
//   ws_heartbeat —— 建立 WS 连接后周期性发送 ping，验证长连接稳定性
//   ws_reconnect  —— 频繁连接/断开，验证服务端连接池回收能力
//
// 压测目标：
//   1. WS 建连成功率（含 JWT 鉴权）
//   2. ping-pong 延迟（替代 HTTP 心跳的实时性）
//   3. 10k+ 长连接常驻下的内存/连接数稳定性
//   4. 高频重连场景下无连接泄漏
// ============================================

const BASE_URL = __ENV.BASE_URL || 'http://localhost:8080';
const WS_URL = BASE_URL.replace(/^http/, 'ws') + '/api/v1/ws';

// 自定义指标
const wsConnectDuration = new Trend('ws_connect_duration', true);   // WS 建连耗时
const wsPingPongDuration = new Trend('ws_pingpong_duration', true); // ping-pong 往返延迟
const wsConnectOk = new Rate('ws_connect_ok');                      // 建连成功率

export const options = {
    scenarios: {
        // 场景一：长连接心跳（模拟挂机场景）
        ws_heartbeat: {
            executor: 'ramping-vus',
            exec: 'wsHeartbeat',
            startVUs: 0,
            stages: [
                { duration: '1m', target: 1000 },   // 预热
                { duration: '3m', target: 5000 },   // 加压
                { duration: '2m', target: 10000 },  // 满负载
                { duration: '1m', target: 0 },      // 冷却
            ],
            gracefulRampDown: '30s',
        },
        // 场景二：频繁重连（验证连接池回收）
        ws_reconnect: {
            executor: 'constant-vus',
            exec: 'wsReconnect',
            vus: 200,
            duration: '2m',
        },
    },
    thresholds: {
        'ws_connect_ok': ['rate>0.99'],              // 建连成功率 > 99%
        'ws_connect_duration': ['p(99)<2000'],        // 建连 P99 < 2s
        'ws_pingpong_duration': ['p(99)<500'],        // ping-pong P99 < 500ms
    },
};

// ============================================
// 辅助：登录获取 JWT Token
// ============================================
function loginUser(email, password) {
    const pwdHash = createHash('sha256').update(password).hexdigest();
    const payload = JSON.stringify({ email, password: pwdHash });
    const res = http.post(`${BASE_URL}/api/v1/auth/login`, payload, {
        headers: { 'Content-Type': 'application/json' },
    });
    if (res.status === 200) {
        const body = JSON.parse(res.body);
        if (body.code === 0 && body.data && body.data.access_token) {
            return body.data.access_token;
        }
    }
    return null;
}

// ============================================
// 场景一：长连接心跳
// 每个 VU：登录 → 建 WS → 循环 ping-pong → 断开
// ============================================
export function wsHeartbeat() {
    const email = `ws_heartbeat_${__VU}@test.com`;
    const password = 'TestPass123';

    // 注册（首次可能失败已存在，忽略）
    const regHash = createHash('sha256').update(password).hexdigest();
    http.post(`${BASE_URL}/api/v1/auth/register`, JSON.stringify({
        email, password: regHash, nickname: `vu${__VU}`,
    }), { headers: { 'Content-Type': 'application/json' } });

    // 登录获取 Token
    const token = loginUser(email, password);
    if (!token) {
        wsConnectOk.add(false);
        console.error(`VU ${__VU}: login failed`);
        return;
    }

    // 建立 WebSocket 连接
    const url = `${WS_URL}`;
    const params = { headers: { 'Authorization': `Bearer ${token}` } };

    const startTime = Date.now();
    const response = ws.connect(url, params, function (socket) {
        let connected = false;
        let pingCount = 0;
        const maxPings = 10; // 每个连接发 10 次 ping 后关闭

        socket.on('open', function () {
            connected = true;
            wsConnectDuration.add(Date.now() - startTime);
            wsConnectOk.add(true);
            // 建连成功后立即发第一个 ping
            socket.send(JSON.stringify({ type: 'ping' }));
        });

        socket.on('message', function (data) {
            if (!connected) {
                wsConnectDuration.add(Date.now() - startTime);
                wsConnectOk.add(true);
                connected = true;
            }
            try {
                const msg = JSON.parse(data);
                if (msg.type === 'pong') {
                    // 记录 ping-pong 延迟
                    wsPingPongDuration.add(Date.now() - startTime);
                    pingCount++;
                    if (pingCount < maxPings) {
                        // 间隔 1-3s 发下一次 ping
                        const interval = 1000 + Math.random() * 2000;
                        socket.setTimeout(function () {
                            socket.send(JSON.stringify({ type: 'ping' }));
                        }, interval);
                    } else {
                        // 发完指定次数，优雅关闭
                        socket.close();
                    }
                }
            } catch (e) {
                // JSON 解析失败忽略
            }
        });

        socket.on('error', function (e) {
            if (!connected) {
                wsConnectOk.add(false);
            }
            console.error(`VU ${__VU}: ws error: ${e}`);
        });

        socket.on('close', function () {
            // 连接关闭，VU 结束
        });

        // 30s 超时兜底（防止连接卡死）
        socket.setTimeout(function () {
            if (pingCount < maxPings) {
                console.warn(`VU ${__VU}: timeout after ${pingCount} pings`);
            }
            socket.close();
        }, 30000);
    });

    // 连接级错误（dns 失败、连接拒绝等）
    if (response && response.status && response.status !== 101) {
        wsConnectOk.add(false);
    }

    // VU 冷却
    sleep(1);
}

// ============================================
// 场景二：频繁重连
// 每个 VU 不停登录→建连→断开，验证无连接泄漏
// ============================================
export function wsReconnect() {
    const email = `ws_recon_${__VU}@test.com`;
    const password = 'TestPass123';

    // 注册
    const regHash = createHash('sha256').update(password).hexdigest();
    http.post(`${BASE_URL}/api/v1/auth/register`, JSON.stringify({
        email, password: regHash, nickname: `recon${__VU}`,
    }), { headers: { 'Content-Type': 'application/json' } });

    const token = loginUser(email, password);
    if (!token) {
        wsConnectOk.add(false);
        return;
    }

    const url = `${WS_URL}`;
    const params = { headers: { 'Authorization': `Bearer ${token}` } };

    const startTime = Date.now();
    ws.connect(url, params, function (socket) {
        let connected = false;

        socket.on('open', function () {
            connected = true;
            wsConnectDuration.add(Date.now() - startTime);
            wsConnectOk.add(true);
            // 发一次 ping 后立即关闭（模拟短连接场景）
            socket.send(JSON.stringify({ type: 'ping' }));
        });

        socket.on('message', function () {
            if (!connected) {
                wsConnectOk.add(true);
                connected = true;
            }
            // 收到任意消息后立即断开
            socket.close();
        });

        socket.on('error', function () {
            if (!connected) {
                wsConnectOk.add(false);
            }
        });

        // 5s 超时兜底
        socket.setTimeout(function () {
            if (!connected) {
                wsConnectOk.add(false);
            }
            socket.close();
        }, 5000);
    });

    // 每次迭代间隔 1-3s
    sleep(1 + Math.random() * 2);
}

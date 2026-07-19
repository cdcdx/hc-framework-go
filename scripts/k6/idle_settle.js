import http from 'k6/http';
import { check, sleep } from 'k6';
import { Trend, Rate } from 'k6/metrics';
import { createHash } from 'k6/crypto';

// ============================================
// 挂机结算压测（idle.settled 结算洪峰）
// ------------------------------------------------------------
// 每个 VU 每轮：start 新会话 -> stop 结算，制造 idle.settled 事件洪峰，
// 验证 settleSession（DB 事务 + 积分流水 + outbox）与下游消费者
// TaskService.ApplyEventProgress 链路。
//
// 重要前提（否则事件静默丢失）：
//   - mq.type=kafka 且 broker 可达，否则 emitIdleSettled 静默跳过（idle_service.go:978/995）。
//   - 观察消费侧：Prometheus idle_settle_total / consumer lag。
//
// 关于指标的两点说明：
//   1) 默认节奏下会话仅存活数秒，durationMinutes 会被钳到最小 1 分钟但
//      DurationSeconds<60 会使消费侧 delta=DurationSeconds/60=0 而跳过任务进度累加
//      （事件仍照常发布/消费/写去重表）。若需压「任务进度累加」，把 SETTLE_DELAY
//      调到 >60s（代价是结算速率下降）。
//   2) 高频结算可能触达 DailyPointsLimit，使 PointsEarned=0，结算仍发生、事件仍发出。
//
// 数据量：每轮 start+stop 用唯一 device，会产生大量 idle_records；建议用独立压测库，
// 或压测后清理。
//
// 用法：
//   k6 run scripts/k6/idle_settle.js
//   BASE_URL=http://localhost:8080 SETTLE_DELAY=65s k6 run scripts/k6/idle_settle.js
// ============================================

// 峰值 VU：必须 <= POOL_SIZE，保证每个 VU 独占一个 user（消除共享 user + max_devices 串扰）。
const MAX_VUS = parseInt(__ENV.MAX_VUS || '500', 10);

export const options = {
    stages: [
        { duration: '1m', target: Math.floor(MAX_VUS * 0.2) }, // 预热
        { duration: '3m', target: Math.floor(MAX_VUS * 0.6) }, // 加压
        { duration: '3m', target: MAX_VUS },                   // 满负载
        { duration: '1m', target: 0 },                         // 冷却
    ],
    // 注册走 bcrypt cost=12，放大 setupTimeout 并并发注册。
    setupTimeout: '180s',
    thresholds: {
        // 结算是一次写事务（乐观锁 + 积分流水 + outbox），在峰值 VU 下 p99 天然高于纯读/心跳。
        // 设为 3000ms 作为“结算事务合理量级”的告警线；若远超此值说明 DB/锁成为瓶颈，需查慢查询或扩容。
        'http_req_duration': ['p(99)<3000'],
        'http_req_failed': ['rate<0.001'],
        'idle_settle_ok': ['rate>0.99'],
        'idle_start_ok': ['rate>0.99'],
    },
};

const BASE_URL = __ENV.BASE_URL || 'http://localhost:8080';
const SETTLE_DELAY = __ENV.SETTLE_DELAY || '1s';

// 自定义指标
const settleDuration = new Trend('idle_settle_duration');
const settleSuccess = new Rate('idle_settle_ok');
const startSuccess = new Rate('idle_start_ok');
// 分错误原因统计结算失败（便于区分乐观锁冲突 vs DB 错误 vs 其他）
const settleFailByCode = {
    10402: new Rate('idle_settle_fail_not_idle'),   // 未在挂机（负缓存命中等）
    10304: new Rate('idle_settle_fail_conflict'),  // 并发冲突 / 乐观锁
    10701: new Rate('idle_settle_fail_db_error'),  // DB 异常
    other: new Rate('idle_settle_fail_other'),     // 其他未知错误
};

export function setup() {
    // 用户池 = 峰值 VU，保证每个 VU 独占一个 user：
    //   /idle/stop-device 只停自己那台设备，且同一 user 的并发设备数始终为 1，
    //   永不触达 max_devices（默认 3），从根本上消除 VU 之间互相结算的假失败。
    const POOL_SIZE = MAX_VUS;
    const users = [];

    console.log(`[Setup] Creating ${POOL_SIZE} test users for idle settle test...`);

    const regRequests = [];
    const emails = [];
    for (let i = 0; i < POOL_SIZE; i++) {
        const email = `idle_settle_${Date.now()}_${i}@example.com`;
        emails.push(email);
        regRequests.push({
            method: 'POST',
            url: `${BASE_URL}/api/v1/auth/register`,
            body: JSON.stringify({
                email: email,
                password: sha256('TestPass123!'),
            }),
            params: { headers: { 'Content-Type': 'application/json' } },
        });
    }

    const regResponses = http.batch(regRequests);
    let ok = 0;
    for (let i = 0; i < regResponses.length; i++) {
        const regRes = regResponses[i];
        if (regRes.status !== 200) continue;
        let token;
        try {
            token = JSON.parse(regRes.body).data.access_token;
        } catch (e) {
            continue;
        }
        if (token) {
            users.push({ email: emails[i], token: token });
            ok++;
        }
    }
    console.log(`[Setup] register ok=${ok}`);
    if (users.length === 0) {
        throw new Error('[Setup] Failed to create any test users');
    }
    return { users };
}

export default function (data) {
    // VU 与 user 一一对应（__VU 从 1 开始）：每个 VU 独占一个 user，
    // 避免多个 VU 共享 user 时被 /idle/stop 全量结算或 max_devices 挤位而互相踩。
    const user = data.users[(__VU - 1) % data.users.length];
    // 每 (VU, iteration) 唯一设备，确保每轮都真实 start+stop，无「已挂机」冲突
    const deviceId = `k6settle_${__VU}_${__ITER}`;
    const headers = {
        'Content-Type': 'application/json',
        'Authorization': `Bearer ${user.token}`,
    };

    // start（建会话；device 唯一，必成功）
    const startRes = http.post(`${BASE_URL}/api/v1/idle/start`, JSON.stringify({ device_id: deviceId }), { headers });
    const startOk = isSettled(startRes);
    startSuccess.add(startOk);
    check(startRes, {
        'start status is 200': (r) => r.status === 200,
        'start response is ok': (r) => startOk,
    });

    // start 失败（多为 DB 连接排队/超时，已由 make mysql-stress-tune 抬上限缓解）则跳过结算，
    // 避免级联成「settle 假失败」污染 idle_settle_ok 指标，便于精确定位瓶颈。
    if (!startOk) {
        sleep(1);
        return;
    }

    // 可选：让会话存活足够久，使消费侧 delta>=1（默认 1s 时 delta 可能为 0）
    if (SETTLE_DELAY && SETTLE_DELAY !== '0s') {
        sleep(parseDuration(SETTLE_DELAY));
    }

    // stop-device -> 只结算自己这台设备（核心压测指标）。
    // 注意：不能用 /idle/stop（那会结算该 user 名下全部设备），否则 VU 之间互相结算产生假失败。
    const res = http.post(`${BASE_URL}/api/v1/idle/stop-device`, JSON.stringify({ device_id: deviceId }), {
        headers,
        tags: { name: 'idle_settle' },
    });

    settleDuration.add(res.timings.duration);
    const settleOK = isSettled(res);
    settleSuccess.add(settleOK);

    check(res, {
        'settle status is 200': (r) => r.status === 200,
        'settle response is ok': (r) => settleOK,
    });

    // stop-device 失败时打印具体 body.code 与错误信息，快速定位根因（乐观锁冲突 / DB 错误 / 负缓存命中 等）
    if (!settleOK) {
        logSettleFailure(res);
    }

    sleep(1);
}

function isSettled(r) {
    try {
        const body = JSON.parse(r.body);
        return r.status === 200 && body.code === 0;
    } catch (e) {
        return false;
    }
}

// logSettleFailure 在 stop-device 失败时把 body.code / HTTP status / message / trace_id
// 打印到控制台，并写入对应错误分类指标，便于快速区分：
//   - 10402 "未在挂机" → 负缓存命中（start 后立即 stop 可能命中 30s 空值缓存）；
//   - 10304 "并发冲突" → 乐观锁竞争（多 VU 共享 user 或 scanner 并发结算）；
//   - 10701 "DB 异常"   → 数据库连接池耗尽 / 主从切换中。
function logSettleFailure(res) {
    let code = res.status; // 默认用 HTTP status
    let message = '';
    let traceId = '';
    try {
        const body = JSON.parse(res.body);
        code = body.code != null ? body.code : code;
        message = body.message || '';
        traceId = body.trace_id || '';
    } catch (_) {
        // 非 JSON body：直接用 HTTP status
    }

    const label = settleFailLabel(code);
    settleFailByCode[label].add(1);

    // 采样输出（每 20 次失败输出一次，避免控制台刷屏但仍能看到各类错误）
    const sampleRate = 20;
    if (__ITER % sampleRate === 0 || label === 'other') {
        console.warn(
            `[idle_settle FAIL] HTTP=${res.status} code=${code}` +
            (message ? ` msg="${message}"` : '') +
            (traceId ? ` trace_id=${traceId}` : '') +
            ` label=${label}`,
        );
    }
}

function settleFailLabel(code) {
    if (code === 10402) return 10402;
    if (code === 10304) return 10304;
    if (code === 10701) return 10701;
    return 'other';
}

function parseDuration(s) {
    // 支持 "Ns" / "Nm" 简化的秒/分；k6 的 sleep 也接受字符串，这里转数字秒兜底
    const m = /^(\d+)(s|m)$/.exec(s);
    if (!m) return 1;
    return m[2] === 'm' ? parseInt(m[1], 10) * 60 : parseInt(m[1], 10);
}

function sha256(str) {
    const hasher = createHash('sha256');
    hasher.update(str);
    return hasher.digest('hex');
}

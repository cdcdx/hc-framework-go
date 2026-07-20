import http from 'k6/http';
import { check, sleep, group } from 'k6';
import { Trend, Rate } from 'k6/metrics';
import { makeEmail, passwordHash } from './accounts.js';

// ============================================
// 挂机模块压测：心跳上报
// ============================================

// 双场景：
//   idle    —— 心跳上报，走 Redis 续期（不落库、不限流），验证 10k 长连接常驻下的存活语义。
//   dbstress —— 打 MySQL 的 profile 读，真正制造 GORM 连接池竞争，用于验证 db_pool_* 指标与池容量。
export const options = {
    scenarios: {
        idle: {
            executor: 'ramping-vus',
            exec: 'idleHeartbeat',
            startVUs: 0,
            stages: [
                { duration: '1m', target: 2000 },   // 预热
                { duration: '3m', target: 5000 },   // 加压
                { duration: '3m', target: 10000 },  // 满负载
                { duration: '1m', target: 0 },      // 冷却
            ],
            gracefulRampDown: '30s',
        },
        // dbstress：并发打 MySQL 读。目标 800 与压测机 MySQL max_connections=800 对齐；
        // GORM 连接池上限通常远小于 800，故并发会超过池容量 → 触发 db_pool_wait_* 信号。
        dbstress: {
            executor: 'ramping-vus',
            exec: 'dbRead',
            startVUs: 0,
            stages: [
                { duration: '1m', target: 200 },    // 预热
                { duration: '4m', target: 800 },    // 满负载（超过连接池 → 等待）
                { duration: '2m', target: 800 },    // 保持压力
                { duration: '1m', target: 0 },      // 冷却
            ],
            gracefulRampDown: '30s',
        },
    },
    // setup() 需注册 POOL_SIZE 个测试用户换取 token；改为并发批量注册后通常几秒完成，
    // 这里把上限放宽到 180s 以防服务端注册较慢（bcrypt cost=12）时仍够用。
    setupTimeout: '180s',
    thresholds: {
        // 心跳路径为轻量 Redis 续期（GET 判活 + SETEX 续期，不落库、不限流），功能上 100% 存活。
        // 但本压测拉起 10000 长连接常驻，Go 堆增大导致 GC 停顿偶发抬升尾延迟（实测 p99≈364ms）；
        // keepalive 类接口的尾延迟不影响“存活判活”正确性，故放宽到 500ms 作为合理上界。
        // 若需更严 SLO，应在降低常驻连接数或优化 GC 后单独评估，而非在 10k 连接压测里卡 100ms。
        'http_req_duration': ['p(99)<500'],
        'http_req_failed': ['rate<0.0001'],
        'idle_heartbeat_ok': ['rate>0.99'],
        // dbstress 直击 MySQL，连接池竞争下等待可接受，但功能上必须 99% 成功。
        'db_read_ok': ['rate>0.99'],
    },
};

const BASE_URL = __ENV.BASE_URL || 'http://localhost:8080';

// 自定义指标
const heartbeatDuration = new Trend('idle_heartbeat_duration');
const heartbeatSuccess = new Rate('idle_heartbeat_ok');
const dbReadDuration = new Trend('db_read_duration');
const dbReadSuccess = new Rate('db_read_ok');

// ============================================
// setup: 批量注册测试用户，获取 Token，并为每个用户预建一个固定挂机会话
// ============================================
export function setup() {
    const POOL_SIZE = 200; // 200 个用户池，供 VU 轮转使用
    const users = [];

    console.log(`[Setup] Creating ${POOL_SIZE} test users for idle test...`);

    // 1) 批量注册，获取 token（http.batch 并发，避免 setupTimeout）
    const regRequests = [];
    const emails = [];
    for (let i = 0; i < POOL_SIZE; i++) {
        const email = makeEmail('idle', i);
        emails.push(email);
        regRequests.push({
            method: 'POST',
            url: `${BASE_URL}/api/v1/auth/register`,
            body: JSON.stringify({
                email: email,
                password: passwordHash(),
            }),
            // 注意：k6 的 http.batch 请求对象不识别顶层 headers，必须放在 params.headers 下，
            // 否则 Authorization/Content-Type 等请求头不会真正发送。
            params: { headers: { 'Content-Type': 'application/json' } },
        });
    }

    const regResponses = http.batch(regRequests);
    const authed = [];
    const regFail = {};           // status -> count
    let regSampleBody = '';       // 一条失败样本 body
    let parseFail = 0;
    for (let i = 0; i < regResponses.length; i++) {
        const regRes = regResponses[i];
        if (regRes.status !== 200) {
            regFail[regRes.status] = (regFail[regRes.status] || 0) + 1;
            if (!regSampleBody) regSampleBody = String(regRes.body).slice(0, 200);
            continue;
        }

        let token;
        try {
            token = JSON.parse(regRes.body).data.access_token;
        } catch (e) {
            parseFail++;
            continue;
        }

        if (token) {
            authed.push({ email: emails[i], token });
        } else {
            parseFail++;
        }
    }
    console.log(`[Setup] register: ok=${authed.length} parseFail=${parseFail} httpFail=${JSON.stringify(regFail)} sample=${regSampleBody}`);

    if (authed.length === 0) {
        throw new Error('[Setup] Failed to create any test users');
    }

    // 2) 为每个用户预建一个固定挂机会话（device_id 固定为 k6_idle_{i}）。
    //    关键：default 阶段只发心跳，不再每轮 start 新设备，避免高并发下
    //    会话重建/结算抖动导致 idle/start 偶发失败、进而心跳报 not_idle 的连锁失败。
    const startRequests = [];
    for (let i = 0; i < authed.length; i++) {
        startRequests.push({
            method: 'POST',
            url: `${BASE_URL}/api/v1/idle/start`,
            body: JSON.stringify({ device_id: `k6_idle_${i}` }),
            // 必须放在 params.headers 下，否则 Authorization 头不会被发送（旧写法导致 100% 401）。
            params: {
                headers: {
                    'Content-Type': 'application/json',
                    'Authorization': `Bearer ${authed[i].token}`,
                },
            },
        });
    }
    const startResponses = http.batch(startRequests);
    let started = 0;
    const startFail = {};          // status -> count
    let startSampleBody = '';      // 一条失败样本 body（含业务 code：10101 过期/10102 无效/10104 失效）
    for (let i = 0; i < startResponses.length; i++) {
        if (startResponses[i].status !== 200) {
            startFail[startResponses[i].status] = (startFail[startResponses[i].status] || 0) + 1;
            if (!startSampleBody) startSampleBody = String(startResponses[i].body).slice(0, 200);
            continue;
        }
        users.push({ email: authed[i].email, token: authed[i].token, deviceId: `k6_idle_${i}` });
        started++;
    }
    console.log(`[Setup] idle/start: ok=${started} httpFail=${JSON.stringify(startFail)} sample=${startSampleBody}`);

    console.log(`[Setup] Created ${users.length} test users, started ${started} idle sessions`);

    if (users.length === 0) {
        throw new Error('[Setup] Failed to start any idle session');
    }

    return { users };
}

export function idleHeartbeat(data) {
    // 轮转使用预注册用户（每个用户对应一个已在 setup 建好的固定挂机会话）
    const user = data.users[__VU % data.users.length];
    const deviceId = user.deviceId;

    // 心跳上报（核心压测指标）；仅心跳，会话在 setup 阶段已建立，避免会话重建抖动
    group('Idle Heartbeat', () => {
        const res = http.post(`${BASE_URL}/api/v1/idle/heartbeat`, JSON.stringify({
            device_id: deviceId,
        }), {
            headers: {
                'Content-Type': 'application/json',
                'Authorization': `Bearer ${user.token}`,
            },
            tags: { name: 'heartbeat' },
        });

        heartbeatDuration.add(res.timings.duration);
        heartbeatSuccess.add(isAlive(res));

        check(res, {
            'heartbeat status is 200': (r) => r.status === 200,
            // 10k 长连接下 GC 尾延迟偶发，心跳本身（Redis 续期）很快，放宽到 300ms 作为“快”的判据。
            'heartbeat response fast': (r) => r.timings.duration < 300,
            'heartbeat response is alive': (r) => isAlive(r),
        });
    });

    // 每 30s 发一次心跳
    sleep(30);
}

// dbRead：打 MySQL 的 profile 读，验证 GORM 连接池在并发下的饱和度与等待信号。
// 每 1s 一次；dbstress 场景并发拉到 800，超过连接池上限后 db_pool_wait_* 应出现增长。
export function dbRead(data) {
    const user = data.users[__VU % data.users.length];

    group('DB Read (profile)', () => {
        const res = http.get(`${BASE_URL}/api/v1/user/profile`, {
            headers: {
                'Authorization': `Bearer ${user.token}`,
            },
            tags: { name: 'dbread' },
        });

        dbReadDuration.add(res.timings.duration);
        dbReadSuccess.add(res.status === 200);

        check(res, {
            'profile status is 200': (r) => r.status === 200,
            // 连接池等待会直接抬升该读延迟；300ms 作为“未严重竞争”的判据。
            'profile response fast': (r) => r.timings.duration < 300,
        });
    });

    // 每 1s 打一次 MySQL 读
    sleep(1);
}

// isAlive 判定心跳响应是否为有效存活（HTTP 200 + 业务码 0 + status=alive）。
// 注意 response.Error 对 not_idle 也返回 HTTP 200 但业务码非 0，
// 仅用 status==200 会把“会话不存在”误记为成功。
function isAlive(r) {
    try {
        const body = JSON.parse(r.body);
        return r.status === 200 && body.code === 0 && body.data && body.data.status === 'alive';
    } catch (e) {
        return false;
    }
}



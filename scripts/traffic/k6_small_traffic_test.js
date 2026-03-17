import http from 'k6/http';
import exec from 'k6/execution';
import { check, sleep } from 'k6';
import { Counter, Rate, Trend } from 'k6/metrics';

// Shared constants for this scenario.
const MB = 1024 * 1024;
const LINE_COUNT = 16;
const TOTAL_ATTEMPT_MB_PER_LINE = 50;
const REQUEST_INTERVAL_SECONDS = 1;
const APP2_SPEED_LIMIT_KBPS = 1000;
const K6_TIMEOUT = __ENV.REQUEST_TIMEOUT || '30s';

// Base URL is configurable so the same script can be reused for local/dev envs.
const BASE_URL = (__ENV.BASE_URL || 'http://localhost:8080').replace(/\/+$/, '');
const API_PATH = (__ENV.API_PATH || '/api/traffic/requests').startsWith('/')
  ? (__ENV.API_PATH || '/api/traffic/requests')
  : `/${__ENV.API_PATH}`;
const TARGET_URL = `${BASE_URL}${API_PATH}`;

// Custom metrics used to quickly verify enqueue quality during load.
const enqueueDurationMs = new Trend('traffic_enqueue_duration_ms');
const enqueueHttpErrorRate = new Rate('traffic_enqueue_http_error_rate');
const enqueueTraceIdErrorRate = new Rate('traffic_enqueue_traceid_error_rate');
const enqueueSuccessCount = new Counter('traffic_enqueue_success_total');
const enqueueFailCount = new Counter('traffic_enqueue_fail_total');
const attemptedBytesCount = new Counter('traffic_attempted_bytes_total');

export const options = {
  scenarios: {
    small_traffic: {
      // We pin one VU per line so each line keeps its own deterministic request sequence.
      executor: 'per-vu-iterations',
      vus: LINE_COUNT,
      iterations: 1,
      maxDuration: __ENV.MAX_DURATION || '30m',
      gracefulStop: '0s',
    },
  },
  thresholds: {
    traffic_enqueue_http_error_rate: ['rate==0'],
    traffic_enqueue_traceid_error_rate: ['rate==0'],
  },
};

// line_id -> family_id mapping:
// 1~4 => 1, 5~8 => 2, 9~12 => 3, 13~16 => 4
function resolveFamilyId(lineId) {
  if (lineId < 1 || lineId > 16) {
    throw new Error(`lineId out of range: ${lineId}`);
  }
  return Math.floor((lineId - 1) / 4) + 1;
}

// Test-group assignment by line_id.
function resolveGroupName(lineId) {
  if (lineId <= 4) return 'G1_NO_RESTRICTION';
  if (lineId <= 8) return 'G2_DAILY_20MB';
  if (lineId <= 12) return 'G3_APP2_DAILY_5MB';
  if (lineId <= 14) return 'G4_SHARED_ONLY';
  return 'G5_APP2_SPEED_1MBPS';
}

// Group-specific app selection:
// - app_id=2 for app-limited groups (G3/G5)
// - app_id=1 for other groups
function resolveAppId(groupName) {
  if (groupName === 'G3_APP2_DAILY_5MB' || groupName === 'G5_APP2_SPEED_1MBPS') {
    return 2;
  }
  return 1;
}

// Lightweight deterministic PRNG so each line always gets the same request chunks.
function createDeterministicRandom(seed) {
  let state = seed >>> 0;
  return () => {
    state = (1664525 * state + 1013904223) >>> 0;
    return state / 4294967296;
  };
}

// Build one line's request plan in MB units.
// Rule: each request must be between 1MB and 3MB, and total must be exactly 50MB.
function buildChunkPlanMb(lineId, totalMb) {
  const random = createDeterministicRandom((lineId * 1103515245 + 12345) >>> 0);
  const chunks = [];
  let remaining = totalMb;

  // 요청 데이터량은 1~3MB만 사용하며, 마지막 요청도 이 범위를 지키도록 분할한다.
  while (remaining > 0) {
    if (remaining <= 3) {
      chunks.push(remaining);
      remaining = 0;
      continue;
    }

    const nextChunkMb = 1 + Math.floor(random() * 3);
    chunks.push(nextChunkMb);
    remaining -= nextChunkMb;
  }

  return chunks;
}

// Precompute all line plans once so every VU works from a fixed script-time blueprint.
function buildLinePlans() {
  const plans = [];

  for (let lineId = 1; lineId <= LINE_COUNT; lineId += 1) {
    const groupName = resolveGroupName(lineId);
    const chunksMb = buildChunkPlanMb(lineId, TOTAL_ATTEMPT_MB_PER_LINE);
    const chunksBytes = chunksMb.map((v) => v * MB);

    plans.push({
      lineId,
      familyId: resolveFamilyId(lineId),
      groupName,
      appId: resolveAppId(groupName),
      chunksMb,
      chunksBytes,
      totalAttemptBytes: chunksBytes.reduce((sum, value) => sum + value, 0),
    });
  }

  return plans;
}

const LINE_PLANS = buildLinePlans();

// Helpful run-time hint printed in setup logs.
// This is not an assertion; it is a planning aid for post-test reconciliation.
function buildExpectedHint(plan) {
  if (plan.groupName === 'G1_NO_RESTRICTION') {
    return `expected_deducted=${TOTAL_ATTEMPT_MB_PER_LINE}MB`;
  }
  if (plan.groupName === 'G2_DAILY_20MB') {
    return 'expected_deducted=20MB';
  }
  if (plan.groupName === 'G3_APP2_DAILY_5MB') {
    return 'expected_deducted=5MB(appId=2)';
  }
  if (plan.groupName === 'G4_SHARED_ONLY') {
    return 'expected_per_line=not_fixed(shared 50MB is contended by line13+14)';
  }
  const speedLimitBytesPerSecond = APP2_SPEED_LIMIT_KBPS * 125;
  const speedCapBytes = speedLimitBytesPerSecond * plan.chunksBytes.length;
  const expectedUpperBytes = Math.min(plan.totalAttemptBytes, speedCapBytes);
  return `expected_deducted_upper=${expectedUpperBytes}B(speed cap, appId=2)`;
}

export function setup() {
  // setup() runs once before VUs start.
  // We print the pre-recorded per-line plan so operators can save it as test evidence.
  const totalRequests = LINE_PLANS.reduce((sum, plan) => sum + plan.chunksBytes.length, 0);
  const totalAttemptBytes = LINE_PLANS.reduce((sum, plan) => sum + plan.totalAttemptBytes, 0);

  console.log(`[k6] target_url=${TARGET_URL}`);
  console.log(`[k6] line_count=${LINE_COUNT} total_requests=${totalRequests} total_attempt_bytes=${totalAttemptBytes}`);
  console.log('[k6] line request plan (pre-recorded):');

  for (const plan of LINE_PLANS) {
    const chunkPreview = plan.chunksMb.join(',');
    console.log(
      `[k6-plan] line=${plan.lineId} family=${plan.familyId} group=${plan.groupName} app=${plan.appId} `
      + `chunks_mb=[${chunkPreview}] total_attempt_bytes=${plan.totalAttemptBytes} ${buildExpectedHint(plan)}`
    );
  }

  return {
    targetUrl: TARGET_URL,
  };
}

export default function (data) {
  // Each VU handles exactly one line by using 1-based idInTest -> 0-based array index.
  const vuId = exec.vu.idInTest;
  const planIndex = vuId - 1;

  if (planIndex < 0 || planIndex >= LINE_PLANS.length) {
    // Safety guard: do nothing if VU count differs from expected mapping.
    return;
  }

  const plan = LINE_PLANS[planIndex];
  const targetUrl = data.targetUrl;

  for (let i = 0; i < plan.chunksBytes.length; i += 1) {
    // We keep a start timestamp to enforce fixed 1-second request-start intervals.
    const requestStartedAt = Date.now();
    const requestBytes = plan.chunksBytes[i];
    const payload = {
      lineId: plan.lineId,
      familyId: plan.familyId,
      appId: plan.appId,
      apiTotalData: requestBytes,
    };

    const tags = {
      line_id: String(plan.lineId),
      family_id: String(plan.familyId),
      group: plan.groupName,
      app_id: String(plan.appId),
      request_index: String(i + 1),
    };

    attemptedBytesCount.add(requestBytes, tags);

    const response = http.post(targetUrl, JSON.stringify(payload), {
      headers: {
        'Content-Type': 'application/json',
      },
      tags,
      timeout: K6_TIMEOUT,
    });

    enqueueDurationMs.add(response.timings.duration, tags);

    const isHttpOk = check(response, {
      'enqueue status is 200': (res) => res.status === 200,
    }, tags);

    enqueueHttpErrorRate.add(!isHttpOk, tags);

    // We additionally validate that traceId is returned,
    // because downstream reconciliation depends on this key.
    let hasTraceId = false;
    if (isHttpOk) {
      try {
        const body = response.json();
        hasTraceId = Boolean(body && typeof body.traceId === 'string' && body.traceId.length > 0);
      } catch (error) {
        hasTraceId = false;
      }
    }

    enqueueTraceIdErrorRate.add(!hasTraceId, tags);

    if (isHttpOk && hasTraceId) {
      enqueueSuccessCount.add(1, tags);
    } else {
      enqueueFailCount.add(1, tags);
    }

    // 사용자 요청 조건: 요청 간격은 라인별로 1초 고정.
    // 응답이 1초보다 빨리 오면 남은 시간만큼만 대기해 요청 시작 간격을 맞춘다.
    if (i < plan.chunksBytes.length - 1) {
      const elapsedMs = Date.now() - requestStartedAt;
      const sleepSeconds = Math.max(0, (REQUEST_INTERVAL_SECONDS * 1000 - elapsedMs) / 1000);
      sleep(sleepSeconds);
    }
  }
}

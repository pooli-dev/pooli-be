import http from 'k6/http';
import exec from 'k6/execution';
import { check, sleep } from 'k6';
import { Counter, Rate, Trend } from 'k6/metrics';

const MB = 1024 * 1024;
const TOTAL_ATTEMPT_MB_PER_LINE = 50;
const REQUEST_INTERVAL_SECONDS = 1;
const APP2_SPEED_LIMIT_KBPS = 1000;

const K6_TIMEOUT = __ENV.REQUEST_TIMEOUT || '30s';
const DEBUG_NON_200 = String(__ENV.DEBUG_NON_200 || 'false').toLowerCase() === 'true';
const MAX_DEBUG_LOGS_PER_VU = Number(__ENV.MAX_DEBUG_LOGS_PER_VU || 3);

const BASE_URL = (__ENV.BASE_URL || 'http://localhost:8080').replace(/\/+$/, '');
const API_PATH = (__ENV.API_PATH || '/api/traffic/requests').startsWith('/')
  ? (__ENV.API_PATH || '/api/traffic/requests')
  : `/${__ENV.API_PATH}`;
const TARGET_URL = `${BASE_URL}${API_PATH}`;

// Custom metrics are shared across scaled scripts.
const enqueueDurationMs = new Trend('traffic_enqueue_duration_ms');
const enqueueHttpErrorRate = new Rate('traffic_enqueue_http_error_rate');
const enqueueTraceIdErrorRate = new Rate('traffic_enqueue_traceid_error_rate');
const enqueueSuccessCount = new Counter('traffic_enqueue_success_total');
const enqueueFailCount = new Counter('traffic_enqueue_fail_total');
const attemptedBytesCount = new Counter('traffic_attempted_bytes_total');

function createDeterministicRandom(seed) {
  let state = seed >>> 0;
  return () => {
    state = (1664525 * state + 1013904223) >>> 0;
    return state / 4294967296;
  };
}

function buildChunkPlanMb(lineId, totalMb) {
  const random = createDeterministicRandom((lineId * 1103515245 + 12345) >>> 0);
  const chunks = [];
  let remaining = totalMb;

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

function buildGroupBoundaries(lineCount) {
  const g1Count = Math.floor(lineCount * 0.25);
  const g2Count = Math.floor(lineCount * 0.25);
  const g3Count = Math.floor(lineCount * 0.25);
  const g4Count = Math.floor(lineCount * 0.125);
  const g5Count = lineCount - g1Count - g2Count - g3Count - g4Count;

  const g1End = g1Count;
  const g2End = g1End + g2Count;
  const g3End = g2End + g3Count;
  const g4End = g3End + g4Count;

  return {
    g1Count,
    g2Count,
    g3Count,
    g4Count,
    g5Count,
    g1End,
    g2End,
    g3End,
    g4End,
  };
}

function resolveGroupName(lineId, boundaries) {
  if (lineId <= boundaries.g1End) return 'G1_NO_RESTRICTION';
  if (lineId <= boundaries.g2End) return 'G2_DAILY_20MB';
  if (lineId <= boundaries.g3End) return 'G3_APP2_DAILY_5MB';
  if (lineId <= boundaries.g4End) return 'G4_SHARED_ONLY';
  return 'G5_APP2_SPEED_1MBPS';
}

function resolveAppId(groupName) {
  if (groupName === 'G3_APP2_DAILY_5MB' || groupName === 'G5_APP2_SPEED_1MBPS') {
    return 2;
  }
  return 1;
}

function resolveFamilyId(lineId, lineCount) {
  if (lineId < 1 || lineId > lineCount) {
    throw new Error(`lineId out of range: ${lineId}`);
  }
  return Math.floor((lineId - 1) / 4) + 1;
}

function buildLinePlans(lineCount, boundaries) {
  const plans = [];

  for (let lineId = 1; lineId <= lineCount; lineId += 1) {
    const groupName = resolveGroupName(lineId, boundaries);
    const chunksMb = buildChunkPlanMb(lineId, TOTAL_ATTEMPT_MB_PER_LINE);
    const chunksBytes = chunksMb.map((v) => v * MB);

    plans.push({
      lineId,
      familyId: resolveFamilyId(lineId, lineCount),
      groupName,
      appId: resolveAppId(groupName),
      chunksMb,
      chunksBytes,
      totalAttemptBytes: chunksBytes.reduce((sum, value) => sum + value, 0),
    });
  }

  return plans;
}

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
    return 'expected_per_line=not_fixed(shared 50MB is contended)';
  }
  const speedLimitBytesPerSecond = APP2_SPEED_LIMIT_KBPS * 125;
  const speedCapBytes = speedLimitBytesPerSecond * plan.chunksBytes.length;
  const expectedUpperBytes = Math.min(plan.totalAttemptBytes, speedCapBytes);
  return `expected_deducted_upper=${expectedUpperBytes}B(speed cap, appId=2)`;
}

export function buildK6Scenario(lineCount) {
  if (lineCount <= 0) {
    throw new Error('lineCount must be positive.');
  }
  if (lineCount % 4 !== 0) {
    throw new Error('lineCount must be a multiple of 4.');
  }

  const boundaries = buildGroupBoundaries(lineCount);
  const linePlans = buildLinePlans(lineCount, boundaries);

  const options = {
    scenarios: {
      scaled_traffic: {
        executor: 'per-vu-iterations',
        vus: lineCount,
        iterations: 1,
        maxDuration: __ENV.MAX_DURATION || '60m',
        gracefulStop: '0s',
      },
    },
    thresholds: {
      traffic_enqueue_http_error_rate: ['rate==0'],
      traffic_enqueue_traceid_error_rate: ['rate==0'],
    },
  };

  function setup() {
    const totalRequests = linePlans.reduce((sum, plan) => sum + plan.chunksBytes.length, 0);
    const totalAttemptBytes = linePlans.reduce((sum, plan) => sum + plan.totalAttemptBytes, 0);
    const familyCount = lineCount / 4;

    console.log(`[k6] target_url=${TARGET_URL}`);
    console.log(
      `[k6] line_count=${lineCount} family_count=${familyCount} total_requests=${totalRequests} total_attempt_bytes=${totalAttemptBytes}`
    );
    console.log(
      `[k6] groups: g1=${boundaries.g1Count}, g2=${boundaries.g2Count}, g3=${boundaries.g3Count}, g4=${boundaries.g4Count}, g5=${boundaries.g5Count}`
    );
    console.log('[k6] line request plan (pre-recorded):');

    for (const plan of linePlans) {
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

  function defaultFn(data) {
    const vuId = exec.vu.idInTest;
    const planIndex = vuId - 1;

    if (planIndex < 0 || planIndex >= linePlans.length) {
      return;
    }

    const plan = linePlans[planIndex];
    const targetUrl = data.targetUrl;
    let debugLogCount = 0;

    for (let i = 0; i < plan.chunksBytes.length; i += 1) {
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
      if (!isHttpOk && DEBUG_NON_200 && debugLogCount < MAX_DEBUG_LOGS_PER_VU) {
        const bodyPreview = (response.body || '').replace(/\s+/g, ' ').slice(0, 240);
        console.error(
          `[k6-non200] line=${plan.lineId} family=${plan.familyId} group=${plan.groupName} `
          + `app=${plan.appId} req_idx=${i + 1} status=${response.status} body="${bodyPreview}"`
        );
        debugLogCount += 1;
      }

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

      if (i < plan.chunksBytes.length - 1) {
        const elapsedMs = Date.now() - requestStartedAt;
        const sleepSeconds = Math.max(0, (REQUEST_INTERVAL_SECONDS * 1000 - elapsedMs) / 1000);
        sleep(sleepSeconds);
      }
    }
  }

  return {
    options,
    setup,
    default: defaultFn,
  };
}


import http from 'k6/http';
import { check } from 'k6';
import { Counter, Rate, Trend } from 'k6/metrics';

const BASE_URL = requiredEnv('BASE_URL').replace(/\/+$/, '');
const ENDPOINT = (__ENV.ENDPOINT || '/api/traffic/requests').trim();
const DATA_FILE = resolveDataFile(__ENV.DATA_FILE || './data/traffic-targets.sample.json');
const FLOW_MODE = normalizeFlowMode(__ENV.FLOW_MODE || 'mixed');
const HTTP_TIMEOUT = (__ENV.HTTP_TIMEOUT || '30s').trim();
const AUTH_TOKEN = (__ENV.AUTH_TOKEN || '').trim();

const REQUEST_COUNTER = new Counter('traffic_requests_total');
const HTTP_ERROR_COUNTER = new Counter('traffic_http_error_total');
const TRACE_ID_MISSING_COUNTER = new Counter('traffic_trace_id_missing_total');
const ENQUEUE_ACCEPTED_RATE = new Rate('traffic_enqueue_accepted_rate');
const TRACE_ID_RATE = new Rate('traffic_trace_id_rate');
const PAYLOAD_BYTES_TREND = new Trend('traffic_payload_bytes', false);

const TARGETS = normalizeDataset(JSON.parse(open(DATA_FILE)));

const STAGES = parseStages(__ENV.STAGES || '20:2m,50:5m,100:10m');
const THRESHOLD_CHECK_RATE = numberEnv('CHECK_RATE', 0.99);
const MAX_HTTP_FAILURE_RATE = numberEnv('MAX_HTTP_FAILURE_RATE', 0.01);
const MIN_ENQUEUE_ACCEPT_RATE = numberEnv('MIN_ENQUEUE_ACCEPT_RATE', 0.99);
const MIN_TRACE_ID_RATE = numberEnv('MIN_TRACE_ID_RATE', 0.99);
const HTTP_P95_MS = integerEnv('HTTP_P95_MS', 500);

export const options = {
  scenarios: {
    traffic_ingress: {
      executor: 'ramping-arrival-rate',
      exec: 'enqueueTraffic',
      startRate: integerEnv('START_RATE', 20),
      timeUnit: '1s',
      preAllocatedVUs: integerEnv('PRE_ALLOCATED_VUS', 100),
      maxVUs: integerEnv('MAX_VUS', 500),
      stages: STAGES,
    },
  },
  thresholds: {
    checks: [`rate>${THRESHOLD_CHECK_RATE}`],
    http_req_failed: [`rate<${MAX_HTTP_FAILURE_RATE}`],
    http_req_duration: [`p(95)<${HTTP_P95_MS}`],
    traffic_enqueue_accepted_rate: [`rate>${MIN_ENQUEUE_ACCEPT_RATE}`],
    traffic_trace_id_rate: [`rate>${MIN_TRACE_ID_RATE}`],
  },
  userAgent: 'pooli-k6/1.0',
};

export function enqueueTraffic() {
  const groupName = pickFlowGroup();
  const target = pickTarget(TARGETS[groupName], groupName);
  const apiTotalData = pickPayloadBytes(target.payloadBytes);
  const body = JSON.stringify({
    lineId: target.lineId,
    familyId: target.familyId,
    appId: target.appId,
    apiTotalData: apiTotalData,
  });

  const tags = {
    flow: groupName,
    target: target.name,
    endpoint: ENDPOINT,
  };

  REQUEST_COUNTER.add(1, tags);
  PAYLOAD_BYTES_TREND.add(apiTotalData, tags);

  const response = http.post(`${BASE_URL}${ENDPOINT}`, body, {
    headers: buildHeaders(),
    timeout: HTTP_TIMEOUT,
    tags: tags,
  });

  const responseJson = safeParseJson(response);
  const accepted = response.status === 200;
  const traceIdPresent = Boolean(responseJson && typeof responseJson.traceId === 'string' && responseJson.traceId.length > 0);

  ENQUEUE_ACCEPTED_RATE.add(accepted, tags);
  TRACE_ID_RATE.add(traceIdPresent, tags);

  if (!accepted) {
    HTTP_ERROR_COUNTER.add(1, withStatus(tags, response.status));
  }

  if (!traceIdPresent) {
    TRACE_ID_MISSING_COUNTER.add(1, withStatus(tags, response.status));
  }

  check(response, {
    'status is 200': (res) => res.status === 200,
    'traceId exists': () => traceIdPresent,
  });

  if ((response.status !== 200 || !traceIdPresent) && __ENV.LOG_ERRORS === 'true') {
    console.error(
      JSON.stringify({
        message: 'traffic enqueue request failed',
        flow: groupName,
        target: target.name,
        status: response.status,
        body: response.body,
      })
    );
  }
}

function buildHeaders() {
  const headers = {
    'Content-Type': 'application/json',
    Accept: 'application/json',
  };

  if (AUTH_TOKEN) {
    headers.Authorization = `Bearer ${AUTH_TOKEN}`;
  }

  return headers;
}

function pickFlowGroup() {
  if (FLOW_MODE !== 'mixed') {
    ensureGroupHasTargets(FLOW_MODE);
    return FLOW_MODE;
  }

  const weightedGroups = [
    { name: 'normal', weight: integerEnv('WEIGHT_NORMAL', 70) },
    { name: 'hot', weight: integerEnv('WEIGHT_HOT', 15) },
    { name: 'refill', weight: integerEnv('WEIGHT_REFILL', 10) },
    { name: 'policyHit', weight: integerEnv('WEIGHT_POLICY', 5) },
  ].filter((entry) => entry.weight > 0 && TARGETS[entry.name].length > 0);

  if (weightedGroups.length === 0) {
    throw new Error('No available target groups for FLOW_MODE=mixed.');
  }

  return weightedPick(weightedGroups).name;
}

function ensureGroupHasTargets(groupName) {
  if (!TARGETS[groupName] || TARGETS[groupName].length === 0) {
    throw new Error(`Target group "${groupName}" is empty in ${DATA_FILE}.`);
  }
}

function pickTarget(targets, groupName) {
  if (!targets || targets.length === 0) {
    throw new Error(`Target group "${groupName}" is empty in ${DATA_FILE}.`);
  }

  return weightedPick(targets);
}

function pickPayloadBytes(payloadBytes) {
  return payloadBytes[randomIndex(payloadBytes.length)];
}

function normalizeDataset(raw) {
  const dataset = {
    normal: normalizeGroup(raw.normal || [], 'normal'),
    hot: normalizeGroup(raw.hot || [], 'hot'),
    refill: normalizeGroup(raw.refill || [], 'refill'),
    policyHit: normalizeGroup(raw.policyHit || raw.policy || [], 'policyHit'),
  };

  const totalTargets =
    dataset.normal.length + dataset.hot.length + dataset.refill.length + dataset.policyHit.length;

  if (totalTargets === 0) {
    throw new Error(`No targets found in ${DATA_FILE}.`);
  }

  return dataset;
}

function normalizeGroup(items, groupName) {
  if (!Array.isArray(items)) {
    throw new Error(`Group "${groupName}" must be an array in ${DATA_FILE}.`);
  }

  return items.map((item, index) => normalizeTarget(item, groupName, index));
}

function normalizeTarget(item, groupName, index) {
  if (!item || typeof item !== 'object') {
    throw new Error(`Invalid target at ${groupName}[${index}] in ${DATA_FILE}.`);
  }

  const payloadSource = Array.isArray(item.payloadBytes)
    ? item.payloadBytes
    : [item.payloadBytes !== undefined ? item.payloadBytes : item.apiTotalData];

  const payloadBytes = payloadSource.map((value) => {
    const parsed = Number(value);
    if (!Number.isFinite(parsed) || parsed < 0) {
      throw new Error(`Invalid payloadBytes in ${groupName}[${index}] in ${DATA_FILE}.`);
    }
    return Math.floor(parsed);
  });

  if (payloadBytes.length === 0) {
    throw new Error(`payloadBytes must not be empty in ${groupName}[${index}] in ${DATA_FILE}.`);
  }

  const lineId = positiveInteger(item.lineId, `lineId in ${groupName}[${index}]`);
  const familyId = positiveInteger(item.familyId, `familyId in ${groupName}[${index}]`);
  const appId = positiveInteger(item.appId, `appId in ${groupName}[${index}]`);
  const weight = positiveInteger(item.weight || 1, `weight in ${groupName}[${index}]`);

  return {
    name: String(item.name || `${groupName}-${index + 1}`),
    lineId: lineId,
    familyId: familyId,
    appId: appId,
    payloadBytes: payloadBytes,
    weight: weight,
  };
}

function parseStages(rawStages) {
  if (!rawStages || !rawStages.trim()) {
    throw new Error('STAGES must not be empty.');
  }

  return rawStages.split(',').map((entry) => {
    const trimmed = entry.trim();
    const parts = trimmed.split(':');

    if (parts.length !== 2) {
      throw new Error(`Invalid stage entry "${trimmed}". Expected target:duration.`);
    }

    return {
      target: positiveInteger(parts[0], `stage target "${trimmed}"`),
      duration: parts[1].trim(),
    };
  });
}

function requiredEnv(name) {
  const value = (__ENV[name] || '').trim();
  if (!value) {
    throw new Error(`${name} is required. Example: -e ${name}=http://localhost:8080`);
  }
  return value;
}

function resolveDataFile(value) {
  const trimmed = String(value || '').trim();

  if (!trimmed) {
    throw new Error('DATA_FILE must not be empty.');
  }

  // k6 open() resolves relative paths from the script location.
  // Accept common workspace-relative forms to reduce caller mistakes.
  const normalized = trimmed.replace(/\\/g, '/');

  if (normalized.startsWith('./') || normalized.startsWith('../')) {
    return normalized;
  }

  if (normalized.startsWith('loadtest/k6/')) {
    return `./${normalized.slice('loadtest/k6/'.length)}`;
  }

  if (normalized.startsWith('k6/')) {
    return `./${normalized.slice('k6/'.length)}`;
  }

  return normalized;
}

function normalizeFlowMode(value) {
  const normalized = String(value || 'mixed').trim().toLowerCase();

  switch (normalized) {
    case 'mixed':
      return 'mixed';
    case 'normal':
      return 'normal';
    case 'hot':
      return 'hot';
    case 'refill':
      return 'refill';
    case 'policy':
    case 'policyhit':
      return 'policyHit';
    default:
      throw new Error(`Unsupported FLOW_MODE "${value}".`);
  }
}

function safeParseJson(response) {
  const contentType = response.headers['Content-Type'] || response.headers['content-type'] || '';
  if (!contentType.includes('application/json')) {
    return null;
  }

  try {
    return response.json();
  } catch (error) {
    return null;
  }
}

function weightedPick(items) {
  const totalWeight = items.reduce((sum, item) => sum + item.weight, 0);

  if (totalWeight <= 0) {
    return items[randomIndex(items.length)];
  }

  let cursor = Math.random() * totalWeight;
  for (let index = 0; index < items.length; index += 1) {
    cursor -= items[index].weight;
    if (cursor < 0) {
      return items[index];
    }
  }

  return items[items.length - 1];
}

function randomIndex(length) {
  return Math.floor(Math.random() * length);
}

function integerEnv(name, fallback) {
  const raw = (__ENV[name] || '').trim();
  if (!raw) {
    return fallback;
  }

  return positiveInteger(raw, `${name}`);
}

function numberEnv(name, fallback) {
  const raw = (__ENV[name] || '').trim();
  if (!raw) {
    return fallback;
  }

  const parsed = Number(raw);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    throw new Error(`${name} must be a positive number.`);
  }
  return parsed;
}

function positiveInteger(value, label) {
  const parsed = Number(value);
  if (!Number.isFinite(parsed) || parsed <= 0 || Math.floor(parsed) !== parsed) {
    throw new Error(`${label} must be a positive integer.`);
  }
  return parsed;
}

function withStatus(tags, status) {
  return Object.assign({}, tags, { status: String(status) });
}

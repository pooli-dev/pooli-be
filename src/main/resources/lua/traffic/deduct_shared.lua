-- deduct_shared.lua
-- whitelist -> immediate -> repeat -> daily -> monthly_shared -> app_daily -> app_speed

local function as_json(answer, status)
  return cjson.encode({ answer = answer, status = status })
end

local function is_policy_enabled(policy_key)
  if not policy_key or policy_key == "" then
    return false
  end
  return tonumber(redis.call("HGET", policy_key, "value") or "0") == 1
end

local function has_missing_global_policy_key(...)
  local policy_keys = { ... }
  local idx = 1
  while idx <= #policy_keys do
    local policy_key = policy_keys[idx]
    if not policy_key or policy_key == "" then
      return true
    end
    if redis.call("EXISTS", policy_key) == 0 then
      return true
    end
    idx = idx + 1
  end
  return false
end

local function is_in_repeat_block(repeat_block_key, day_num, sec_of_day)
  if not repeat_block_key or repeat_block_key == "" then
    return false
  end
  if redis.call("EXISTS", repeat_block_key) == 0 then
    return false
  end

  local cursor = "0"
  repeat
    local scan_res = redis.call("HSCAN", repeat_block_key, cursor, "MATCH", "day:" .. day_num .. ":*", "COUNT", 100)
    cursor = scan_res[1]
    local entries = scan_res[2]

    local idx = 1
    while idx <= #entries do
      local range_text = entries[idx + 1]
      if range_text then
        local start_text, end_text = string.match(range_text, "^(%-?%d+):(%-?%d+)$")
        local start_sec = tonumber(start_text)
        local end_sec = tonumber(end_text)
        if start_sec and end_sec and sec_of_day >= start_sec and sec_of_day <= end_sec then
          return true
        end
      end
      idx = idx + 2
    end
  until cursor == "0"

  return false
end

-- KEYS
local remaining_key = KEYS[1]
local policy_repeat_key = KEYS[2]
local policy_immediate_key = KEYS[3]
local policy_shared_key = KEYS[4]
local policy_daily_key = KEYS[5]
local policy_app_data_key = KEYS[6]
local policy_app_speed_key = KEYS[7]
local policy_whitelist_key = KEYS[8]
local app_whitelist_key = KEYS[9]
local immediately_block_end_key = KEYS[10]
local repeat_block_key = KEYS[11]
local daily_total_limit_key = KEYS[12]
local daily_total_usage_key = KEYS[13]
local monthly_shared_limit_key = KEYS[14]
local monthly_shared_usage_key = KEYS[15]
local app_data_daily_limit_key = KEYS[16]
local daily_app_usage_key = KEYS[17]
local app_speed_limit_key = KEYS[18]
local speed_bucket_key = KEYS[19]

-- ARGV
local target_data = tonumber(ARGV[1])
local app_id = tonumber(ARGV[2])
local day_num = tonumber(ARGV[3])
local sec_of_day = tonumber(ARGV[4])
local now_epoch_second = tonumber(ARGV[5])
local daily_expire_at = tonumber(ARGV[6])
local monthly_expire_at = tonumber(ARGV[7])

if not remaining_key or remaining_key == "" then
  return as_json(-1, "ERROR")
end
if not target_data or target_data < 0 then
  return as_json(-1, "ERROR")
end
if not app_id or app_id < 0 then
  return as_json(-1, "ERROR")
end
if not day_num or day_num < 0 or day_num > 6 then
  return as_json(-1, "ERROR")
end
if not sec_of_day or sec_of_day < 0 or sec_of_day > 86399 then
  return as_json(-1, "ERROR")
end
if not now_epoch_second or now_epoch_second <= 0 then
  return as_json(-1, "ERROR")
end
if not daily_expire_at or daily_expire_at <= 0 then
  return as_json(-1, "ERROR")
end
if not monthly_expire_at or monthly_expire_at <= 0 then
  return as_json(-1, "ERROR")
end

-- 전역 정책 키 중 하나라도 누락되면 워커가 전체 정책 hydrate를 먼저 수행한다.
if has_missing_global_policy_key(
  policy_repeat_key,
  policy_immediate_key,
  policy_shared_key,
  policy_daily_key,
  policy_app_data_key,
  policy_app_speed_key,
  policy_whitelist_key
) then
  return as_json(0, "GLOBAL_POLICY_HYDRATE")
end

local current_amount = tonumber(redis.call("HGET", remaining_key, "amount") or "-1")
if current_amount < 0 then
  return as_json(0, "HYDRATE")
end

local final_status = "OK"
local answer = 0

local app_member = tostring(math.floor(app_id))
local app_usage_field = "app:" .. app_member
local app_limit_field = "limit:" .. app_member
local app_speed_field = "speed:" .. app_member

local whitelist_bypass = false
if is_policy_enabled(policy_whitelist_key) and app_whitelist_key and app_whitelist_key ~= "" then
  whitelist_bypass = redis.call("SISMEMBER", app_whitelist_key, app_member) == 1
end

if not whitelist_bypass then
  if is_policy_enabled(policy_immediate_key) then
    local block_end_at = tonumber(redis.call("HGET", immediately_block_end_key, "value") or "0")
    if block_end_at > 0 and now_epoch_second <= block_end_at then
      return as_json(0, "BLOCKED_IMMEDIATE")
    end
  end

  if is_policy_enabled(policy_repeat_key) then
    if is_in_repeat_block(repeat_block_key, day_num, sec_of_day) then
      return as_json(0, "BLOCKED_REPEAT")
    end
  end
end

answer = math.min(current_amount, target_data)

if current_amount < target_data then
  final_status = "NO_BALANCE"
end

if answer <= 0 then
  return as_json(0, final_status)
end

if not whitelist_bypass then
  if is_policy_enabled(policy_daily_key) then
    local daily_limit = tonumber(redis.call("HGET", daily_total_limit_key, "value") or "-1")
    if daily_limit >= 0 then
      local daily_used = tonumber(redis.call("GET", daily_total_usage_key) or "0")
      local daily_remaining = math.max(0, daily_limit - daily_used)
      local before_daily_cap = answer
      answer = math.min(answer, daily_remaining)
      if answer <= 0 then
        return as_json(0, "HIT_DAILY_LIMIT")
      end
      -- 정책에 의해 부분 제한이 발생하면 OK를 유지하지 않고 제한 상태를 기록한다.
      if answer < before_daily_cap then
        final_status = "HIT_DAILY_LIMIT"
      end
    end
  end

  if is_policy_enabled(policy_shared_key) then
    local monthly_limit = tonumber(redis.call("HGET", monthly_shared_limit_key, "value") or "-1")
    if monthly_limit >= 0 then
      local monthly_used = tonumber(redis.call("GET", monthly_shared_usage_key) or "0")
      local monthly_remaining = math.max(0, monthly_limit - monthly_used)
      local before_monthly_cap = answer
      answer = math.min(answer, monthly_remaining)
      if answer <= 0 then
        return as_json(0, "HIT_MONTHLY_SHARED_LIMIT")
      end
      if answer < before_monthly_cap then
        final_status = "HIT_MONTHLY_SHARED_LIMIT"
      end
    end
  end

  if is_policy_enabled(policy_app_data_key) then
    local app_daily_limit = tonumber(redis.call("HGET", app_data_daily_limit_key, app_limit_field) or "-1")
    if app_daily_limit >= 0 then
      local app_daily_used = tonumber(redis.call("HGET", daily_app_usage_key, app_usage_field) or "0")
      local app_daily_remaining = math.max(0, app_daily_limit - app_daily_used)
      local before_app_daily_cap = answer
      answer = math.min(answer, app_daily_remaining)
      if answer <= 0 then
        return as_json(0, "HIT_APP_DAILY_LIMIT")
      end
      if answer < before_app_daily_cap then
        final_status = "HIT_APP_DAILY_LIMIT"
      end
    end
  end

  if is_policy_enabled(policy_app_speed_key) then
    local app_speed_limit = tonumber(redis.call("HGET", app_speed_limit_key, app_speed_field) or "-1")
    if app_speed_limit >= 0 then
      local speed_used = tonumber(redis.call("GET", speed_bucket_key) or "0")
      local speed_remaining = math.max(0, app_speed_limit - speed_used)
      local before_speed_cap = answer
      answer = math.min(answer, speed_remaining)
      if answer <= 0 then
        return as_json(0, "HIT_APP_SPEED")
      end
      if answer < before_speed_cap then
        final_status = "HIT_APP_SPEED"
      end
    end
  end
end

redis.call("HINCRBY", remaining_key, "amount", -answer)

redis.call("INCRBY", daily_total_usage_key, answer)
redis.call("EXPIREAT", daily_total_usage_key, daily_expire_at)
redis.call("INCRBY", monthly_shared_usage_key, answer)
redis.call("EXPIREAT", monthly_shared_usage_key, monthly_expire_at)
redis.call("HINCRBY", daily_app_usage_key, app_usage_field, answer)
redis.call("EXPIREAT", daily_app_usage_key, daily_expire_at)

return as_json(answer, final_status)

-- deduct_indiv_tick.lua
-- 반환 형식: {"answer":number,"status":"..."} JSON 문자열
-- 정책 적용 순서(개인풀):
-- Encode the numeric answer and status label into a JSON string.
-- @param answer The numeric value representing the deducted or granted amount.
-- @param status A short status string describing the outcome (e.g., "OK", "ERROR").
-- @return A JSON-encoded string with fields `answer` (number) and `status` (string).

local function as_json(answer, status)
  return cjson.encode({ answer = answer, status = status })
end

-- Determines whether a policy is enabled by checking for the presence of its Redis key.
-- @param policy_key Redis key that represents the policy flag; nil or empty keys are treated as disabled.
-- @return `true` if the Redis key exists (policy enabled), `false` otherwise.
local function is_policy_enabled(policy_key)
  if not policy_key or policy_key == "" then
    return false
  end
  return redis.call("EXISTS", policy_key) == 1
end

-- Checks whether the given second-of-day falls within any configured repeat-block range for the specified day.
-- @param repeat_block_key Redis hash key that contains repeat-block entries named "day:<day_num>:*"; each entry's value is a "start:end" range in seconds.
-- @param day_num Integer day index (typically 0–6) to match against keys in the hash.
-- @param sec_of_day Integer second within the day (0–86399) to test for inclusion in any range.
-- @return `true` if `sec_of_day` is within any "start:end" range for `day_num` in `repeat_block_key`, `false` otherwise.
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
local policy_daily_key = KEYS[4]
local policy_app_data_key = KEYS[5]
local policy_app_speed_key = KEYS[6]
local policy_whitelist_key = KEYS[7]
local app_whitelist_key = KEYS[8]
local immediately_block_end_key = KEYS[9]
local repeat_block_key = KEYS[10]
local daily_total_limit_key = KEYS[11]
local daily_total_usage_key = KEYS[12]
local app_data_daily_limit_key = KEYS[13]
local daily_app_usage_key = KEYS[14]
local app_speed_limit_key = KEYS[15]
local speed_bucket_key = KEYS[16]

-- ARGV
local target_data = tonumber(ARGV[1])
local app_id = tonumber(ARGV[2])
local day_num = tonumber(ARGV[3])
local sec_of_day = tonumber(ARGV[4])
local now_epoch_second = tonumber(ARGV[5])
local daily_expire_at = tonumber(ARGV[6])

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

local current_amount = tonumber(redis.call("HGET", remaining_key, "amount") or "-1")
if current_amount < 0 then
  return as_json(0, "HYDRATE")
end

local answer = math.min(current_amount, target_data)
if answer <= 0 then
  redis.call("HSET", remaining_key, "is_empty", "1")
  return as_json(0, "NO_BALANCE")
end

local app_member = tostring(math.floor(app_id))
local app_usage_field = "app:" .. app_member
local app_limit_field = "limit:" .. app_member
local app_speed_field = "speed:" .. app_member

local whitelist_bypass = false
if is_policy_enabled(policy_whitelist_key) and app_whitelist_key and app_whitelist_key ~= "" then
  whitelist_bypass = redis.call("SISMEMBER", app_whitelist_key, app_member) == 1
end

if not whitelist_bypass then
  -- 1) 즉시 차단
  if is_policy_enabled(policy_immediate_key) then
    local block_end_at = tonumber(redis.call("GET", immediately_block_end_key) or "0")
    if block_end_at > 0 and now_epoch_second <= block_end_at then
      return as_json(0, "BLOCKED_IMMEDIATE")
    end
  end

  -- 2) 반복 차단
  if is_policy_enabled(policy_repeat_key) then
    if is_in_repeat_block(repeat_block_key, day_num, sec_of_day) then
      return as_json(0, "BLOCKED_REPEAT")
    end
  end

  -- 3) 일 총량 제한
  if is_policy_enabled(policy_daily_key) then
    local daily_limit = tonumber(redis.call("GET", daily_total_limit_key) or "-1")
    if daily_limit >= 0 then
      local daily_used = tonumber(redis.call("GET", daily_total_usage_key) or "0")
      local daily_remaining = math.max(0, daily_limit - daily_used)
      answer = math.min(answer, daily_remaining)
      if answer <= 0 then
        return as_json(0, "HIT_DAILY_LIMIT")
      end
    end
  end

  -- 4) 앱 일 총량 제한
  if is_policy_enabled(policy_app_data_key) then
    local app_daily_limit = tonumber(redis.call("HGET", app_data_daily_limit_key, app_limit_field) or "-1")
    if app_daily_limit >= 0 then
      local app_daily_used = tonumber(redis.call("HGET", daily_app_usage_key, app_usage_field) or "0")
      local app_daily_remaining = math.max(0, app_daily_limit - app_daily_used)
      answer = math.min(answer, app_daily_remaining)
      if answer <= 0 then
        return as_json(0, "HIT_APP_DAILY_LIMIT")
      end
    end
  end

  -- 5) 앱 속도 제한(초당)
  if is_policy_enabled(policy_app_speed_key) then
    local app_speed_limit = tonumber(redis.call("HGET", app_speed_limit_key, app_speed_field) or "-1")
    if app_speed_limit >= 0 then
      local speed_used = tonumber(redis.call("GET", speed_bucket_key) or "0")
      local speed_remaining = math.max(0, app_speed_limit - speed_used)
      answer = math.min(answer, speed_remaining)
      if answer <= 0 then
        return as_json(0, "HIT_APP_SPEED")
      end
    end
  end
end

-- 실제 차감/사용량 반영
redis.call("HINCRBY", remaining_key, "amount", -answer)

local remaining_after = current_amount - answer
if remaining_after <= 0 then
  redis.call("HSET", remaining_key, "is_empty", "1")
else
  redis.call("HSET", remaining_key, "is_empty", "0")
end

redis.call("INCRBY", daily_total_usage_key, answer)
redis.call("EXPIREAT", daily_total_usage_key, daily_expire_at)
redis.call("HINCRBY", daily_app_usage_key, app_usage_field, answer)
redis.call("EXPIREAT", daily_app_usage_key, daily_expire_at)

return as_json(answer, "OK")

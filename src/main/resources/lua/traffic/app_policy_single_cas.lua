local incoming = tonumber(ARGV[1])
if not incoming then
  return -1
end

local currentRaw = redis.call('HGET', KEYS[1], '__version')
if currentRaw then
  local current = tonumber(currentRaw)
  if current and incoming <= current then
    return 0
  end
end

local appId = ARGV[2]
local isActive = ARGV[3]
local dataLimit = ARGV[4]
local speedLimit = ARGV[5]
local isWhitelist = ARGV[6]

local limitField = 'limit:' .. appId
local speedField = 'speed:' .. appId

redis.call('HSET', KEYS[1], '__version', ARGV[1])
if isActive == '1' then
  redis.call('HSET', KEYS[1], limitField, dataLimit)
  redis.call('HSET', KEYS[2], speedField, speedLimit)
  if isWhitelist == '1' then
    redis.call('SADD', KEYS[3], appId)
  else
    redis.call('SREM', KEYS[3], appId)
  end
else
  redis.call('HDEL', KEYS[1], limitField)
  redis.call('HDEL', KEYS[2], speedField)
  redis.call('SREM', KEYS[3], appId)
end

return 1

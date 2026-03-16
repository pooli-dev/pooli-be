local incoming = tonumber(ARGV[1])
if not incoming then
  return -1
end

local currentRaw = redis.call('HGET', KEYS[1], 'version')
if currentRaw then
  local current = tonumber(currentRaw)
  if current and incoming <= current then
    return 0
  end
end

redis.call('HSET', KEYS[1], 'value', ARGV[2], 'version', ARGV[1])
return 1

-- KEYS[1]: remaining_* balance hash key
-- KEYS[2]: refill idempotency key
-- ARGV[1]: refill amount
-- ARGV[2]: balance key expireAt epoch seconds
-- ARGV[3]: refill uuid value to store in idempotency key
-- ARGV[4]: idempotency key ttl seconds
-- ARGV[5]: is_empty flag ("1" if DB source is exhausted, "0" otherwise)

if redis.call('EXISTS', KEYS[2]) == 1 then
    return 0
end

local refillAmount = tonumber(ARGV[1])
if not refillAmount or refillAmount <= 0 then
    return -1
end

-- amount 증가와 is_empty 플래그 갱신을 단일 Lua 블록 내에서 원자적으로 처리한다.
-- DB claim 이후 is_empty 상태를 별도 Java 호출로 기록하면 락 상실 보상 등의 경우
-- Redis 상태가 부정합해질 수 있으므로, 이 위치에서 함께 기록한다.
redis.call('HINCRBY', KEYS[1], 'amount', refillAmount)
redis.call('HSET', KEYS[1], 'is_empty', ARGV[5])
redis.call('EXPIREAT', KEYS[1], ARGV[2])
redis.call('SET', KEYS[2], ARGV[3], 'EX', ARGV[4], 'NX')
return 1

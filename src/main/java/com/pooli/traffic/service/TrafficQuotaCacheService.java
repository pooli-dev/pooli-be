package com.pooli.traffic.service;

import java.time.Instant;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Profile;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * 트래픽 잔량 해시(remaining_*)를 읽고 갱신하는 Redis 유틸 서비스입니다.
 * HYDRATE/REFILL 어댑터가 공통적으로 사용하는 캐시 접근 로직을 모읍니다.
 */
@Slf4j
@Service
@Profile({"local", "traffic"})
@RequiredArgsConstructor
public class TrafficQuotaCacheService {

    @Qualifier("cacheStringRedisTemplate")
    private final StringRedisTemplate cacheStringRedisTemplate;

    /**
     * Fetches the current "amount" value for the given balance key from Redis.
     *
     * If the "amount" field is missing or cannot be parsed as a long, the provided
     * defaultValue is returned.
     *
     * @param balanceKey  the Redis hash key that stores the balance fields
     * @param defaultValue the value to return when the stored amount is missing or invalid
     * @return `defaultValue` if the amount field is missing or cannot be parsed as a long, otherwise the parsed long amount
     */
    public long readAmountOrDefault(String balanceKey, long defaultValue) {
        // amount 필드가 없으면 defaultValue를 반환해 호출자가 분기 판단을 이어갈 수 있게 한다.
        Object rawAmount = cacheStringRedisTemplate.opsForHash().get(balanceKey, "amount");
        if (rawAmount == null) {
            return defaultValue;
        }

        try {
            return Long.parseLong(String.valueOf(rawAmount));
        } catch (NumberFormatException e) {
            // 캐시 값이 깨진 경우에도 어댑터 흐름이 멈추지 않게 defaultValue로 보정한다.
            log.warn("traffic_quota_amount_parse_failed key={} value={}", balanceKey, rawAmount);
            return defaultValue;
        }
    }

    /**
     * Initializes the Redis hash for a balance key with an amount and an empty flag, and applies the key expiration.
     *
     * If `initialAmount` is negative it is treated as zero. Fields are set only when absent to avoid overwriting existing state.
     *
     * @param balanceKey            the Redis key of the balance hash
     * @param initialAmount         the initial amount to populate; negative values are treated as 0
     * @param expireAtEpochSeconds  the expiration time for the key expressed as epoch seconds
     */
    public void hydrateBalance(String balanceKey, long initialAmount, long expireAtEpochSeconds) {
        long normalizedAmount = Math.max(0L, initialAmount);

        // HYDRATE 단계는 키가 없던 상태를 복구하는 목적이므로 putIfAbsent를 사용한다.
        cacheStringRedisTemplate.opsForHash().putIfAbsent(balanceKey, "amount", String.valueOf(normalizedAmount));
        cacheStringRedisTemplate.opsForHash().putIfAbsent(balanceKey, "is_empty", normalizedAmount <= 0 ? "1" : "0");

        // 월별 잔량 키는 명세에 맞춰 월말+10d 시점으로 만료를 설정한다.
        cacheStringRedisTemplate.expireAt(balanceKey, Instant.ofEpochSecond(expireAtEpochSeconds));
    }

    /**
     * Increments the cached remaining amount for the given balance key, clears the empty flag, and sets the key's expiration.
     *
     * <p>If the provided refill amount is less than or equal to zero, the method performs no changes.</p>
     *
     * @param balanceKey              the Redis key (hash) identifying the balance entry
     * @param refillAmount            the amount to add to the existing cached amount; non-positive values are ignored
     * @param expireAtEpochSeconds    the epoch second at which the key should expire; the TTL is set to this instant
     */
    public void refillBalance(String balanceKey, long refillAmount, long expireAtEpochSeconds) {
        long normalizedRefillAmount = Math.max(0L, refillAmount);
        if (normalizedRefillAmount <= 0) {
            // 0 이하 리필은 실효성이 없으므로 무해하게 종료한다.
            return;
        }

        // 리필은 기존 잔량에 누적 증가시키는 동작이므로 increment를 사용한다.
        cacheStringRedisTemplate.opsForHash().increment(balanceKey, "amount", normalizedRefillAmount);
        cacheStringRedisTemplate.opsForHash().put(balanceKey, "is_empty", "0");

        // 리필 시점에도 동일 TTL 정책을 유지한다.
        cacheStringRedisTemplate.expireAt(balanceKey, Instant.ofEpochSecond(expireAtEpochSeconds));
    }
}

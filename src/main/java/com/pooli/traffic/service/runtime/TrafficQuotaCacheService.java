package com.pooli.traffic.service.runtime;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Profile;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Service;

import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * 트래픽 잔량 해시(remaining_*)를 읽고 갱신하는 Redis 유틸 서비스입니다.
 * HYDRATE/REFILL 어댑터가 공통적으로 사용하는 캐시 접근 로직을 모읍니다.
 */
@Slf4j
@Service
@Profile({"local", "api", "traffic"})
@RequiredArgsConstructor
public class TrafficQuotaCacheService {

    private static final String REFILL_APPLY_WITH_IDEMPOTENCY_SCRIPT_RESOURCE = "lua/traffic/refill_apply_with_idempotency.lua";

    @Qualifier("cacheStringRedisTemplate")
    private final StringRedisTemplate cacheStringRedisTemplate;
    private RedisScript<Long> refillApplyWithIdempotencyScript;

    @PostConstruct
    public void initializeScripts() {
        refillApplyWithIdempotencyScript = buildLongScript(loadScriptText(REFILL_APPLY_WITH_IDEMPOTENCY_SCRIPT_RESOURCE));
    }

    /**
     * 외부 저장소에서 현재 데이터를 조회해 반환합니다.
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
     * DB 원천 잔량 고갈 여부를 is_empty 필드(1/0)로 기록합니다.
     */
    public void writeDbEmptyFlag(String balanceKey, boolean isDbEmpty) {
        cacheStringRedisTemplate.opsForHash().put(
                balanceKey,
                "is_empty",
                isDbEmpty ? "1" : "0"
        );
    }

    /**
      * `hydrateBalance` 처리 목적에 맞는 핵심 로직을 수행합니다.
     */
    public void hydrateBalance(String balanceKey, long expireAtEpochSeconds) {
        long normalizedAmount = 0L;

        // HYDRATE 단계는 키가 없던 상태를 복구하는 목적이므로 putIfAbsent를 사용한다.
        cacheStringRedisTemplate.opsForHash().putIfAbsent(balanceKey, "amount", String.valueOf(normalizedAmount));
        // hydrate 기본값은 DB 고갈 "미확정" 상태이므로 0으로 시작한다.
        cacheStringRedisTemplate.opsForHash().putIfAbsent(balanceKey, "is_empty", "0");

        // 월별 잔량 키는 명세에 맞춰 월말+10d 시점으로 만료를 설정한다.
        cacheStringRedisTemplate.expireAt(balanceKey, Instant.ofEpochSecond(expireAtEpochSeconds));
    }

    /**
     * 개인풀 잔량 해시에 QoS 값을 기록합니다.
     * null/음수는 허용하지 않으므로 0 이상으로 정규화해 저장합니다.
     */
    public void putQos(String balanceKey, long qos) {
        long normalizedQos = Math.max(0L, qos);
        cacheStringRedisTemplate.opsForHash().put(balanceKey, "qos", String.valueOf(normalizedQos));
    }

    /**
      * `refillBalance` 처리 목적에 맞는 핵심 로직을 수행합니다.
     */
    public void refillBalance(String balanceKey, long refillAmount, long expireAtEpochSeconds) {
        long normalizedRefillAmount = Math.max(0L, refillAmount);
        if (normalizedRefillAmount <= 0) {
            // 0 이하 리필은 실효성이 없으므로 무해하게 종료한다.
            return;
        }

        // 리필은 기존 잔량에 누적 증가시키는 동작이므로 increment를 사용한다.
        // is_empty는 DB 고갈 플래그이므로 여기서 변경하지 않고 어댑터(DB claim 결과)에서만 갱신한다.
        cacheStringRedisTemplate.opsForHash().increment(balanceKey, "amount", normalizedRefillAmount);

        // 리필 시점에도 동일 TTL 정책을 유지한다.
        cacheStringRedisTemplate.expireAt(balanceKey, Instant.ofEpochSecond(expireAtEpochSeconds));
    }

    /**
     * amount 증가, is_empty 플래그 갱신, 멱등키 등록을 단일 Lua로 원자 처리합니다.
     *
     * <p>is_empty는 Java에서 별도로 HSET하면 락 상실 보상 흐름 등에서 부정합이 발생할 수 있으므로
     * 반드시 이 메서드를 통해 amount 증가와 함께 원자적으로 기록해야 합니다.
     *
     * @param isDbEmpty DB 원천 잔량이 0 이하인지 여부 (Lua 내부에서 is_empty 필드에 기록됨)
     * @return true면 이번 호출에서 amount가 실제 증가됨, false면 이미 처리된 uuid
     */
    public boolean applyRefillWithIdempotency(
            String balanceKey,
            String idempotencyKey,
            String refillUuid,
            long refillAmount,
            long expireAtEpochSeconds,
            long idempotencyTtlSeconds,
            boolean isDbEmpty
    ) {
        long normalizedRefillAmount = Math.max(0L, refillAmount);
        if (normalizedRefillAmount <= 0) {
            return false;
        }

        Long rawResult = cacheStringRedisTemplate.execute(
                requireScript(refillApplyWithIdempotencyScript, "refillApplyWithIdempotencyScript"),
                List.of(balanceKey, idempotencyKey),
                String.valueOf(normalizedRefillAmount),
                String.valueOf(expireAtEpochSeconds),
                refillUuid == null ? "1" : refillUuid,
                String.valueOf(Math.max(1L, idempotencyTtlSeconds)),
                // is_empty 플래그: DB 잔량 소진 여부를 Lua 내부에서 amount와 함께 원자적으로 기록한다.
                isDbEmpty ? "1" : "0"
        );

        return rawResult != null && rawResult == 1L;
    }


    private String loadScriptText(String resourcePath) {
        ClassPathResource resource = new ClassPathResource(resourcePath);
        try {
            return new String(resource.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to load Lua script text. resource=" + resourcePath, e);
        }
    }

    private RedisScript<Long> requireScript(RedisScript<Long> script, String scriptName) {
        if (script == null) {
            throw new IllegalStateException("Lua script is not initialized. script=" + scriptName);
        }
        return script;
    }

    private static RedisScript<Long> buildLongScript(String scriptText) {
        DefaultRedisScript<Long> script = new DefaultRedisScript<>();
        script.setScriptText(scriptText);
        script.setResultType(Long.class);
        return script;
    }
}
